//! Per-inode operation ordering for synchronization between
//! asynchronous (worker pool) and synchronous (main thread) FUSE operations.
//!
//! This module implements a sequence-based barrier mechanism that ensures
//! operations execute in FIFO order: a READ that arrives after WRITE1 but
//! before WRITE2 will see WRITE1's data but not wait for WRITE2.

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Per-inode synchronization state with sequence tracking.
struct InodeState {
    /// Global sequence counter for this inode.
    next_seq: AtomicU64,
    /// Set of in-flight operation sequence numbers.
    in_flight: Mutex<BTreeSet<u64>>,
    /// Condvar for notification when operations complete.
    condvar: Condvar,
}

impl Default for InodeState {
    fn default() -> Self {
        Self {
            next_seq: AtomicU64::new(1),
            in_flight: Mutex::new(BTreeSet::new()),
            condvar: Condvar::new(),
        }
    }
}

/// Global pending operations tracker with sequence-based ordering.
///
/// Ensures barrier operations wait only for operations that were
/// submitted BEFORE them, not for all pending operations.
#[derive(Default)]
pub struct PendingOps {
    inodes: DashMap<u64, Arc<InodeState>>,
}

impl PendingOps {
    /// Create a new pending operations tracker.
    pub fn new() -> Self {
        Self {
            inodes: DashMap::new(),
        }
    }

    /// Get or create inode state, returning Arc to release DashMap lock.
    fn get_state(&self, ino: u64) -> Arc<InodeState> {
        self.inodes
            .entry(ino)
            .or_insert_with(|| Arc::new(InodeState::default()))
            .clone()
    }

    /// Start a mutating operation: get sequence number and mark as in-flight.
    /// Call before submitting to worker pool. Returns sequence number for decrement.
    pub fn increment(&self, ino: u64) -> u64 {
        let state = self.get_state(ino);
        let seq = state.next_seq.fetch_add(1, Ordering::SeqCst);
        state.in_flight.lock().insert(seq);
        seq
    }

    /// Complete a mutating operation: remove from in-flight and notify waiters.
    /// Call after operation completes in worker.
    pub fn decrement(&self, ino: u64, seq: u64) {
        if let Some(state) = self.inodes.get(&ino) {
            let mut in_flight = state.in_flight.lock();
            in_flight.remove(&seq);
            // Notify all waiters - they will check if their barrier is satisfied
            state.condvar.notify_all();
        }
    }

    /// Wait for all operations with sequence < barrier_seq to complete.
    /// Call before barrier operations (open, read, getattr).
    /// Returns the barrier sequence number.
    pub fn wait_barrier(&self, ino: u64) -> u64 {
        let state = self.get_state(ino);

        // Get current sequence as our barrier point - we wait for all ops < this
        let barrier_seq = state.next_seq.load(Ordering::SeqCst);

        let mut in_flight = state.in_flight.lock();
        loop {
            // Check if any in-flight operation has seq < barrier_seq
            let has_preceding = in_flight.iter().any(|&seq| seq < barrier_seq);
            if !has_preceding {
                break;
            }
            // Wait for notification
            state.condvar.wait(&mut in_flight);
        }

        barrier_seq
    }

    /// Wait for all operations with sequence < my_seq to complete.
    /// Call in worker before executing a mutating operation to ensure FIFO order.
    pub fn wait_for_preceding(&self, ino: u64, my_seq: u64) {
        if let Some(state) = self.inodes.get(&ino) {
            let mut in_flight = state.in_flight.lock();
            loop {
                // Check if any in-flight operation has seq < my_seq
                let has_preceding = in_flight.iter().any(|&seq| seq < my_seq);
                if !has_preceding {
                    break;
                }
                // Wait for notification
                state.condvar.wait(&mut in_flight);
            }
        }
    }

    /// Check if there are pending operations (non-blocking).
    #[allow(dead_code)]
    pub fn has_pending(&self, ino: u64) -> bool {
        self.inodes
            .get(&ino)
            .map(|s| !s.in_flight.lock().is_empty())
            .unwrap_or(false)
    }

    /// Cleanup inode entry when inode is forgotten.
    #[allow(dead_code)]
    pub fn remove(&self, ino: u64) {
        self.inodes.remove(&ino);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_increment_decrement() {
        let ops = PendingOps::new();
        let ino = 42;

        assert!(!ops.has_pending(ino));

        let seq1 = ops.increment(ino);
        assert!(ops.has_pending(ino));
        assert_eq!(seq1, 1);

        let seq2 = ops.increment(ino);
        assert!(ops.has_pending(ino));
        assert_eq!(seq2, 2);

        ops.decrement(ino, seq1);
        assert!(ops.has_pending(ino));

        ops.decrement(ino, seq2);
        assert!(!ops.has_pending(ino));
    }

    #[test]
    fn test_wait_barrier_immediate() {
        let ops = PendingOps::new();
        let ino = 42;

        // Should return immediately when nothing is pending
        let barrier = ops.wait_barrier(ino);
        assert_eq!(barrier, 1);
    }

    #[test]
    fn test_wait_barrier_waits_for_preceding_only() {
        let ops = Arc::new(PendingOps::new());
        let ino = 42;

        // WRITE1 starts (seq=1, next_seq becomes 2)
        let seq1 = ops.increment(ino);
        assert_eq!(seq1, 1);

        // READ arrives - should wait for WRITE1
        let ops_clone = ops.clone();
        let waiter = thread::spawn(move || ops_clone.wait_barrier(ino));

        // Give waiter time to block
        thread::sleep(Duration::from_millis(50));
        assert!(!waiter.is_finished());

        // WRITE2 starts AFTER READ arrived (seq=2, next_seq becomes 3)
        let seq2 = ops.increment(ino);
        assert_eq!(seq2, 2);

        // Complete WRITE1 - READ should wake up (doesn't need to wait for WRITE2)
        ops.decrement(ino, seq1);

        let barrier = waiter.join().expect("waiter should complete");
        // barrier_seq was 2 when wait_barrier was called
        assert_eq!(barrier, 2);

        // WRITE2 still in flight
        assert!(ops.has_pending(ino));
        ops.decrement(ino, seq2);
        assert!(!ops.has_pending(ino));
    }

    #[test]
    fn test_fifo_order() {
        let ops = Arc::new(PendingOps::new());
        let ino = 42;

        // Simulate: WRITE1 → READ → WRITE2
        // READ should complete after WRITE1, before WRITE2

        let seq1 = ops.increment(ino); // WRITE1 arrives (seq=1)

        // READ arrives and waits
        let ops_for_read = ops.clone();
        let read_handle = thread::spawn(move || {
            let barrier = ops_for_read.wait_barrier(ino);
            // Return when we woke up
            barrier
        });

        thread::sleep(Duration::from_millis(30));

        let seq2 = ops.increment(ino); // WRITE2 arrives AFTER READ (seq=2)

        thread::sleep(Duration::from_millis(30));

        // WRITE1 completes
        ops.decrement(ino, seq1);

        // READ should wake up now, not waiting for WRITE2
        let read_barrier = read_handle.join().expect("read should complete");

        // WRITE2 still pending
        assert!(ops.has_pending(ino));
        assert_eq!(seq2, 2);

        ops.decrement(ino, seq2);
        assert!(!ops.has_pending(ino));

        // READ got barrier=2, meaning it waited for seq<2 (only WRITE1)
        assert_eq!(read_barrier, 2);
    }

    #[test]
    fn test_multiple_waiters_at_different_barriers() {
        let ops = Arc::new(PendingOps::new());
        let ino = 42;

        let seq1 = ops.increment(ino); // WRITE1 (seq=1)

        // READ1 waits for WRITE1
        let ops1 = ops.clone();
        let read1 = thread::spawn(move || ops1.wait_barrier(ino));

        thread::sleep(Duration::from_millis(20));

        let seq2 = ops.increment(ino); // WRITE2 (seq=2)

        // READ2 waits for WRITE1 and WRITE2
        let ops2 = ops.clone();
        let read2 = thread::spawn(move || ops2.wait_barrier(ino));

        thread::sleep(Duration::from_millis(20));

        // Complete WRITE1 - READ1 should wake, READ2 still waiting
        ops.decrement(ino, seq1);

        let b1 = read1.join().expect("read1 done");
        thread::sleep(Duration::from_millis(20));
        assert!(!read2.is_finished()); // READ2 still waiting for WRITE2

        // Complete WRITE2 - READ2 should wake
        ops.decrement(ino, seq2);
        let b2 = read2.join().expect("read2 done");

        assert!(b1 < b2); // READ1 had earlier barrier than READ2
    }

    #[test]
    fn test_independent_inodes() {
        let ops = Arc::new(PendingOps::new());

        let seq1 = ops.increment(1);
        let seq2 = ops.increment(2);

        // wait_barrier on ino 1 should not be affected by ino 2
        ops.decrement(1, seq1);
        ops.wait_barrier(1); // immediate

        assert!(ops.has_pending(2));
        ops.decrement(2, seq2);
    }
}
