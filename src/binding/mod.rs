//! Binding and lock management scaffolding.
//!
//! Handles on-disk binding metadata tying a diff directory to a specific
//! instance + backup id as well as simple lock helpers.

pub mod lock;

pub use lock::{BindingRecord, BindingState, DiffDir, LockMarker, BINDING_FILE, LOCK_FILE};
