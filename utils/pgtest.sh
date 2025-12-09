#!/bin/bash

# Fail fast and surface the failing command.
set -euo pipefail
trap 'echo "FAILED at line $LINENO: ${BASH_COMMAND}" >&2' ERR

pkill -9 postgres || true
pkill pbkfs || true
cargo build
export PATH=$PATH:/home/vbponomarev/postgres_bin/bin
target/debug/pbkfs cleanup --diff-dir /home/vbponomarev/pbkfs/postgres/pbk_diff --force

# Ensure clean mountpoint and diff dirs.
fusermount -uz /home/vbponomarev/pbkfs/postgres/pbk_target 2>/dev/null || true
umount -lf /home/vbponomarev/pbkfs/postgres/pbk_target 2>/dev/null || true
rm -rf /home/vbponomarev/pbkfs/postgres/pbk_target
mkdir -p /home/vbponomarev/pbkfs/postgres/pbk_target
mkdir -p /home/vbponomarev/pbkfs/postgres/pbk_diff

# Start mount in background and wait until the mountpoint is ready.
RUST_LOG=debug target/debug/pbkfs mount --pbk-store /home/vbponomarev/pbkfs/postgres/pbk_store/ \
  --mnt-path /home/vbponomarev/pbkfs/postgres/pbk_target/ \
  --diff-dir /home/vbponomarev/pbkfs/postgres/pbk_diff/ \
  --instance main --backup-id T68JRR > /tmp/pbkfs_debug.log 2>&1 &
mount_pid=$!
trap 'kill "$mount_pid" 2>/dev/null || true' EXIT

for i in {1..30}; do
  if mountpoint -q /home/vbponomarev/pbkfs/postgres/pbk_target; then
    break
  fi
  sleep 1
  if ! kill -0 "$mount_pid" 2>/dev/null; then
    echo "mount process exited prematurely" >&2
    wait "$mount_pid"
  fi
  if [[ $i -eq 30 ]]; then
    echo "mount did not become ready within 30s" >&2
    exit 1
  fi
done

chmod 700 /home/vbponomarev/pbkfs/postgres/pbk_target
pg_ctl -D /home/vbponomarev/pbkfs/postgres/pbk_target/ start
psql -c "select count(*) from pgbench_accounts;" pgbench
psql -c "select sum(abalance) from pgbench_accounts;" pgbench
psql -c "select count(*) from pgbench_accounts where abalance>0;" pgbench
psql -c "select sum(abalance) from pgbench_accounts where abalance>0" pgbench
export PGPASSWORD='password'
# testing for (3+3)*10=60 mins
for i in {1..10}; do
  pgbench --client=1 --jobs=1 --protocol=prepared --progress=3 --time=180 --username=pgbench --select-only --no-vacuum pgbench
  pgbench --client=1 --jobs=1 --protocol=prepared --progress=3 --time=180 --username=pgbench --no-vacuum pgbench
done
