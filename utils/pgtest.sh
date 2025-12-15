#!/bin/bash

# Fail fast and surface the failing command.
set -euo pipefail
trap 'echo "FAILED at line $LINENO: ${BASH_COMMAND}" >&2' ERR

print_help() {
  cat >&2 <<'EOF'
utils/pgtest.sh: smoke-test pgbench against a pbkfs-mounted backup

Required env:
  PBK_INSTANCE=<instance-name>   # e.g. main-checksums
  PBK_BACKUP_ID=<backup-id>      # e.g. T6ZTYL

Optional env:
  POSTGRES_BIN_DIR=<dir>         # adds <dir> to PATH (should contain pg_ctl/psql/pgbench)
  PBK_STORE_REL=postgres/pbk_store
  PBK_TARGET_REL=postgres/pbk_target
  PBK_DIFF_REL=postgres/pbk_diff
  PBKFS_LOG=/tmp/pbkfs_debug.log
  PBK_MOUNT_TIMEOUT_SECS=30

Example:
  PBK_INSTANCE=main-checksums PBK_BACKUP_ID=T6ZTYL POSTGRES_BIN_DIR=$HOME/postgres_bin/bin ./utils/pgtest.sh
EOF
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
PBKFS_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"

PBK_STORE_REL="${PBK_STORE_REL:-postgres/pbk_store}"
PBK_TARGET_REL="${PBK_TARGET_REL:-postgres/pbk_target}"
PBK_DIFF_REL="${PBK_DIFF_REL:-postgres/pbk_diff}"
PBKFS_LOG="${PBKFS_LOG:-/tmp/pbkfs_debug.log}"
PBK_MOUNT_TIMEOUT_SECS="${PBK_MOUNT_TIMEOUT_SECS:-30}"

if [[ ! -f "${PBKFS_ROOT}/Cargo.toml" ]]; then
  echo "ERROR: pbkfs repo root not found (Cargo.toml missing at ${PBKFS_ROOT})." >&2
  print_help
  exit 2
fi

if [[ -z "${PBK_INSTANCE:-}" || -z "${PBK_BACKUP_ID:-}" ]]; then
  echo "ERROR: PBK_INSTANCE and PBK_BACKUP_ID must be set." >&2
  print_help
  exit 2
fi

if [[ -n "${POSTGRES_BIN_DIR:-}" ]]; then
  export PATH="${PATH}:${POSTGRES_BIN_DIR}"
fi

PBK_STORE_DIR="${PBKFS_ROOT}/${PBK_STORE_REL}"
PBK_TARGET_DIR="${PBKFS_ROOT}/${PBK_TARGET_REL}"
PBK_DIFF_DIR="${PBKFS_ROOT}/${PBK_DIFF_REL}"

if [[ ! -d "${PBK_STORE_DIR}" ]]; then
  echo "ERROR: PBK_STORE_DIR not found: ${PBK_STORE_DIR}" >&2
  print_help
  exit 2
fi

cd "${PBKFS_ROOT}"

pkill -9 postgres || true
pkill pbkfs || true
cargo build
target/debug/pbkfs cleanup --diff-dir "${PBK_DIFF_DIR}" --force

# Ensure clean mountpoint and diff dirs.
fusermount -uz "${PBK_TARGET_DIR}" 2>/dev/null || true
umount -lf "${PBK_TARGET_DIR}" 2>/dev/null || true
rm -rf "${PBK_TARGET_DIR}"
mkdir -p "${PBK_TARGET_DIR}"
mkdir -p "${PBK_DIFF_DIR}"

# Start mount in background and wait until the mountpoint is ready.
: >"${PBKFS_LOG}"
echo "Starting pbkfs mount; log: ${PBKFS_LOG}" >&2
RUST_LOG="${RUST_LOG:-debug}" target/debug/pbkfs mount --pbk-store "${PBK_STORE_DIR}/" \
  --mnt-path "${PBK_TARGET_DIR}/" \
  --diff-dir "${PBK_DIFF_DIR}/" \
  --no-wal --wait \
  --instance "${PBK_INSTANCE}" --backup-id "${PBK_BACKUP_ID}" >"${PBKFS_LOG}" 2>&1 &
mount_pid=$!
trap 'kill "$mount_pid" 2>/dev/null || true' EXIT

for ((i = 1; i <= PBK_MOUNT_TIMEOUT_SECS; i++)); do
  if mountpoint -q "${PBK_TARGET_DIR}"; then
    break
  fi
  if ! kill -0 "$mount_pid" 2>/dev/null; then
    set +e
    wait "$mount_pid"
    rc=$?
    set -e
    if mountpoint -q "${PBK_TARGET_DIR}"; then
      break
    fi
    echo "pbkfs mount exited before mountpoint became ready (exit code: ${rc})." >&2
    echo "Last 200 lines from ${PBKFS_LOG}:" >&2
    tail -n 200 "${PBKFS_LOG}" >&2 || true
    exit "${rc}"
  fi
  sleep 1
done

if ! mountpoint -q "${PBK_TARGET_DIR}"; then
  echo "mount did not become ready within ${PBK_MOUNT_TIMEOUT_SECS}s" >&2
  echo "Last 200 lines from ${PBKFS_LOG}:" >&2
  tail -n 200 "${PBKFS_LOG}" >&2 || true
  exit 1
fi

chmod 700 "${PBK_TARGET_DIR}"
pg_ctl -D "${PBK_TARGET_DIR}/" start
time psql -c "select count(*) from pgbench_accounts;" pgbench
time psql -c "select sum(abalance) from pgbench_accounts;" pgbench
time psql -c "select count(*) from pgbench_accounts where abalance>0;" pgbench
time psql -c "select sum(abalance) from pgbench_accounts where abalance>0" pgbench
export PGPASSWORD='password'
# testing for (2+2)*3=12 mins
for i in {1..3}; do
  pgbench --client=1 --jobs=1 --protocol=prepared --progress=3 --time=120 --username=pgbench --select-only --no-vacuum pgbench
  pgbench --client=1 --jobs=1 --protocol=prepared --progress=3 --time=120 --username=pgbench --no-vacuum pgbench
done

pg_ctl -D "${PBK_TARGET_DIR}/" stop -m fast >/dev/null 2>&1 || true
set +e
target/debug/pbkfs unmount --mnt-path "${PBK_TARGET_DIR}" --diff-dir "${PBK_DIFF_DIR}"
unmount_rc=$?
set -e
if [[ ${unmount_rc} -ne 0 ]]; then
  echo "ERROR: pbkfs unmount failed (exit code: ${unmount_rc})" >&2
  mount | grep -F -- "${PBK_TARGET_DIR}" >&2 || true
  exit "${unmount_rc}"
fi
for _ in {1..10}; do
  if ! mountpoint -q "${PBK_TARGET_DIR}"; then
    break
  fi
  sleep 1
done
if mountpoint -q "${PBK_TARGET_DIR}"; then
  echo "ERROR: failed to unmount ${PBK_TARGET_DIR}" >&2
  mount | grep -F -- "${PBK_TARGET_DIR}" >&2 || true
  exit 1
fi
