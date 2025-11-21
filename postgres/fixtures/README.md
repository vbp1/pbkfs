# pg_probackup Fixtures

This directory will contain small pg_probackup backup chains used by integration tests.

Suggested layout:

- `store/` — pg_probackup instance root with `backups/` inside
- `store/backups/<instance>/FULL1/` — base FULL backup
- `store/backups/<instance>/INC1/` — incremental backup referencing FULL1
- `store/backups/<instance>/INC2/` — incremental backup referencing INC1

Tests may mount these fixtures into `pbk_target` using `pbkfs mount` and write their diffs into `pbk_diff`.
