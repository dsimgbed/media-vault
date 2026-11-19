#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.backup import restore_backup_file, verify_backup_file
from app.config import Settings


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify or restore a Vault SQLite database backup (.db.gz or .db)."
    )
    parser.add_argument(
        "--backup",
        required=True,
        type=Path,
        help="Path to the .db.gz or .db backup file.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        type=Path,
        help="Target SQLite database path (defaults to Settings().db_path).",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Validate backup integrity without modifying the database.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip interactive confirmation prompt before restoring.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    backup_path = Path(args.backup)
    db_path = Path(args.db_path) if args.db_path is not None else Settings().db_path

    try:
        info = verify_backup_file(backup_path)
    except (ValueError, FileNotFoundError, OSError, sqlite3.Error) as exc:
        print(f"ERROR: Backup verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"Backup file   : {info['backup_path']}")
    print(f"Size (disk)   : {info['size_bytes']} bytes")
    print(f"Size (raw DB) : {info['uncompressed_bytes']} bytes")
    print(f"SHA-256       : {info['sha256']}")
    print(f"Media items   : {info['media_count']}")
    print(f"Ingest jobs   : {info['jobs_count']}")
    print(f"Tables        : {', '.join(info['tables'])}")

    if args.verify_only:
        print("Status        : VALID (verify-only; database not modified)")
        return 0

    print("")
    print(
        "WARNING: Ensure the Vault server is STOPPED before restoring so active "
        "connections do not hold locks or overwrite WAL state."
    )
    print(f"Target DB     : {db_path}")

    if not args.yes:
        try:
            answer = input("Proceed with database restore? [y/N]: ")
        except (EOFError, KeyboardInterrupt):
            print("\nRestore aborted by user.", file=sys.stderr)
            return 2
        if answer.strip().lower() not in {"y", "yes"}:
            print("Restore aborted by user.", file=sys.stderr)
            return 2

    try:
        result = restore_backup_file(backup_path, db_path, verify_only=False)
    except (ValueError, FileNotFoundError, OSError, sqlite3.Error, RuntimeError) as exc:
        print(f"ERROR: Restore failed: {exc}", file=sys.stderr)
        return 1

    print("")
    print("Status        : RESTORED")
    print(f"Restored DB   : {result['db_path']}")
    print(
        f"Pre-restore   : {result['pre_restore_backup'] or 'none (no existing DB)'}"
    )
    print(
        "Reminder      : Stale -wal/-shm sidecars were removed; you may now restart the server."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
