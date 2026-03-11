"""
Queue episode downloads into the download_tasks table.

Two modes:
  1. Schedule mode (default): reads episode_schedules for episodes due today
     based on the current day of the week, and queues them.
  2. File mode (--file): reads episode IDs from a text file, as before.

Usage:
    python queue_downloads.py                    # queue from schedules for today
    python queue_downloads.py --date 2026-02-16  # queue from schedules for a specific date
    python queue_downloads.py --file initial_episodes.txt  # queue from a text file
    python queue_downloads.py --dry-run          # preview without writing
"""

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def parse_episode_ids(txt_path: Path) -> list[str]:
    """Read episode IDs from a text file.

    Accepts plain one-id-per-line or tab/comma-separated where the first
    column is the episode_id.  Blank lines and # comments are skipped.
    """
    ids: list[str] = []
    with open(txt_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            eid = line.split("\t")[0].split(",")[0].strip()
            if eid:
                ids.append(eid)
    return ids


def get_scheduled_episodes(cur: sqlite3.Cursor, day_name: str) -> list[str]:
    """Return episode IDs from episode_schedules that are due on the given day."""
    cur.execute(
        """SELECT episode_id FROM episode_schedules
           WHERE is_enabled = 1 AND days_of_week LIKE ?""",
        (f"%{day_name}%",),
    )
    return [row[0] for row in cur.fetchall()]


def queue_episodes(cur, episode_ids: list[str], scheduled_date: str, dry_run: bool):
    """Insert episodes into download_tasks. Returns (inserted, skipped_dup, skipped_missing)."""
    inserted = 0
    skipped_dup = 0
    skipped_missing = 0

    for eid in episode_ids:
        # Verify episode exists
        cur.execute("SELECT 1 FROM episodes WHERE episode_id = ?", (eid,))
        if cur.fetchone() is None:
            print(f"  WARNING: episode '{eid}' not found in episodes table – skipping")
            skipped_missing += 1
            continue

        # Check for duplicate (unique index on episode_id + scheduled_date)
        cur.execute(
            "SELECT task_id, status FROM download_tasks WHERE episode_id = ? AND scheduled_date = ?",
            (eid, scheduled_date),
        )
        existing = cur.fetchone()
        if existing:
            task_id, status = existing
            status_names = {0: "READY", 1: "DOWNLOADING", 2: "WAITING", 3: "FINISHED", 4: "ERROR"}
            print(f"  SKIP: episode '{eid}' already queued for {scheduled_date} (task {task_id}, {status_names.get(status, status)})")
            skipped_dup += 1
            continue

        if dry_run:
            print(f"  WOULD queue: {eid} for {scheduled_date}")
        else:
            cur.execute(
                "INSERT INTO download_tasks (episode_id, scheduled_date, status) VALUES (?, ?, 0)",
                (eid, scheduled_date),
            )
        inserted += 1

    return inserted, skipped_dup, skipped_missing


def main():
    parser = argparse.ArgumentParser(
        description="Queue episode downloads into the download_tasks table",
    )
    parser.add_argument("--file", default=None,
                        help="Text file with episode IDs (overrides schedule-based mode)")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="Scheduled date for the tasks (default: today, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be inserted without writing",
    )
    args = parser.parse_args()

    # ── validate DB path ─────────────────────────────────────────────────
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    scheduled_date = args.date

    # ── get episode list ─────────────────────────────────────────────────
    if args.file:
        # File mode
        txt_path = Path(args.file).expanduser().resolve()
        if not txt_path.is_file():
            print(f"Error: file not found: {txt_path}", file=sys.stderr)
            sys.exit(1)
        episode_ids = parse_episode_ids(txt_path)
        if not episode_ids:
            print("No episode IDs found in the file.")
            conn.close()
            return
        print(f"Loaded {len(episode_ids)} episode ID(s) from {txt_path.name}")
    else:
        # Schedule mode — look up today's day of week
        target_date = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
        day_name = DAY_NAMES[target_date.weekday()]
        print(f"Checking schedules for {scheduled_date} ({day_name})")

        episode_ids = get_scheduled_episodes(cur, day_name)
        if not episode_ids:
            print(f"No episodes scheduled for {day_name}.")
            conn.close()
            return
        print(f"Found {len(episode_ids)} episode(s) scheduled for {day_name}")

    # ── queue episodes ───────────────────────────────────────────────────
    inserted, skipped_dup, skipped_missing = queue_episodes(
        cur, episode_ids, scheduled_date, args.dry_run,
    )

    if not args.dry_run:
        conn.commit()

    conn.close()

    # ── summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 50}")
    if args.dry_run:
        print("Dry-run complete (nothing written).")
    else:
        print("Queueing complete!")
    print(f"  Queued          : {inserted}")
    print(f"  Already queued  : {skipped_dup}")
    print(f"  Not in DB       : {skipped_missing}")
    print(f"  Total episodes  : {len(episode_ids)}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
