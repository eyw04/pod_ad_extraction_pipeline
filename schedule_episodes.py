"""
Read a CSV file and create episode_schedule entries in pipeline.db.

CSV format (header required):
  Option A – schedule by podcast name (schedules ALL episodes for that podcast):
      podcast_name,days_of_week
      The Joe Rogan Experience,"Mon,Wed,Fri"

  Option B – schedule individual episodes:
      episode_id,days_of_week
      a7d449165e25aa211e234781,"Mon,Wed,Fri"

  You can mix both styles in the same CSV; rows with an `episode_id` column
  value are used directly, otherwise `podcast_name` is looked up.

Usage:
    python schedule_episodes.py <csv_file> [--db pipeline.db] [--dry-run]
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")


def resolve_episode_ids(cur: sqlite3.Cursor, row: dict) -> list[str]:
    """Return a list of episode_ids for the given CSV row."""
    episode_id = (row.get("episode_id") or "").strip()
    if episode_id:
        # Verify it exists
        cur.execute("SELECT 1 FROM episodes WHERE episode_id = ?", (episode_id,))
        if cur.fetchone() is None:
            print(f"  WARNING: episode_id '{episode_id}' not found – skipping")
            return []
        return [episode_id]

    podcast_name = (row.get("podcast_name") or "").strip()
    if not podcast_name:
        print("  WARNING: row has neither episode_id nor podcast_name – skipping")
        return []

    # Look up podcast_id by name (case-insensitive)
    cur.execute(
        "SELECT podcast_id FROM podcasts WHERE LOWER(name) = LOWER(?)",
        (podcast_name,),
    )
    matches = cur.fetchall()
    if not matches:
        print(f"  WARNING: podcast '{podcast_name}' not found – skipping")
        return []
    if len(matches) > 1:
        print(f"  WARNING: multiple podcasts match '{podcast_name}' – using first")

    podcast_id = matches[0][0]
    cur.execute(
        "SELECT episode_id FROM episodes WHERE podcast_id = ?", (podcast_id,)
    )
    ids = [r[0] for r in cur.fetchall()]
    if not ids:
        print(f"  WARNING: podcast '{podcast_name}' has 0 episodes – skipping")
    return ids


def main():
    parser = argparse.ArgumentParser(description="Schedule episode downloads from CSV")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Path to the SQLite DB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be inserted without writing",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_file).expanduser().resolve()
    if not csv_path.is_file():
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate header
        fields = set(reader.fieldnames or [])
        if "days_of_week" not in fields:
            print(
                "Error: CSV must have a 'days_of_week' column",
                file=sys.stderr,
            )
            sys.exit(1)
        if "episode_id" not in fields and "podcast_name" not in fields:
            print(
                "Error: CSV must have either 'episode_id' or 'podcast_name' column",
                file=sys.stderr,
            )
            sys.exit(1)

        total_inserted = 0
        total_skipped = 0

        for row_num, row in enumerate(reader, start=2):  # row 1 is the header
            days_of_week = (row.get("days_of_week") or "").strip()
            if not days_of_week:
                print(f"  Row {row_num}: empty days_of_week – skipping")
                total_skipped += 1
                continue

            episode_ids = resolve_episode_ids(cur, row)
            label = row.get("podcast_name") or row.get("episode_id") or "?"

            if not episode_ids:
                total_skipped += 1
                continue

            # Skip episodes that already have an enabled schedule with the same days
            new_ids = []
            for eid in episode_ids:
                cur.execute(
                    """SELECT 1 FROM episode_schedules
                       WHERE episode_id = ? AND days_of_week = ? AND is_enabled = 1""",
                    (eid, days_of_week),
                )
                if cur.fetchone():
                    continue
                new_ids.append(eid)

            skipped_dups = len(episode_ids) - len(new_ids)
            if skipped_dups:
                print(
                    f"  Row {row_num} ({label}): {skipped_dups} already scheduled – skipping duplicates"
                )

            if not new_ids:
                total_skipped += 1
                continue

            if args.dry_run:
                print(
                    f"  Row {row_num} ({label}): would schedule {len(new_ids)} episode(s) on [{days_of_week}]"
                )
            else:
                cur.executemany(
                    """INSERT INTO episode_schedules (episode_id, days_of_week)
                       VALUES (?, ?)""",
                    [(eid, days_of_week) for eid in new_ids],
                )
                print(
                    f"  Row {row_num} ({label}): scheduled {len(new_ids)} episode(s) on [{days_of_week}]"
                )

            total_inserted += len(new_ids)

    if args.dry_run:
        print(f"\nDry run complete. Would insert {total_inserted} schedule(s).")
    else:
        conn.commit()
        print(f"\nDone. Inserted {total_inserted} schedule(s), skipped {total_skipped} row(s).")

    # Show current totals
    cur.execute("SELECT COUNT(*) FROM episode_schedules")
    count = cur.fetchone()[0]
    print(f"Total episode_schedules in DB: {count}")

    conn.close()


if __name__ == "__main__":
    main()
