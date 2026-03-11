"""
Download podcast episodes from the download_tasks queue and track each
download in the database.

Picks up tasks with status 0 (READY) from the download_tasks table, then
for each one:

  1. Looks up the audio URL from the episodes table.
  2. Downloads the file with resume support (HTTP Range).
  3. Computes an MD5 checksum of the saved file.
  4. Inserts a row into the `downloads` table.
  5. Marks the download_task as FINISHED (status 3), or ERROR (status 4).

Usage:
    python download_episodes.py                # download all READY tasks
    python download_episodes.py --limit 20     # download up to 20 tasks
    python download_episodes.py --date 2026-02-14  # only tasks for a specific date
    python download_episodes.py --dry-run      # preview without downloading
"""

import argparse
import hashlib
import os
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")
DEFAULT_OUTPUT = Path("/shared/6/projects/podcast-ads/episode-downloads")
CHUNK_SIZE = 8192
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds


# ── helpers ──────────────────────────────────────────────────────────────────

def file_md5(path: str) -> str | None:
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def sanitize_filename(episode_id: str, url: str) -> str:
    """Derive a safe filename from episode_id + URL extension."""
    ext = "mp3"  # sensible default
    path_part = urlparse(url).path
    if "." in os.path.basename(path_part):
        candidate = path_part.rsplit(".", 1)[-1].lower()
        if candidate in ("mp3", "m4a", "wav", "ogg", "opus", "flac", "aac"):
            ext = candidate
    return f"{episode_id}.{ext}"


# ── download logic ───────────────────────────────────────────────────────────

def download_one(
    url: str,
    dest: Path,
    retries: int = MAX_RETRIES,
) -> tuple[int, str | None]:
    """Download *url* to *dest* with resume support.

    Returns (http_status_code, error_message_or_None).
    """
    for attempt in range(1, retries + 1):
        try:
            existing_size = dest.stat().st_size if dest.exists() else 0
            headers = {"Range": f"bytes={existing_size}-"} if existing_size else {}

            with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers) as resp:
                if resp.status_code == 416:
                    # Range not satisfiable → verify local file matches remote size
                    try:
                        head = requests.head(url, timeout=REQUEST_TIMEOUT)
                        remote_size = int(head.headers.get("Content-Length", -1))
                        local_size = dest.stat().st_size if dest.exists() else 0
                        if remote_size > 0 and local_size == remote_size:
                            return 200, None
                        # Size mismatch — re-download from scratch
                        print(f"    416 but size mismatch (local={local_size}, remote={remote_size}), re-downloading")
                        if dest.exists():
                            dest.unlink()
                        continue
                    except (requests.RequestException, OSError):
                        # Can't verify — re-download to be safe
                        print("    416 and HEAD check failed, re-downloading")
                        if dest.exists():
                            dest.unlink()
                        continue

                if resp.status_code not in (200, 206):
                    return resp.status_code, f"HTTP {resp.status_code}"

                mode = "ab" if resp.status_code == 206 else "wb"
                with open(dest, mode) as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

                return resp.status_code, None

        except requests.RequestException as exc:
            if attempt < retries:
                wait = RETRY_BACKOFF * attempt
                print(f"    retry {attempt}/{retries} in {wait}s – {exc}")
                time.sleep(wait)
            else:
                return 0, str(exc)

    return 0, "max retries exceeded"


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download podcast episodes from the download_tasks queue",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT),
        help="Directory to save audio files",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of tasks to process (default: all READY tasks)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Only process tasks for this scheduled_date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without actually downloading",
    )
    args = parser.parse_args()

    # ── validate paths ───────────────────────────────────────────────────
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir).expanduser().resolve()
    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    # ── open database ────────────────────────────────────────────────────
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    # ── fetch READY tasks ────────────────────────────────────────────────
    query = """
        SELECT t.task_id, t.episode_id, e.url, e.title
        FROM download_tasks t
        JOIN episodes e ON e.episode_id = t.episode_id
        WHERE t.status = 0
    """
    params: list = []

    if args.date:
        query += " AND t.scheduled_date = ?"
        params.append(args.date)

    query += " ORDER BY t.task_id"

    if args.limit:
        query += " LIMIT ?"
        params.append(args.limit)

    cur.execute(query, params)
    tasks = cur.fetchall()

    if not tasks:
        print("No READY tasks found in download_tasks.")
        conn.close()
        return

    print(f"Found {len(tasks)} READY task(s) to process")

    # ── process each task ────────────────────────────────────────────────
    stats = {"downloaded": 0, "failed": 0, "no_url": 0}

    for idx, (task_id, eid, url, title) in enumerate(tasks, 1):
        prefix = f"[{idx}/{len(tasks)}]"
        short_title = (title or eid)[:60]

        if not url:
            print(f"{prefix} {short_title} – no URL, skipping")
            cur.execute(
                "UPDATE download_tasks SET status = 4, error_message = 'no URL' WHERE task_id = ?",
                (task_id,),
            )
            conn.commit()
            stats["no_url"] += 1
            continue

        filename = sanitize_filename(eid, url)
        existing_file = out_dir / filename

        # Always download to a staging path first to avoid clobbering
        staging_dir = out_dir / "staging"
        staging_dest = staging_dir / filename

        if args.dry_run:
            print(f"{prefix} task {task_id}: {short_title} → {filename}")
            stats["downloaded"] += 1
            continue

        staging_dir.mkdir(parents=True, exist_ok=True)

        # ── mark as DOWNLOADING ──────────────────────────────────────
        cur.execute("UPDATE download_tasks SET status = 1 WHERE task_id = ?", (task_id,))
        conn.commit()

        # ── download to staging ──────────────────────────────────────
        print(f"{prefix} {short_title}")
        print(f"    url:  {url[:80]}...")

        status_code, error = download_one(url, staging_dest)

        if error:
            print(f"    FAILED: {error}")
            stats["failed"] += 1

            # Clean up partial staging file
            if staging_dest.exists():
                staging_dest.unlink()

            # Record the failed attempt
            cur.execute(
                """INSERT INTO downloads
                       (task_id, episode_id, file_path, status_code, error_message, attempt_number)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (task_id, eid, str(existing_file), status_code, error),
            )
            cur.execute(
                "UPDATE download_tasks SET status = 4, error_message = ? WHERE task_id = ?",
                (error, task_id),
            )
            conn.commit()
            continue

        # ── success: decide final destination ────────────────────────
        if existing_file.exists():
            # File already exists → move new copy to temp/
            temp_dir = out_dir / "temp"
            temp_dir.mkdir(parents=True, exist_ok=True)
            final_dest = temp_dir / filename
            staging_dest.rename(final_dest)
            print(f"    dest: {final_dest}  (existing file kept)")
        else:
            # No conflict → move from staging to main directory
            final_dest = existing_file
            staging_dest.rename(final_dest)
            print(f"    dest: {final_dest}")

        md5 = file_md5(str(final_dest))
        fsize = final_dest.stat().st_size
        print(f"    OK  {fsize / 1_048_576:.1f} MB  md5={md5}")

        cur.execute(
            """INSERT INTO downloads
                   (task_id, episode_id, md5_checksum, file_path,
                    download_version, attempt_number, status_code)
               VALUES (?, ?, ?, ?, 1, 1, ?)""",
            (task_id, eid, md5, str(final_dest), status_code),
        )
        cur.execute(
            "UPDATE download_tasks SET status = 3 WHERE task_id = ?",
            (task_id,),
        )
        conn.commit()
        stats["downloaded"] += 1

    # ── summary ──────────────────────────────────────────────────────────
    conn.close()

    print(f"\n{'=' * 50}")
    if args.dry_run:
        print("Dry-run complete (no files downloaded).")
    else:
        print("Download complete!")
    print(f"  Downloaded : {stats['downloaded']}")
    print(f"  Failed     : {stats['failed']}")
    print(f"  No URL     : {stats['no_url']}")
    print(f"  Total      : {len(tasks)}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
