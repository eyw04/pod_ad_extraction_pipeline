"""
Parse local RSS feed files and schedule the latest episode from each.

For each .rss.xml file in rss_feeds/:
  1. Map the file index to a podcast via castbox_to_rss.csv → rss_url → DB.
  2. Parse the XML and extract the most recent episode (first <item> with audio).
  3. Insert the episode into the episodes table if it doesn't already exist.
  4. Create an episode_schedule entry (skips duplicates).

Usage:
    python schedule_latest_episodes.py                          # schedule latest from all feeds
    python schedule_latest_episodes.py --days Mon,Wed,Fri       # custom schedule days
    python schedule_latest_episodes.py --limit 10               # only process first 10 feeds
    python schedule_latest_episodes.py --dry-run                # preview without writing
    python schedule_latest_episodes.py --rss-dir /path/to/feeds # custom feed directory
"""

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RSS_DIR = PROJECT_ROOT / "rss_feeds"
DEFAULT_MAPPING_CSV = PROJECT_ROOT / "data" / "castbox_to_rss.csv"
DEFAULT_DAYS = "Mon,Tue,Wed,Thu,Fri,Sat,Sun"


def make_episode_id(url: str) -> str:
    digest = hashlib.sha1(url.strip().encode("utf-8")).hexdigest()
    return f"ep_{digest}"


def extract_audio_url(item) -> str | None:
    """Extract the first audio URL from an RSS <item>."""
    enclosure = item.find("enclosure")
    if enclosure is not None:
        url = enclosure.get("url", "")
        enc_type = enclosure.get("type", "").lower()
        if url and ("audio" in enc_type or any(url.lower().endswith(ext) for ext in (".mp3", ".m4a", ".ogg", ".opus"))):
            return url

    for ns in ["{http://search.yahoo.com/mrss/}", ""]:
        media = item.find(f"{ns}content")
        if media is not None:
            url = media.get("url", "")
            if url and any(url.lower().endswith(ext) for ext in (".mp3", ".m4a")):
                return url

    return None


def parse_latest_episode(rss_path: Path) -> dict | None:
    """Parse a local RSS XML file and return info about the latest episode with audio."""
    try:
        tree = ET.parse(rss_path)
    except ET.ParseError:
        return None

    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        return None

    podcast_title_elem = channel.find("title")
    podcast_title = podcast_title_elem.text.strip() if podcast_title_elem is not None and podcast_title_elem.text else ""

    for item in channel.findall("item"):
        audio_url = extract_audio_url(item)
        if not audio_url:
            continue

        title_elem = item.find("title")
        title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        pub_date_elem = item.find("pubDate")
        pub_date = pub_date_elem.text.strip() if pub_date_elem is not None and pub_date_elem.text else None

        return {
            "audio_url": audio_url,
            "title": title,
            "pub_date": pub_date,
            "podcast_title": podcast_title,
            "episode_id": make_episode_id(audio_url),
        }

    return None


def load_index_to_rss_url(csv_path: Path) -> dict[int, str]:
    """Build a mapping from file index → rss_url using castbox_to_rss.csv."""
    mapping = {}
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            rss_url = (row.get("rss_url") or "").strip()
            if rss_url:
                mapping[idx] = rss_url
    return mapping


def main():
    parser = argparse.ArgumentParser(
        description="Parse local RSS feeds and schedule the latest episode from each",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument("--rss-dir", default=str(DEFAULT_RSS_DIR),
                        help="Directory containing .rss.xml files")
    parser.add_argument("--mapping-csv", default=str(DEFAULT_MAPPING_CSV),
                        help="CSV mapping file indices to rss_url (default: castbox_to_rss.csv)")
    parser.add_argument("--days", default=DEFAULT_DAYS,
                        help=f"Days of week for the schedule (default: {DEFAULT_DAYS})")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of feeds to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to the database")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    rss_dir = Path(args.rss_dir).expanduser().resolve()
    if not rss_dir.is_dir():
        print(f"Error: RSS directory not found: {rss_dir}", file=sys.stderr)
        sys.exit(1)

    mapping_csv = Path(args.mapping_csv).expanduser().resolve()
    if not mapping_csv.is_file():
        print(f"Error: mapping CSV not found: {mapping_csv}", file=sys.stderr)
        sys.exit(1)

    # Build file-index → rss_url mapping
    idx_to_rss = load_index_to_rss_url(mapping_csv)

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    # Build rss_url → podcast_id lookup from DB
    cur.execute("SELECT podcast_id, name, rss_url FROM podcasts WHERE rss_url IS NOT NULL AND rss_url != ''")
    rss_to_podcast = {}
    for pid, pname, rss_url in cur.fetchall():
        rss_to_podcast[rss_url] = (pid, pname)

    # Collect RSS feed files
    def _file_index(p: Path) -> int:
        num = p.name.split(".")[0]
        return int(num) if num.isdigit() else 0

    rss_files = sorted(rss_dir.glob("*.rss.xml"), key=_file_index)
    if args.limit:
        rss_files = rss_files[:args.limit]

    if not rss_files:
        print("No .rss.xml files found in the RSS directory.")
        conn.close()
        return

    print(f"Processing {len(rss_files)} RSS feed file(s)\n")

    stats = {"parsed": 0, "new_episodes": 0, "scheduled": 0, "already_scheduled": 0, "no_audio": 0, "no_podcast": 0, "parse_errors": 0}

    for file_idx, rss_file in enumerate(rss_files, 1):
        prefix = f"[{file_idx}/{len(rss_files)}]"
        num_str = rss_file.name.split(".")[0]
        file_num = int(num_str) if num_str.isdigit() else None

        # Look up podcast via file index → rss_url → DB
        rss_url = idx_to_rss.get(file_num) if file_num is not None else None
        podcast_id, podcast_name = rss_to_podcast.get(rss_url, (None, None)) if rss_url else (None, None)

        ep = parse_latest_episode(rss_file)

        if ep is None:
            short = podcast_name or rss_file.name
            print(f"{prefix} {short[:60]}: no audio episode found or parse error")
            stats["no_audio" if file_num is not None else "parse_errors"] += 1
            continue

        short_name = (podcast_name or ep["podcast_title"] or rss_file.name)[:60]

        if podcast_id is None:
            print(f"{prefix} {short_name}: no matching podcast in DB – skipping")
            stats["no_podcast"] += 1
            continue

        stats["parsed"] += 1
        eid = ep["episode_id"]

        # Insert episode if it doesn't exist
        cur.execute("SELECT 1 FROM episodes WHERE episode_id = ?", (eid,))
        is_new = cur.fetchone() is None

        if is_new:
            metadata = json.dumps({
                "podcast_title": podcast_name or ep["podcast_title"],
                "episode_title": ep["title"],
                "pub_date": ep["pub_date"],
            }, ensure_ascii=False, separators=(",", ":"))

            if not args.dry_run:
                cur.execute(
                    "INSERT INTO episodes (episode_id, podcast_id, title, url, metadata) VALUES (?, ?, ?, ?, ?)",
                    (eid, podcast_id, ep["title"], ep["audio_url"], metadata),
                )
            stats["new_episodes"] += 1

        # Create schedule if not already present
        cur.execute(
            "SELECT 1 FROM episode_schedules WHERE episode_id = ? AND days_of_week = ? AND is_enabled = 1",
            (eid, args.days),
        )
        already = cur.fetchone() is not None

        if already:
            print(f"{prefix} {short_name}: already scheduled – {ep['title'][:50]}")
            stats["already_scheduled"] += 1
        else:
            if not args.dry_run:
                cur.execute(
                    "INSERT INTO episode_schedules (episode_id, days_of_week) VALUES (?, ?)",
                    (eid, args.days),
                )
            action = "would schedule" if args.dry_run else "scheduled"
            new_tag = " (new episode)" if is_new else ""
            print(f"{prefix} {short_name}: {action}{new_tag} – {ep['title'][:50]}")
            stats["scheduled"] += 1

        if not args.dry_run:
            conn.commit()

    conn.close()

    print(f"\n{'=' * 55}")
    if args.dry_run:
        print("Dry-run complete (nothing written).")
    else:
        print("Scheduling complete!")
    print(f"  Feed files processed : {len(rss_files)}")
    print(f"  Parsed OK            : {stats['parsed']}")
    print(f"  New episodes added   : {stats['new_episodes']}")
    print(f"  Newly scheduled      : {stats['scheduled']}")
    print(f"  Already scheduled    : {stats['already_scheduled']}")
    print(f"  No audio found       : {stats['no_audio']}")
    print(f"  No podcast in DB     : {stats['no_podcast']}")
    print(f"  Parse errors         : {stats['parse_errors']}")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
