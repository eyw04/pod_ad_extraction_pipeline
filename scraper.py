# -*- coding: utf-8 -*-
"""
Scrape local RSS XML feeds (rss_feeds/*.rss.xml) and populate the
podcasts / episodes / download_tasks tables defined in schema.sql.
"""

import csv
import hashlib
import json
import os
import sqlite3
import xml.etree.ElementTree as ET
from datetime import date

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = "/shared/6/projects/podcast-ads/pipeline.db"
RSS_FOLDER = os.path.join(CURRENT_DIR, "rss_feeds")
MOST_SUBSCRIBED_CSV = os.path.join(CURRENT_DIR, "most_subscribed.csv")
CASTBOX_TO_RSS_CSV = os.path.join(CURRENT_DIR, "castbox_to_rss.csv")

# XML namespaces used in podcast RSS feeds
NS = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "atom": "http://www.w3.org/2005/Atom",
    "media": "http://search.yahoo.com/mrss/",
    "podcast": "https://podcastindex.org/namespace/1.0",
    "googleplay": "http://www.google.com/schemas/play-podcasts/1.0",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _text(element, tag, namespaces=None):
    """Return the text of a child element, or None."""
    child = element.find(tag, namespaces or NS)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _make_podcast_id(rss_url, castbox_url=None):
    """Deterministic podcast id from the RSS URL (or castbox URL as fallback)."""
    source = rss_url or castbox_url or ""
    return hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]


def _make_episode_id(podcast_id, raw_guid):
    """Namespace a raw guid with the podcast_id so episode_ids are globally unique."""
    composite = f"{podcast_id}:{raw_guid}"
    return hashlib.sha256(composite.encode("utf-8")).hexdigest()[:24]


def _extract_categories(channel):
    """Return a JSON array of itunes:category text values."""
    cats = []
    for cat_el in channel.findall("itunes:category", NS):
        text = cat_el.get("text")
        if text:
            cats.append(text)
        # nested sub-categories
        for sub in cat_el.findall("itunes:category", NS):
            sub_text = sub.get("text")
            if sub_text:
                cats.append(sub_text)
    return json.dumps(cats, ensure_ascii=False) if cats else None


def _extract_guid(item):
    """Extract the <guid> text from an <item>."""
    guid_el = item.find("guid")
    if guid_el is not None and guid_el.text:
        return guid_el.text.strip()
    return None


def _extract_enclosure_url(item):
    """Extract the audio URL from <enclosure url="..."/>."""
    enc = item.find("enclosure")
    if enc is not None:
        return enc.get("url")
    return None


def _episode_metadata(item):
    """Build a small metadata dict from useful item-level fields."""
    meta = {}
    desc = _text(item, "description")
    if desc:
        meta["description"] = desc
    pub_date = _text(item, "pubDate")
    if pub_date:
        meta["pub_date"] = pub_date
    duration = _text(item, "itunes:duration", NS)
    if duration:
        meta["duration"] = duration
    ep_type = _text(item, "itunes:episodeType", NS)
    if ep_type:
        meta["episode_type"] = ep_type
    episode_num = _text(item, "itunes:episode", NS)
    if episode_num:
        meta["episode_number"] = episode_num
    explicit = _text(item, "itunes:explicit", NS)
    if explicit:
        meta["explicit"] = explicit
    link = _text(item, "link")
    if link:
        meta["link"] = link
    enc = item.find("enclosure")
    if enc is not None:
        length = enc.get("length")
        if length:
            meta["enclosure_length"] = length
        mime = enc.get("type")
        if mime:
            meta["enclosure_type"] = mime
    return meta


# ---------------------------------------------------------------------------
# CSV loaders
# ---------------------------------------------------------------------------

def load_castbox_csv():
    """Return {row_index: row_dict} from castbox_to_rss.csv."""
    mapping = {}
    if not os.path.exists(CASTBOX_TO_RSS_CSV):
        return mapping
    with open(CASTBOX_TO_RSS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            mapping[i] = row
    return mapping


def load_most_subscribed_csv():
    """Return {row_index: row_dict} from most_subscribed.csv."""
    mapping = {}
    if not os.path.exists(MOST_SUBSCRIBED_CSV):
        return mapping
    with open(MOST_SUBSCRIBED_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            mapping[i] = row
    return mapping


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def init_db(conn):
    """Create tables from schema.sql if they don't already exist."""
    schema_path = os.path.join(CURRENT_DIR, "schema.sql")
    if os.path.exists(schema_path):
        with open(schema_path, encoding="utf-8") as f:
            conn.executescript(f.read())
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")


def upsert_podcast(cursor, podcast_id, name, author, rss_url, publisher,
                   language, categories_json, metadata_json, episode_count):
    """Insert or update a podcast row."""
    cursor.execute(
        """
        INSERT INTO podcasts
            (podcast_id, name, author, rss_url, publisher,
             language, categories, metadata, episode_count, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(podcast_id) DO UPDATE SET
            name           = COALESCE(excluded.name, podcasts.name),
            author         = COALESCE(excluded.author, podcasts.author),
            rss_url        = COALESCE(excluded.rss_url, podcasts.rss_url),
            publisher      = COALESCE(excluded.publisher, podcasts.publisher),
            language       = COALESCE(excluded.language, podcasts.language),
            categories     = COALESCE(excluded.categories, podcasts.categories),
            metadata       = COALESCE(excluded.metadata, podcasts.metadata),
            episode_count  = COALESCE(excluded.episode_count, podcasts.episode_count),
            last_updated   = CURRENT_TIMESTAMP
        """,
        (podcast_id, name, author, rss_url, publisher,
         language, categories_json, metadata_json, episode_count),
    )


def upsert_episode(cursor, episode_id, podcast_id, title, url, metadata_json):
    """Insert or update an episode row."""
    cursor.execute(
        """
        INSERT INTO episodes
            (episode_id, podcast_id, title, url, metadata, last_updated)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(episode_id) DO UPDATE SET
            podcast_id   = COALESCE(excluded.podcast_id, episodes.podcast_id),
            title        = COALESCE(excluded.title, episodes.title),
            url          = COALESCE(excluded.url, episodes.url),
            metadata     = COALESCE(excluded.metadata, episodes.metadata),
            last_updated = CURRENT_TIMESTAMP
        """,
        (episode_id, podcast_id, title, url, metadata_json),
    )


def insert_download_task(cursor, episode_id, scheduled_date):
    """Insert a download task (READY) if one doesn't exist for this episode+date."""
    cursor.execute(
        """
        INSERT OR IGNORE INTO download_tasks
            (episode_id, scheduled_date, status)
        VALUES (?, ?, 0)
        """,
        (episode_id, scheduled_date),
    )


# ---------------------------------------------------------------------------
# Core parsing
# ---------------------------------------------------------------------------

def parse_rss_file(file_path, file_index, castbox_row, subscribed_row):
    """
    Parse a single RSS XML file and return:
        podcast_dict   – fields for the podcasts table
        episodes_list  – list of dicts for the episodes table
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    channel = root.find("channel")
    if channel is None:
        print(f"  [{file_index}] No <channel> found, skipping")
        return None, []

    # --- Podcast-level fields ---
    rss_url = None
    if castbox_row:
        rss_url = castbox_row.get("rss_url") or None
    # Fallback: atom:link[@rel='self']
    if not rss_url:
        atom_link = channel.find("atom:link[@rel='self']", NS)
        if atom_link is not None:
            rss_url = atom_link.get("href")

    castbox_url = None
    if castbox_row:
        castbox_url = castbox_row.get("channel_url")
    elif subscribed_row:
        castbox_url = subscribed_row.get("channel_url")

    podcast_id = _make_podcast_id(rss_url, castbox_url)

    name = _text(channel, "title")
    author = _text(channel, "itunes:author", NS)
    language = _text(channel, "language")

    # publisher: prefer itunes:owner/itunes:name, fall back to author
    owner_el = channel.find("itunes:owner", NS)
    publisher = None
    if owner_el is not None:
        publisher = _text(owner_el, "itunes:name", NS)
    if not publisher:
        publisher = author

    categories_json = _extract_categories(channel)

    # Build channel-level metadata blob
    channel_meta = {}
    desc = _text(channel, "description")
    if desc:
        channel_meta["description"] = desc
    copyright_text = _text(channel, "copyright")
    if copyright_text:
        channel_meta["copyright"] = copyright_text
    explicit = _text(channel, "itunes:explicit", NS)
    if explicit:
        channel_meta["explicit"] = explicit
    itunes_type = _text(channel, "itunes:type", NS)
    if itunes_type:
        channel_meta["itunes_type"] = itunes_type
    img_el = channel.find("itunes:image", NS)
    if img_el is not None:
        channel_meta["image_url"] = img_el.get("href")
    link = _text(channel, "link")
    if link:
        channel_meta["link"] = link
    # Include castbox stats if available
    if subscribed_row:
        for key in ("subscribed_count", "played_count"):
            val = subscribed_row.get(key)
            if val:
                channel_meta[key] = val

    metadata_json = json.dumps(channel_meta, ensure_ascii=False) if channel_meta else None

    items = channel.findall("item")
    episode_count = len(items)

    podcast_dict = {
        "podcast_id": podcast_id,
        "name": name,
        "author": author,
        "rss_url": rss_url,
        "publisher": publisher,
        "language": language,
        "categories": categories_json,
        "metadata": metadata_json,
        "episode_count": episode_count,
    }

    # --- Episode-level fields ---
    episodes_list = []
    for item in items:
        audio_url = _extract_enclosure_url(item)
        if not audio_url:
            continue

        guid = _extract_guid(item)
        # Fallback chain for raw guid, then hash with podcast_id for uniqueness
        raw_guid = guid or _text(item, "link") or audio_url
        if not raw_guid:
            continue
        episode_id = _make_episode_id(podcast_id, raw_guid)

        title = _text(item, "title")
        ep_meta = _episode_metadata(item)
        ep_meta_json = json.dumps(ep_meta, ensure_ascii=False) if ep_meta else None

        episodes_list.append({
            "episode_id": episode_id,
            "podcast_id": podcast_id,
            "title": title,
            "url": audio_url,
            "metadata": ep_meta_json,
        })

    return podcast_dict, episodes_list


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_scraper():
    castbox_map = load_castbox_csv()
    subscribed_map = load_most_subscribed_csv()

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    cursor = conn.cursor()

    today = date.today().isoformat()

    # Discover all RSS XML files
    rss_files = sorted(
        f for f in os.listdir(RSS_FOLDER)
        if f.endswith(".rss.xml")
    )
    print(f"Found {len(rss_files)} RSS feed files in {RSS_FOLDER}")

    total_podcasts = 0
    total_episodes = 0
    total_tasks = 0

    for filename in rss_files:
        # Extract the numeric index from e.g. "42.rss.xml"
        stem = filename.replace(".rss.xml", "")
        try:
            file_index = int(stem)
        except ValueError:
            print(f"  Skipping non-numeric file: {filename}")
            continue

        file_path = os.path.join(RSS_FOLDER, filename)
        castbox_row = castbox_map.get(file_index)
        subscribed_row = subscribed_map.get(file_index)

        try:
            podcast, episodes = parse_rss_file(
                file_path, file_index, castbox_row, subscribed_row,
            )
        except ET.ParseError as e:
            print(f"  [{file_index}] XML parse error: {e}")
            continue

        if podcast is None:
            continue

        # --- Write to DB ---
        upsert_podcast(
            cursor,
            podcast["podcast_id"],
            podcast["name"],
            podcast["author"],
            podcast["rss_url"],
            podcast["publisher"],
            podcast["language"],
            podcast["categories"],
            podcast["metadata"],
            podcast["episode_count"],
        )
        total_podcasts += 1

        for ep in episodes:
            upsert_episode(
                cursor,
                ep["episode_id"],
                ep["podcast_id"],
                ep["title"],
                ep["url"],
                ep["metadata"],
            )
            insert_download_task(cursor, ep["episode_id"], today)
            total_episodes += 1
            total_tasks += 1

        # Commit per-feed to avoid holding a huge transaction
        conn.commit()

        if (total_podcasts % 100) == 0:
            print(f"  Progress: {total_podcasts} podcasts, {total_episodes} episodes")

    conn.close()
    print(
        f"\nDone. Inserted/updated {total_podcasts} podcasts, "
        f"{total_episodes} episodes, {total_tasks} download tasks."
    )


if __name__ == "__main__":
    run_scraper()
