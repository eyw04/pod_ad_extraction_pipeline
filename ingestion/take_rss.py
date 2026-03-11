import os
import time
import hashlib
import requests
import pandas as pd
from pathlib import Path
from difflib import SequenceMatcher
from urllib.parse import quote_plus
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_IN = str(PROJECT_ROOT / "data" / "most_subscribed.csv")
CSV_OUT = str(PROJECT_ROOT / "data" / "castbox_to_rss.csv")

BASE = "https://api.podcastindex.org/api/1.0"

load_dotenv(PROJECT_ROOT / ".env")

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

def auth_headers():
    # Podcast Index auth: sha1(key + secret + epochSeconds)
    now = str(int(time.time()))
    token = hashlib.sha1((API_KEY + API_SECRET + now).encode("utf-8")).hexdigest()
    return {
        "User-Agent": "podcast-ads-rss-resolver/0.1",
        "X-Auth-Date": now,
        "X-Auth-Key": API_KEY,
        "Authorization": token,
    }

def sim(a, b):
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()

def best_match(feeds, title, author):
    best = None
    best_score = -1.0
    for f in feeds:
        t = f.get("title", "") or ""
        a = f.get("author", "") or ""
        score = 0.75 * sim(title, t) + 0.25 * sim(author, a)
        if score > best_score:
            best_score = score
            best = f
    return best, best_score

def resolve_rss(title, author):
    q = f"{title} {author}".strip()
    url = f"{BASE}/search/byterm?q={quote_plus(q)}"
    r = requests.get(url, headers=auth_headers(), timeout=30)
    r.raise_for_status()
    data = r.json()
    feeds = data.get("feeds", []) or []
    if not feeds:
        return None, None, None, None

    m, score = best_match(feeds, title, author)
    return (
        m.get("url"),          # RSS feed URL (often called "url" or "feedUrl" depending on endpoint/field)
        score,
        m.get("title"),
        m.get("author"),
    )

def main():
    df = pd.read_csv(CSV_IN)
    rss_dir = PROJECT_ROOT / "rss_feeds"
    os.makedirs(rss_dir, exist_ok=True)

    out_rows = []
    for i, row in df.iterrows():
        title = str(row["title"]) if pd.notna(row["title"]) else ""
        author = str(row["author"]) if pd.notna(row["author"]) else ""

        rss, score, mt, ma = resolve_rss(title, author)

        out_rows.append({
            "channel_url": row["channel_url"],
            "title": title,
            "author": author,
            "rss_url": rss,
            "confidence_score": score,
            "api_title": mt,
            "api_author": ma,
        })

        if rss and str(rss).strip().startswith("http"):
            try:
                resp = requests.get(str(rss).strip(), timeout=30)
                resp.raise_for_status()
                (rss_dir / f"{i}.rss.xml").write_text(resp.text, encoding="utf-8", errors="replace")
            except Exception as e:
                print(f"  [{i}] download failed: {e}")

        # be nice to rate limits
        time.sleep(1)

        if (i + 1) % 50 == 0:
            print(f"Resolved {i+1}/{len(df)}...")

    pd.DataFrame(out_rows).to_csv(CSV_OUT, index=False)
    print("Wrote:", CSV_OUT)

if __name__ == "__main__":
    main()