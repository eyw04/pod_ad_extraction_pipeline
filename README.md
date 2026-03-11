# Podcast Ad Detection Pipeline

An end-to-end system for automatically detecting and extracting dynamically inserted advertisements from podcast episodes. The pipeline repeatedly downloads the same episode over time, compares versions at the MP3 frame level, and isolates segments that differ between downloads -- these are the dynamically inserted ads. Extracted ads are transcribed and stored in a structured database.

## How It Works

Podcast networks use **Dynamic Ad Insertion (DAI)** to swap out ad segments in episodes after publication. By downloading the same episode at different points in time, the ad slots change while the editorial content stays the same. This pipeline exploits that property:

1. **Ingest** podcast metadata from RSS feeds and resolve Castbox URLs to standard RSS
2. **Schedule** recurring downloads of target episodes
3. **Download** episodes on a daily cadence, keeping both the original and a fresh re-download
4. **Compare** the two versions using frame-level MP3 alignment (via `SequenceMatcher`) to find non-matching gaps
5. **Extract** the differing segments as standalone audio files (the ads)
6. **Transcribe** each ad segment using [faster-whisper](https://github.com/SYSTRAN/faster-whisper)
7. **Store** timing, transcripts, and file paths in a SQLite database

## Project Structure

```
.
├── pipeline/                     # Core daily pipeline
│   ├── run_pipeline.py           # Orchestrator: queue → download → extract
│   ├── run_pipeline_daily.sh     # Cron wrapper with logging
│   ├── queue_downloads.py        # Queue today's scheduled episodes
│   ├── download_episodes.py      # Download queued episodes with resume support
│   └── compare_and_extract_ads.py # Frame-level comparison & ad extraction
│
├── ingestion/                    # RSS ingestion & scheduling
│   ├── take_rss.py               # Resolve Castbox podcast URLs to RSS via Podcast Index API
│   ├── scraper.py                # Parse RSS XML feeds into the database
│   ├── schedule_episodes.py      # Create download schedules from a CSV
│   └── schedule_latest_episodes.py # Auto-schedule the latest episode from each feed
│
├── analysis/                     # Post-processing & analysis tools
│   ├── plot_ad_stats.py          # Plot ad timing distributions
│   ├── split_ad_segments.py      # Split concatenated ad segments into individual ads
│   └── transcribe.py             # Batch transcribe audio files with faster-whisper
│
├── scripts/                      # Standalone utilities
│   ├── download_mp3.py           # Download audio from a CSV of URLs
│   ├── get_mp3_urls.py           # Extract MP3 URLs from RSS feed files
│   ├── sample_mp3.py             # Sample one episode per podcast
│   ├── task_worker.py            # Alternative batch download worker
│   └── transcribe.sh             # SLURM job script for GPU transcription
│
├── data/                         # Reference data files
│   ├── castbox_to_rss.csv        # Castbox → RSS URL mapping with confidence scores
│   ├── most_subscribed.csv       # Top Castbox podcasts by subscriber count
│   ├── schedule.csv              # Episode download schedule definitions
│   └── initial_episodes.txt      # Initial episode IDs for bootstrapping
│
├── Podcast-Ad-Detection/         # Submodule: MP3 frame parsing, alignment, & dataset building
├── schema.sql                    # SQLite schema for all pipeline tables
├── requirements.txt              # Python dependencies
└── .gitignore
```

### Generated directories (gitignored)

| Directory    | Contents |
|-------------|----------|
| `rss_feeds/` | Downloaded RSS XML files (one per podcast) |
| `logs/`      | Daily pipeline run logs |
| `plots/`     | Alignment visualization plots |
| `split/`     | Ad segments split into individual ads |

## Database Schema

The pipeline uses a SQLite database with these tables:

| Table | Purpose |
|-------|---------|
| `podcasts` | Show-level metadata (name, author, RSS URL, language, categories) |
| `episodes` | Episode-level metadata (title, audio URL, podcast foreign key) |
| `episode_schedules` | Recurring download schedules per episode (days of week) |
| `download_tasks` | Task queue: one row per scheduled download (status: READY → DOWNLOADING → FINISHED/ERROR) |
| `downloads` | Download attempt log (file path, MD5 checksum, version, HTTP status) |
| `ads` | Extracted ad segments (timing, transcript, file path) |

## Setup

```bash
git clone --recurse-submodules <repo-url>
cd infra
pip install -r requirements.txt
```

You also need `ffmpeg` installed for audio processing.

For RSS feed resolution via the Podcast Index API, create a `.env` file:

```
API_KEY=your_podcast_index_api_key
API_SECRET=your_podcast_index_api_secret
```

## Usage

### Run the full daily pipeline

```bash
python pipeline/run_pipeline.py
python pipeline/run_pipeline.py --date 2026-03-11
python pipeline/run_pipeline.py --dry-run
python pipeline/run_pipeline.py --skip-ads          # skip ad extraction
python pipeline/run_pipeline.py --no-transcribe     # skip transcription
```

### Or run the cron wrapper (logs to `logs/`)

```bash
bash pipeline/run_pipeline_daily.sh
```

### Run individual pipeline steps

```bash
# Queue downloads for today's scheduled episodes
python pipeline/queue_downloads.py

# Download all queued episodes
python pipeline/download_episodes.py

# Compare versions and extract ads
python pipeline/compare_and_extract_ads.py
python pipeline/compare_and_extract_ads.py --plots  # generate alignment visualizations
```

### Ingest new podcasts

```bash
# Resolve Castbox URLs to RSS feeds
python ingestion/take_rss.py

# Scrape RSS feeds into the database
python ingestion/scraper.py

# Schedule the latest episode from each feed
python ingestion/schedule_latest_episodes.py --days Mon,Wed,Fri

# Schedule episodes from a CSV file
python ingestion/schedule_episodes.py data/schedule.csv
```

### Analysis

```bash
# Plot ad timing statistics
python analysis/plot_ad_stats.py

# Split ad segments into individual ads
python analysis/split_ad_segments.py --in /path/to/ads --out /path/to/output

# Transcribe ad audio files
python analysis/transcribe.py /path/to/audio_dir
```
