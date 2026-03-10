PRAGMA foreign_keys = ON;

-- =========================
-- Podcasts (show-level metadata)
-- =========================
CREATE TABLE IF NOT EXISTS podcasts (
    podcast_id    TEXT PRIMARY KEY,  -- e.g., RSS feed URL hash, feed GUID, etc.

    name          TEXT,
    author        TEXT,
    rss_url       TEXT,
    publisher     TEXT,

    language      TEXT,
    categories    TEXT CHECK (categories IS NULL OR json_valid(categories)),  -- JSON array
    metadata      TEXT CHECK (metadata IS NULL OR json_valid(metadata)),      -- JSON blob

    episode_count INTEGER,
    last_updated  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_podcasts_rss_url
ON podcasts(rss_url);

-- =========================
-- Episodes (episode-level metadata)
-- =========================
CREATE TABLE IF NOT EXISTS episodes (
    episode_id      TEXT PRIMARY KEY,
    podcast_id      TEXT,            -- nullable if you ingest episodes before resolving podcast
    title           TEXT,
    url             TEXT,
    metadata        TEXT,
    last_updated    DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (podcast_id) REFERENCES podcasts(podcast_id)
);

CREATE INDEX IF NOT EXISTS idx_episodes_podcast
ON episodes(podcast_id);

-- =========================
-- Recurring schedule rules (per episode)
-- =========================
CREATE TABLE IF NOT EXISTS episode_schedules (
    schedule_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id      TEXT NOT NULL,
    days_of_week    TEXT NOT NULL,

    is_enabled      INTEGER DEFAULT 1,

    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id)
);

CREATE INDEX IF NOT EXISTS idx_episode_schedules_episode
ON episode_schedules(episode_id);

CREATE INDEX IF NOT EXISTS idx_episode_schedules_enabled
ON episode_schedules(is_enabled);

-- =========================
-- Download task queue (one row per scheduled run)
-- =========================
CREATE TABLE IF NOT EXISTS download_tasks (
    task_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id     INTEGER,
    episode_id      TEXT NOT NULL,

    scheduled_date DATE NOT NULL,

    -- 0:READY, 1:DOWNLOADING, 2:WAITING(backoff/deferred), 3:FINISHED, 4:ERROR
    status          INTEGER DEFAULT 0 CHECK (status IN (0,1,2,3,4)),
    error_message   TEXT,

    FOREIGN KEY (schedule_id) REFERENCES episode_schedules(schedule_id),
    FOREIGN KEY (episode_id)  REFERENCES episodes(episode_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_task_episode_date
ON download_tasks(episode_id, scheduled_date);

CREATE INDEX IF NOT EXISTS idx_tasks_status_date
ON download_tasks(status, scheduled_date);

CREATE INDEX IF NOT EXISTS idx_tasks_episode
ON download_tasks(episode_id);

-- =========================
-- Download attempts log (multiple per task allowed)
-- =========================
CREATE TABLE IF NOT EXISTS downloads (
    download_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id          INTEGER,
    episode_id       TEXT NOT NULL,

    download_date    DATETIME DEFAULT CURRENT_TIMESTAMP,

    md5_checksum     TEXT,
    file_path        TEXT,

    download_version INTEGER DEFAULT 0,
    attempt_number   INTEGER DEFAULT 1 CHECK (attempt_number >= 1),

    vpn_status       TEXT,
    status_code      INTEGER,
    error_message    TEXT,

    FOREIGN KEY (task_id)    REFERENCES download_tasks(task_id),
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id)
);

CREATE INDEX IF NOT EXISTS idx_downloads_task
ON downloads(task_id);

CREATE INDEX IF NOT EXISTS idx_downloads_episode_date
ON downloads(episode_id, download_date);

-- =========================
-- Extracted ad segments for a specific download
-- =========================
CREATE TABLE IF NOT EXISTS ads (
    ad_id           INTEGER PRIMARY KEY AUTOINCREMENT,

    download_id     INTEGER NOT NULL,
    episode_id      TEXT NOT NULL,

    timing_start    REAL CHECK (timing_start >= 0), -- in seconds
    timing_end      REAL CHECK (timing_end > timing_start), -- in seconds

    transcript_text TEXT,
    ad_file_path    TEXT,

    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (download_id) REFERENCES downloads(download_id),
    FOREIGN KEY (episode_id)  REFERENCES episodes(episode_id)
);

CREATE INDEX IF NOT EXISTS idx_ads_download
ON ads(download_id);

CREATE INDEX IF NOT EXISTS idx_ads_episode
ON ads(episode_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ads_segment
ON ads(download_id, timing_start, timing_end);