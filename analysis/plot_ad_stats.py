"""
Plot advertisement timing statistics from the pipeline database.

Generates two plots:
  1. Distribution of ad start times (in minutes from episode start)
  2. Distribution of ad durations (in seconds)

Usage:
    python plot_ad_stats.py
    python plot_ad_stats.py --db /path/to/pipeline.db
    python plot_ad_stats.py --output ad_stats.png
"""

import argparse
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")


def main():
    parser = argparse.ArgumentParser(description="Plot ad timing statistics")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument("--output", default="ad_stats.png", help="Output image path")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=60)
    rows = conn.execute("SELECT timing_start, timing_end FROM ads").fetchall()
    conn.close()

    if not rows:
        print("No ads found in database.")
        return

    starts_min = np.array([r[0] / 60.0 for r in rows])
    durations_sec = np.array([r[1] - r[0] for r in rows])

    print(f"Total ads: {len(rows)}")
    print(f"Start time — mean: {starts_min.mean():.1f} min, median: {np.median(starts_min):.1f} min")
    print(f"Duration   — mean: {durations_sec.mean():.1f}s, median: {np.median(durations_sec):.1f}s")

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Plot 1: Ad start times
    ax1.hist(starts_min, bins=40, color="#4a90d9", edgecolor="white", alpha=0.85)
    ax1.axvline(np.median(starts_min), color="#d94a4a", linestyle="--", linewidth=1.5,
                label=f"Median: {np.median(starts_min):.1f} min")
    ax1.set_xlabel("Ad Start Time (minutes from episode start)")
    ax1.set_ylabel("Count")
    ax1.set_title("When Do Ads Appear?")
    ax1.legend()

    # Plot 2: Ad durations
    ax2.hist(durations_sec, bins=40, color="#5bb370", edgecolor="white", alpha=0.85)
    ax2.axvline(np.median(durations_sec), color="#d94a4a", linestyle="--", linewidth=1.5,
                label=f"Median: {np.median(durations_sec):.1f}s")
    ax2.set_xlabel("Ad Duration (seconds)")
    ax2.set_ylabel("Count")
    ax2.set_title("How Long Are Ads?")
    ax2.legend()

    fig.suptitle(f"Podcast Ad Statistics (n={len(rows)})", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(args.output, dpi=150)
    print(f"Saved to {args.output}")
    plt.show()


if __name__ == "__main__":
    main()
