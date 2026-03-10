"""
Daily pipeline manager. Runs the three pipeline steps sequentially:

  1. queue_downloads.py    — queue today's scheduled episodes into download_tasks
  2. download_episodes.py  — download all READY tasks
  3. compare_and_extract_ads.py — compare versions, extract ads, promote files

If any step fails, the pipeline stops and reports the error.

Usage:
    python run_pipeline.py                  # run full pipeline for today
    python run_pipeline.py --date 2026-02-16
    python run_pipeline.py --dry-run        # dry-run all steps
    python run_pipeline.py --skip-ads       # skip ad extraction step
    python run_pipeline.py --no-transcribe  # skip whisper transcription
"""

import argparse
import subprocess
import sys
import time
from datetime import date

SCRIPTS_DIR = __file__.rsplit("/", 1)[0] or "."


def run_step(name: str, cmd: list[str]) -> bool:
    """Run a pipeline step. Returns True on success, False on failure."""
    print(f"\n{'=' * 60}")
    print(f"  STEP: {name}")
    print(f"  CMD:  {' '.join(cmd)}")
    print(f"{'=' * 60}\n")

    start = time.time()
    result = subprocess.run(cmd)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n[FAILED] {name} exited with code {result.returncode} ({elapsed:.1f}s)")
        return False

    print(f"\n[OK] {name} completed ({elapsed:.1f}s)")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the daily podcast ad pipeline")
    parser.add_argument("--date", default=str(date.today()),
                        help="Date to process (default: today)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Pass --dry-run to all steps")
    parser.add_argument("--skip-ads", action="store_true",
                        help="Skip the compare_and_extract_ads step")
    parser.add_argument("--no-transcribe", action="store_true",
                        help="Skip whisper transcription in ad extraction")
    args = parser.parse_args()

    python = sys.executable
    pipeline_date = args.date

    print(f"Pipeline started for {pipeline_date}")
    pipeline_start = time.time()

    # ── Step 1: Queue downloads ──────────────────────────────────────────
    cmd = [python, f"{SCRIPTS_DIR}/queue_downloads.py", "--date", pipeline_date]
    if args.dry_run:
        cmd.append("--dry-run")

    if not run_step("Queue Downloads", cmd):
        sys.exit(1)

    # ── Step 2: Download episodes ────────────────────────────────────────
    cmd = [python, f"{SCRIPTS_DIR}/download_episodes.py", "--date", pipeline_date]
    if args.dry_run:
        cmd.append("--dry-run")

    if not run_step("Download Episodes", cmd):
        sys.exit(1)

    # ── Step 3: Compare & extract ads ────────────────────────────────────
    if args.skip_ads:
        print("\n[SKIP] Ad extraction skipped (--skip-ads)")
    else:
        cmd = [python, f"{SCRIPTS_DIR}/compare_and_extract_ads.py"]
        if args.dry_run:
            cmd.append("--dry-run")
        if args.no_transcribe:
            cmd.append("--no-transcribe")

        if not run_step("Compare & Extract Ads", cmd):
            sys.exit(1)

    # ── Done ─────────────────────────────────────────────────────────────
    total = time.time() - pipeline_start
    print(f"\n{'=' * 60}")
    print(f"  Pipeline complete for {pipeline_date} ({total:.1f}s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
