"""
Compare two downloaded versions of the same podcast episode and extract
ad segments using frame-level MP3 alignment.

Workflow:
  1. Scans temp/ for files that also exist in the main downloads directory.
  2. For each pair, parses MP3 frames and computes per-frame hashes.
  3. Uses SequenceMatcher to align frames; non-matching gaps are ad candidates.
  4. Filters by duration (default 15–120 s) and exports ad audio files.
  5. Transcribes each ad segment using faster-whisper.
  6. Records each ad segment in the `ads` table.
  7. Promotes the temp copy to replace the original and updates the DB.

Usage:
    python compare_and_extract_ads.py
    python compare_and_extract_ads.py --min-dur 10 --max-dur 90
    python compare_and_extract_ads.py --limit 10 --dry-run
    python compare_and_extract_ads.py --ads-dir /path/to/ads
    python compare_and_extract_ads.py --plots --plots-dir /path/to/plots

Requires the Podcast-Ad-Detection core modules (mp3_parser, aligner).
"""

import argparse
import os
import sqlite3
import sys
from pathlib import Path

from faster_whisper import WhisperModel

# Add Podcast-Ad-Detection to the import path
sys.path.insert(0, str(Path(__file__).parent / "Podcast-Ad-Detection"))

from core.mp3_parser import scan_mp3_frames, parse_header
from core.aligner import quick_align_stats, decide_mode
from utils.visualization import plot_alignment_blocks, first_frame_ms

DEFAULT_DB = Path("/shared/6/projects/podcast-ads/pipeline.db")
DEFAULT_ADS_DIR = Path("/shared/6/projects/podcast-ads/extracted-ads")
DEFAULT_PLOTS_DIR = Path("/shared/6/projects/podcast-ads/ad-plots")
MATCH_THRESHOLD = 0.60


# ── transcription ────────────────────────────────────────────────────────────

def load_whisper_model(model_name: str = "small"):
    """Load faster-whisper model for CPU transcription."""
    print(f"Loading faster-whisper model ({model_name}) on cpu (int8)")
    return WhisperModel(model_name, device="cpu", compute_type="int8")


def transcribe_audio(model: WhisperModel, audio_path: str) -> str:
    """Transcribe an audio file and return the full text."""
    segments, _ = model.transcribe(
        audio_path,
        beam_size=1,
        vad_filter=True,
    )
    parts = []
    for seg in segments:
        t = (seg.text or "").strip()
        if t:
            parts.append(t)
    return " ".join(parts)


# ── helpers ──────────────────────────────────────────────────────────────────

def export_mp3_segment(src_path: str, offsets: list, headers: list,
                       seg_start: int, seg_end: int, out_path: str):
    """Extract a range of MP3 frames [seg_start, seg_end) to a file."""
    ends = []
    for off, hdr in zip(offsets, headers):
        ok, info = parse_header(hdr[0], hdr[1], hdr[2], hdr[3])
        ends.append(off + (info["frame_len"] if ok else 0))

    byte_start = offsets[seg_start]
    byte_end = ends[seg_end - 1]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(src_path, "rb") as f:
        f.seek(byte_start)
        data = f.read(byte_end - byte_start)
    with open(out_path, "wb") as f:
        f.write(data)


def find_download_pairs(main_dir: Path, temp_dir: Path, limit: int | None = None):
    """Find episode pairs by scanning temp/ for files that also exist in the main dir.

    Returns list of (episode_id, path_a, path_b) where:
        path_a = main dir file (original download)
        path_b = temp dir file (redownload)
        episode_id = filename stem (the episode_id)
    """
    if not temp_dir.is_dir():
        return []

    pairs = []
    for temp_file in sorted(temp_dir.iterdir()):
        if not temp_file.is_file() or temp_file.suffix.lower() not in (".mp3", ".m4a", ".wav", ".ogg", ".opus", ".flac"):
            continue
        main_file = main_dir / temp_file.name
        if not main_file.is_file():
            continue
        episode_id = temp_file.stem
        pairs.append((episode_id, str(main_file), str(temp_file)))
        if limit and len(pairs) >= limit:
            break

    return pairs


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare episode download pairs and extract ad segments",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument(
        "--ads-dir",
        default=str(DEFAULT_ADS_DIR),
        help="Output directory for extracted ad audio files",
    )
    parser.add_argument("--threshold", type=float, default=MATCH_THRESHOLD,
                        help="Min matching ratio for frame-level alignment (default: 0.60)")
    parser.add_argument("--min-dur", type=float, default=15.0,
                        help="Minimum ad duration in seconds (default: 15)")
    parser.add_argument("--max-dur", type=float, default=120.0,
                        help="Maximum ad duration in seconds (default: 120)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of pairs to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without extracting")
    parser.add_argument("--plots", action="store_true",
                        help="Generate alignment plots (off by default)")
    parser.add_argument("--plots-dir", default=str(DEFAULT_PLOTS_DIR),
                        help="Output directory for alignment plots")
    parser.add_argument("--no-transcribe", action="store_true",
                        help="Skip transcribing ad segments")
    parser.add_argument("--whisper-model", default="small",
                        help="Whisper model size (default: small)")
    args = parser.parse_args()

    # ── validate ─────────────────────────────────────────────────────────
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    ads_dir = Path(args.ads_dir).expanduser().resolve()
    plots_dir = Path(args.plots_dir).expanduser().resolve()

    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    cur = conn.cursor()

    # ── find pairs by scanning main dir + temp dir ───────────────────────
    main_dir = Path("/shared/6/projects/podcast-ads/episode-downloads")
    temp_dir = main_dir / "temp"

    pairs = find_download_pairs(main_dir, temp_dir, args.limit)
    if not pairs:
        print("No episode pairs found (temp/ is empty or has no matching files).")
        conn.close()
        return

    # ── load whisper model (once, reused for all ads) ────────────────────
    whisper_model = None
    if not args.no_transcribe and not args.dry_run:
        whisper_model = load_whisper_model(args.whisper_model)

    print(f"Found {len(pairs)} episode pair(s) to compare")

    stats = {"processed": 0, "ads_found": 0, "skipped": 0, "low_match": 0}

    for idx, (eid, path_a, path_b) in enumerate(pairs, 1):
        prefix = f"[{idx}/{len(pairs)}]"

        # Look up episode title for display
        cur.execute("SELECT title FROM episodes WHERE episode_id = ?", (eid,))
        row = cur.fetchone()
        short_title = ((row[0] if row else None) or eid)[:60]

        if args.dry_run:
            print(f"{prefix} {short_title}")
            print(f"    A: {path_a}")
            print(f"    B: {path_b}")
            stats["processed"] += 1
            continue

        print(f"\n{prefix} {short_title}")

        # ── parse MP3 frames ─────────────────────────────────────────
        offs_a, hdrs_a, hashes_a, ms_a = scan_mp3_frames(path_a)
        offs_b, hdrs_b, hashes_b, ms_b = scan_mp3_frames(path_b)

        if not hashes_a or not hashes_b:
            print(f"    Could not parse MP3 frames, skipping ad extraction")
            stats["skipped"] += 1
            ratio = 0.0
            blocks = []
            gaps = {"A_only": [], "B_only": []}
            frame_ms = None
            mode = "Unparseable"
            longest_sec = 0.0
        else:
            print(f"    Frames: A={len(hashes_a)}, B={len(hashes_b)}")

            # ── align ────────────────────────────────────────────────
            ratio, longest, blocks, gaps = quick_align_stats(hashes_a, hashes_b)
            frame_ms = first_frame_ms(ms_a) or first_frame_ms(ms_b)
            mode, longest_sec = decide_mode(ratio, longest, frame_ms)

            print(f"    Match ratio: {ratio:.2%}, longest run: {longest} frames ({longest_sec:.1f}s)")
            print(f"    Mode: {mode}")

        # ── look up download_ids for DB recording ─────────────────────
        cur.execute(
            "SELECT download_id FROM downloads WHERE file_path = ? AND error_message IS NULL LIMIT 1",
            (path_a,),
        )
        row_a = cur.fetchone()
        did_a = row_a[0] if row_a else None

        cur.execute(
            "SELECT download_id FROM downloads WHERE file_path = ? AND error_message IS NULL LIMIT 1",
            (path_b,),
        )
        row_b = cur.fetchone()
        did_b = row_b[0] if row_b else None

        # ── extract ad segments from both sides ──────────────────────
        episode_ads = 0

        if ratio < args.threshold or not mode.startswith("Frame-level"):
            print(f"    Below threshold or re-encoded – skipping ad extraction")
            stats["low_match"] += 1
        else:
            for side, gap_list, src_path, did, offsets, headers in [
                ("A", gaps["A_only"], path_a, did_a, offs_a, hdrs_a),
                ("B", gaps["B_only"], path_b, did_b, offs_b, hdrs_b),
            ]:
                for seg_idx, (i0, i1) in enumerate(gap_list, 1):
                    dur_sec = (i1 - i0) * frame_ms / 1000.0
                    start_sec = i0 * frame_ms / 1000.0
                    end_sec = i1 * frame_ms / 1000.0

                    if not (args.min_dur <= dur_sec <= args.max_dur):
                        continue

                    # Export ad audio file
                    ad_subdir = ads_dir / eid
                    ad_filename = f"{side}_{seg_idx:02d}_{start_sec:.1f}s-{end_sec:.1f}s.mp3"
                    ad_path = str(ad_subdir / ad_filename)

                    export_mp3_segment(src_path, offsets, headers, i0, i1, ad_path)

                    # Transcribe the ad segment
                    transcript = None
                    if whisper_model is not None:
                        try:
                            transcript = transcribe_audio(whisper_model, ad_path)
                        except Exception as e:
                            print(f"    Transcription failed for {ad_filename}: {e}")

                    # Record in database
                    cur.execute(
                        """INSERT OR IGNORE INTO ads
                               (download_id, episode_id, timing_start, timing_end, transcript_text, ad_file_path)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (did, eid, round(start_sec, 3), round(end_sec, 3), transcript, ad_path),
                    )

                    print(f"    Ad [{side}]: {start_sec:.1f}s – {end_sec:.1f}s ({dur_sec:.1f}s) → {ad_filename}")
                    if transcript:
                        print(f"        Transcript: {transcript[:100]}{'...' if len(transcript) > 100 else ''}")
                    episode_ads += 1

        # ── optional alignment plot ──────────────────────────────────
        if args.plots and blocks:
            plot_path = str(plots_dir / f"{eid}_diff.png")
            os.makedirs(str(plots_dir), exist_ok=True)
            plot_alignment_blocks(
                blocks=blocks[:10],
                frame_ms_a=frame_ms,
                frame_ms_b=frame_ms,
                outfile=plot_path,
                total_frames_a=len(hashes_a),
                total_frames_b=len(hashes_b),
            )
            print(f"    Plot saved: {plot_path}")

        # ── promote temp copy: replace original with redownload ─────
        #   1. Move temp file (path_b) to main dir (path_a), overwriting old
        #   2. Null out file_path on the old download record
        #   3. Update the new download record to point to the main dir path
        temp_file = Path(path_b)
        main_file = Path(path_a)

        temp_file.rename(main_file)
        print(f"    Promoted: temp → {main_file}")

        if did_a is not None:
            cur.execute(
                "UPDATE downloads SET file_path = NULL WHERE download_id = ?",
                (did_a,),
            )
        if did_b is not None:
            cur.execute(
                "UPDATE downloads SET file_path = ? WHERE download_id = ?",
                (str(main_file), did_b),
            )

        conn.commit()
        stats["processed"] += 1
        stats["ads_found"] += episode_ads

        if episode_ads == 0:
            print(f"    No ads found in duration range [{args.min_dur}s, {args.max_dur}s]")

    # ── summary ──────────────────────────────────────────────────────────
    conn.close()

    print(f"\n{'=' * 50}")
    if args.dry_run:
        print("Dry-run complete.")
    else:
        print("Ad extraction complete!")
    print(f"  Pairs processed : {stats['processed']}")
    print(f"  Ads found       : {stats['ads_found']}")
    print(f"  Low match/skip  : {stats['low_match']}")
    print(f"  File missing    : {stats['skipped']}")
    print(f"  Total pairs     : {len(pairs)}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
