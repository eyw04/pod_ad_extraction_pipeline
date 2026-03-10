#!/usr/bin/env python3
"""
Split ad-segment MP3s (or any MP3 file) into likely individual ad items.

Accepts:
  - A directory of MP3 files
  - OR a single MP3 file

Pipeline:
  Stage A: detect candidate boundaries using:
    - MP3 header regime changes  (encoding parameter shifts)
    - Silence-gap detection       (sustained low-energy regions between ads)
    - Structural fallback          (SSM novelty + BIC when no silence exists)
  Stage B: prune + validate + merge small fragments

Requires:
  - ffmpeg installed
  - numpy
  - Your existing core.mp3_parser (scan_mp3_frames, parse_header)

Usage:

Single file:
  python split_ad_segments.py --in some_file.mp3 --out output_dir

Directory:
  python split_ad_segments.py --in ../ads/A_only --out ../ads_split/A_only
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent / "Podcast-Ad-Detection"))

from core.mp3_parser import scan_mp3_frames, parse_header


# ----------------------------
# Audio decoding
# ----------------------------

def decode_mp3(path, sr=16000):
    cmd = [
        "ffmpeg", "-v", "error",
        "-i", str(path),
        "-f", "f32le",
        "-ac", "1",
        "-ar", str(sr),
        "pipe:1"
    ]
    raw = subprocess.check_output(cmd)
    return np.frombuffer(raw, dtype=np.float32)


def detect_silence_gaps(y, sr, silence_thresh_db=-45, min_silence_s=0.3,
                        hop_s=0.02, verbose=False):
    """Return scored silence-gap candidates.

    Each candidate is a dict with keys: time, duration, floor_db, score.
    Score = gap_duration * abs(floor_db), which naturally ranks deep long
    silences (true ad boundaries) above brief shallow pauses (speech gaps).
    """
    hop = int(sr * hop_s)
    rms_vals = np.array([
        np.sqrt(np.mean(y[i:i+hop] ** 2) + 1e-12)
        for i in range(0, len(y) - hop, hop)
    ])
    rms_db = 20.0 * np.log10(rms_vals)

    is_silent = rms_db < silence_thresh_db
    min_silent_frames = max(1, int(min_silence_s / hop_s))

    candidates = []
    i = 0
    while i < len(is_silent):
        if is_silent[i]:
            j = i + 1
            while j < len(is_silent) and is_silent[j]:
                j += 1
            gap_len = (j - i) * hop_s
            floor = float(np.min(rms_db[i:j]))
            mid_time = ((i + j) / 2.0) * hop_s

            if (j - i) >= min_silent_frames:
                score = gap_len * abs(floor)
                candidates.append({
                    "time": mid_time,
                    "duration": gap_len,
                    "floor_db": floor,
                    "score": score,
                })
                if verbose:
                    print(f"    silence @ {mid_time:.2f}s  "
                          f"(dur {gap_len:.3f}s, "
                          f"floor {floor:.1f} dB, "
                          f"score {score:.1f})")
            elif verbose:
                print(f"    (skipped short silence @ {mid_time:.2f}s  "
                      f"dur {gap_len:.3f}s)")
            i = j
        else:
            i += 1
    return candidates


# ----------------------------
# Adaptive estimation
# ----------------------------

def estimate_max_ads(duration, min_ad_len=20.0, hard_max=4):
    """Estimate the maximum number of ads in a clip from its duration."""
    import math
    return min(hard_max, max(1, math.ceil(duration / min_ad_len)))


def select_boundaries(candidates, max_boundaries, min_score_ratio=0.42,
                      min_spacing=15.0, verbose=False):
    """Conservatively pick the strongest scored candidates.

    Keeps up to *max_boundaries* candidates, but only those whose score
    is at least *min_score_ratio* of the strongest candidate's score.
    Skips any candidate that is within *min_spacing* seconds of an
    already-selected boundary (prevents clustered picks that would
    create segments shorter than min_spacing).
    """
    if not candidates or max_boundaries <= 0:
        return []
    ranked = sorted(candidates, key=lambda c: c["score"], reverse=True)
    best_score = ranked[0]["score"]
    threshold = best_score * min_score_ratio
    selected = []
    for c in ranked:
        if len(selected) >= max_boundaries:
            break
        if c["score"] < threshold:
            if verbose:
                print(f"    (dropped boundary @ {c['time']:.2f}s, "
                      f"score {c['score']:.1f} < threshold {threshold:.1f})")
            continue
        too_close = any(abs(c["time"] - s) < min_spacing for s in selected)
        if too_close:
            if verbose:
                print(f"    (skipped boundary @ {c['time']:.2f}s, "
                      f"too close to selected)")
            continue
        selected.append(c["time"])
        if verbose:
            print(f"    selected boundary @ {c['time']:.2f}s  "
                  f"(score {c['score']:.1f}, "
                  f"threshold {threshold:.1f})")
    return sorted(selected)


# ----------------------------
# Spectral feature extraction
# ----------------------------

def extract_spectral_features(y, sr, hop_s=0.5, win_s=0.05, n_fft=1024):
    """Log-power spectrogram averaged in hop_s-wide windows.  numpy only."""
    win_len = int(sr * win_s)
    hop = int(sr * hop_s)
    hann = np.hanning(win_len)

    frames = []
    for i in range(0, len(y) - hop, hop):
        sub_specs = []
        for j in range(0, hop - win_len, win_len):
            if i + j + win_len > len(y):
                break
            chunk = y[i + j : i + j + win_len] * hann
            spec = np.abs(np.fft.rfft(chunk, n=n_fft)) ** 2
            sub_specs.append(spec)
        if sub_specs:
            frames.append(np.log(np.mean(sub_specs, axis=0) + 1e-12))

    return np.array(frames)


# ----------------------------
# Structural boundary detection
# ----------------------------

def ssm_boundaries(y, sr, kernel_size=16, z=1.5, hop_s=0.5, verbose=False):
    """Detect structural boundaries via self-similarity matrix novelty.

    Returns scored candidates (dicts with time/score).  Only peaks above
    mean + z * std are returned.
    """
    feats = extract_spectral_features(y, sr, hop_s=hop_s)
    if len(feats) < kernel_size * 2:
        return []

    norms = np.linalg.norm(feats, axis=1, keepdims=True) + 1e-12
    feats_n = feats / norms
    S = feats_n @ feats_n.T

    k = kernel_size
    half = k // 2
    kernel = np.ones((k, k))
    kernel[:half, half:] = -1
    kernel[half:, :half] = -1

    n = len(S)
    novelty = np.zeros(n)
    for i in range(half, n - half):
        patch = S[i - half : i + half, i - half : i + half]
        novelty[i] = np.sum(patch * kernel)

    novelty = np.maximum(novelty, 0)
    mean_n = np.mean(novelty)
    std_n = np.std(novelty) + 1e-12
    thresh = mean_n + z * std_n

    candidates = []
    for i in range(1, len(novelty) - 1):
        if (novelty[i] > novelty[i - 1]
                and novelty[i] >= novelty[i + 1]
                and novelty[i] > thresh):
            t = i * hop_s
            candidates.append({"time": t, "score": float(novelty[i])})
            if verbose:
                print(f"    SSM peak @ {t:.1f}s  "
                      f"(novelty {novelty[i]:.1f}, thresh {thresh:.1f})")
    return candidates


def bic_boundaries(y, sr, window_s=10.0, hop_s=0.5, penalty=0.5, z=1.0,
                   verbose=False):
    """Detect boundaries via sliding-window BIC divergence.

    Returns scored candidates (dicts with time/score).  Only peaks above
    mean + z * std of the positive delta-BIC values are returned.
    """
    feats = extract_spectral_features(y, sr, hop_s=hop_s)
    n_frames = len(feats)
    w = int(window_s / hop_s)

    if n_frames < 2 * w + 2:
        return []

    n_coeffs = min(13, feats.shape[1])
    N = feats.shape[1]
    dct_basis = np.cos(
        np.pi * np.arange(n_coeffs)[:, None]
        * (2 * np.arange(N)[None, :] + 1) / (2 * N)
    )
    mfcc = feats @ dct_basis.T

    d = mfcc.shape[1]
    reg = 1e-6 * np.eye(d)
    delta_bic = np.full(n_frames, -np.inf)

    for t in range(w, n_frames - w):
        left = mfcc[t - w : t]
        right = mfcc[t : t + w]
        full = mfcc[t - w : t + w]

        n_total = len(full)
        n_left = len(left)
        n_right = len(right)

        ld_full = np.linalg.slogdet(np.cov(full.T) + reg)[1]
        ld_left = np.linalg.slogdet(np.cov(left.T) + reg)[1]
        ld_right = np.linalg.slogdet(np.cov(right.T) + reg)[1]

        bic_diff = (0.5 * n_total * ld_full
                    - 0.5 * n_left * ld_left
                    - 0.5 * n_right * ld_right)
        pen = penalty * 0.5 * (d + 0.5 * d * (d + 1)) * np.log(n_total)
        delta_bic[t] = bic_diff - pen

    positive = delta_bic[delta_bic > 0]
    if len(positive) == 0:
        return []
    bic_thresh = np.mean(positive) + z * np.std(positive)

    candidates = []
    for i in range(w + 1, n_frames - w - 1):
        if (delta_bic[i] > bic_thresh
                and delta_bic[i] > delta_bic[i - 1]
                and delta_bic[i] >= delta_bic[i + 1]):
            t = i * hop_s
            candidates.append({"time": t, "score": float(delta_bic[i])})
            if verbose:
                print(f"    BIC peak @ {t:.1f}s  "
                      f"(delta {delta_bic[i]:.1f}, "
                      f"thresh {bic_thresh:.1f})")
    return candidates


def merge_all_candidates(*candidate_lists, weights=None, tolerance=3.0,
                         verbose=False):
    """Merge scored candidates from multiple detection methods.

    Each list's scores are independently normalized to [0, weight], then
    candidates within *tolerance* seconds are fused: their positions are
    averaged and their weighted scores are summed.  *weights* defaults to
    equal (1.0 per list).
    """
    if weights is None:
        weights = [1.0] * len(candidate_lists)

    def normalize(cands, weight):
        if not cands:
            return []
        max_score = max(c["score"] for c in cands)
        if max_score <= 0:
            return []
        return [{"time": c["time"], "score": c["score"] / max_score * weight}
                for c in cands]

    merged = []
    for cands, w in zip(candidate_lists, weights):
        for nc in normalize(cands, w):
            best_match = None
            best_dist = tolerance + 1
            for m in merged:
                dist = abs(m["time"] - nc["time"])
                if dist <= tolerance and dist < best_dist:
                    best_match = m
                    best_dist = dist
            if best_match is not None:
                best_match["time"] = (best_match["time"] + nc["time"]) / 2.0
                best_match["score"] += nc["score"]
            else:
                merged.append(dict(nc))

    if verbose and merged:
        for c in sorted(merged, key=lambda x: -x["score"]):
            print(f"    merged candidate @ {c['time']:.1f}s  "
                  f"(confidence {c['score']:.2f})")

    return merged


# ----------------------------
# Header regime detection
# ----------------------------

def header_tuple(hdr):
    ok, info = parse_header(hdr[0], hdr[1], hdr[2], hdr[3])
    if not ok:
        return None
    return (
        info.get("bitrate"),
        info.get("sample_rate"),
        info.get("chan_mode"),
        info.get("frame_len"),
    )


def header_boundaries(hdrs, frame_ms, min_persist_s=1.0):
    min_frames = max(1, int((min_persist_s * 1000) / frame_ms))
    regimes = [header_tuple(h) for h in hdrs]

    runs = []
    i = 0
    while i < len(regimes):
        j = i + 1
        while j < len(regimes) and regimes[j] == regimes[i]:
            j += 1
        if (j - i) >= min_frames:
            runs.append((i, j))
        i = j

    bounds = []
    for k in range(1, len(runs)):
        bounds.append(runs[k][0])
    return bounds


# ----------------------------
# Utility
# ----------------------------

def prune(boundaries, min_gap):
    boundaries = sorted(boundaries)
    out = []
    for t in boundaries:
        if not out or t - out[-1] >= min_gap:
            out.append(t)
    return out


def split_ranges(duration, boundaries):
    cuts = [0.0] + boundaries + [duration]
    return [(cuts[i], cuts[i+1]) for i in range(len(cuts)-1)]


def merge_short(segs, min_len):
    """Merge segments shorter than *min_len* into a neighbor.

    Repeats until every segment meets the minimum or only one remains.
    """
    changed = True
    while changed and len(segs) > 1:
        changed = False
        out = []
        for i, (s, e) in enumerate(segs):
            if not out:
                out.append((s, e))
                continue
            if (e - s) < min_len:
                ps, pe = out[-1]
                out[-1] = (ps, e)
                changed = True
            else:
                out.append((s, e))
        if len(out) > 1 and (out[0][1] - out[0][0]) < min_len:
            out = [(out[0][0], out[1][1])] + out[2:]
            changed = True
        segs = out
    return segs


def cap_segments(segs, max_segs):
    """Iteratively merge the shortest segment into its neighbor."""
    while len(segs) > max_segs:
        min_idx = min(range(len(segs)),
                      key=lambda i: segs[i][1] - segs[i][0])
        s, e = segs[min_idx]
        if min_idx == 0:
            segs = [(s, segs[1][1])] + segs[2:]
        elif min_idx == len(segs) - 1:
            segs = segs[:-2] + [(segs[-2][0], e)]
        else:
            prev_len = segs[min_idx - 1][1] - segs[min_idx - 1][0]
            next_len = segs[min_idx + 1][1] - segs[min_idx + 1][0]
            if prev_len <= next_len:
                segs = (segs[:min_idx - 1]
                        + [(segs[min_idx - 1][0], e)]
                        + segs[min_idx + 1:])
            else:
                segs = (segs[:min_idx]
                        + [(s, segs[min_idx + 1][1])]
                        + segs[min_idx + 2:])
    return segs


def export_segments(in_path, segs, out_dir, base):
    os.makedirs(out_dir, exist_ok=True)
    out_files = []
    for idx, (s, e) in enumerate(segs, 1):
        out_path = os.path.join(out_dir, f"{base}_part{idx:02d}.mp3")
        cmd = [
            "ffmpeg", "-v", "error",
            "-ss", f"{s:.3f}",
            "-to", f"{e:.3f}",
            "-i", str(in_path),
            "-c", "copy",
            out_path
        ]
        subprocess.call(cmd)
        out_files.append(out_path)
    return out_files


# ----------------------------
# Main
# ----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", required=True, help="Input MP3 file OR directory")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--sr", type=int, default=16000)
    parser.add_argument("--min_gap", type=float, default=3.0,
                        help="Min seconds between two split points (default 3)")
    parser.add_argument("--min_seg", type=float, default=15.0,
                        help="Segments shorter than this are merged (default 15)")
    parser.add_argument("--max_segs", type=int, default=None,
                        help="Override max segments per file (default: auto from duration)")
    parser.add_argument("--min_ad_len", type=float, default=20.0,
                        help="Minimum expected ad length in seconds; used to "
                             "estimate max ad count as ceil(duration/min_ad_len) "
                             "(default 20)")
    parser.add_argument("--silence_thresh", type=float, default=-45,
                        help="Silence threshold in dB (default -45)")
    parser.add_argument("--min_silence", type=float, default=0.3,
                        help="Min duration (seconds) of silence to count as gap (default 0.3)")
    parser.add_argument("--no-header", action="store_true")
    parser.add_argument("--no-audio", action="store_true")
    parser.add_argument("--no-structural", action="store_true",
                        help="Disable SSM/BIC structural detection")
    parser.add_argument("--structural-method",
                        choices=["auto", "ssm", "bic"], default="auto",
                        help="Structural method: auto (both), ssm only, or bic only")
    parser.add_argument("--kernel-size", type=int, default=16,
                        help="SSM checkerboard kernel size (default 16)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print detected boundaries")
    args = parser.parse_args()

    in_path = Path(args.__dict__["in"])
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if in_path.is_file():
        mp3s = [in_path]
    else:
        mp3s = sorted(in_path.glob("*.mp3"))

    for mp3 in mp3s:
        print(f"Processing {mp3.name}")
        boundaries = []

        # HEADER DETECTION
        if not args.no_header:
            offs, hdrs, h, ms_list = scan_mp3_frames(str(mp3))
            frame_ms = next((x for x in ms_list if x is not None), 26.0)
            b_frames = header_boundaries(hdrs, frame_ms)
            boundaries += [bf * frame_ms / 1000.0 for bf in b_frames]

        # AUDIO-BASED DETECTION
        if not args.no_audio:
            y = decode_mp3(mp3, sr=args.sr)
            duration = len(y) / args.sr

            max_ads = estimate_max_ads(duration, args.min_ad_len)
            if args.max_segs is not None:
                max_ads = min(max_ads, args.max_segs)
            if args.verbose:
                print(f"    duration {duration:.1f}s → "
                      f"estimated max {max_ads} ad(s)")

            silence_candidates = detect_silence_gaps(
                y, args.sr,
                silence_thresh_db=args.silence_thresh,
                min_silence_s=args.min_silence,
                verbose=args.verbose,
            )

            ssm_cands, bic_cands = [], []
            if not args.no_structural:
                method = args.structural_method
                if method in ("auto", "ssm"):
                    ssm_cands = ssm_boundaries(
                        y, args.sr,
                        kernel_size=args.kernel_size,
                        verbose=args.verbose,
                    )
                if method in ("auto", "bic"):
                    bic_cands = bic_boundaries(
                        y, args.sr, verbose=args.verbose,
                    )

            all_candidates = merge_all_candidates(
                silence_candidates, ssm_cands, bic_cands,
                weights=[2.0, 0.5, 0.5],
                verbose=args.verbose,
            )
            selected = select_boundaries(
                all_candidates, max_ads - 1,
                min_spacing=args.min_seg,
                verbose=args.verbose,
            )
            boundaries += selected
        else:
            duration = subprocess.check_output([
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=nokey=1:noprint_wrappers=1",
                str(mp3)
            ]).decode().strip()
            duration = float(duration)
            max_ads = estimate_max_ads(duration, args.min_ad_len)
            if args.max_segs is not None:
                max_ads = min(max_ads, args.max_segs)

        boundaries = prune(boundaries, args.min_gap)
        segs = split_ranges(duration, boundaries)
        segs = merge_short(segs, args.min_seg)
        segs = cap_segments(segs, max_ads)

        if args.verbose:
            for i, (s, e) in enumerate(segs, 1):
                print(f"    seg {i}: {s:.2f}s – {e:.2f}s  ({e-s:.1f}s)")

        export_segments(mp3, segs, out_dir, mp3.stem)
        print(f"  → {len(segs)} segment(s)")

    print("Done.")


if __name__ == "__main__":
    main()