import sys
from pathlib import Path

from faster_whisper import WhisperModel

AUDIO_EXTENSIONS = {".mp3", ".m4a", ".wav", ".flac", ".ogg", ".opus"}


def main(ads_audio_dir: str):
    ads_dir = Path(ads_audio_dir).expanduser().resolve()
    if not ads_dir.is_dir():
        raise FileNotFoundError(f"Not a directory: {ads_dir}")

    # ---- CPU transcription, text only ----
    model_name = "small"          # change to "base", "medium", etc. if you want
    device = "cpu"
    compute_type = "int8"         # best for CPU (fast + low memory)

    print(f"Loading faster-whisper model ({model_name}) on {device} ({compute_type})")
    model = WhisperModel(model_name, device=device, compute_type=compute_type)

    files = sorted(
        f for f in ads_dir.iterdir()
        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS
    )
    if not files:
        print(f"No audio files found in {ads_dir}")
        return

    print(f"Found {len(files)} audio file(s)")
    for audio_path in files:
        print(f"\n--- {audio_path.name} ---")
        print("Transcribing…")

        # beam_size=1 is usually fastest and fine for many use-cases
        segments, info = model.transcribe(
            str(audio_path),
            beam_size=1,
            vad_filter=True,  # helps skip silence
        )

        text_parts = []
        for seg in segments:
            t = (seg.text or "").strip()
            if t:
                text_parts.append(t)

        text = " ".join(text_parts)
        out_path = audio_path.with_suffix(".txt")
        out_path.write_text(text + "\n", encoding="utf-8")
        print(f"Done → {out_path}")

    print(f"\nFinished. Transcribed {len(files)} file(s).")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python transcribe.py <ads_audio_folder>")
        sys.exit(1)
    main(sys.argv[1])