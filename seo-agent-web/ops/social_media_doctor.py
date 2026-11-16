#!/usr/bin/env python3
"""Check this host's video encoder with fictional captures and synthetic silence."""

import argparse
import base64
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import wave

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check(lang="fr", seconds=30):
    # These modules expose fixed public scenes; never load app, DB or credentials.
    from backend import own_social_media as media, own_social_media_render as render

    if lang not in {"fr", "en"} or seconds not in {8, 30, 60}:
        raise ValueError("invalid_check")
    binary = render.executable()
    probe = shutil.which("ffprobe")
    if not probe:
        raise ValueError("probe_required")
    version = (
        subprocess.run(
            [binary, "-version"], capture_output=True, timeout=10, check=True
        )
        .stdout.decode("utf-8", errors="replace")
        .splitlines()[0][:180]
    )
    voice = io.BytesIO()
    with wave.open(voice, "wb") as output:
        output.setparams((1, 2, 24000, 0, "NONE", "not compressed"))
        output.writeframes(b"\0\0" * 24000 * seconds)
    packet = {
        "lang": lang,
        "scenes": [
            {"heading": h, "capture": c, "text": t} for h, c, t in media.SCENES[lang]
        ],
    }
    started = time.monotonic()
    result = render.render(packet, voice.getvalue())
    with tempfile.TemporaryDirectory(prefix="noyaru-encoder-check-") as directory:
        video = Path(directory) / "check.mp4"
        video.write_bytes(base64.b64decode(result["video"], validate=True))
        observed = json.loads(
            subprocess.run(
                [
                    probe,
                    "-v",
                    "error",
                    "-show_streams",
                    "-show_format",
                    "-of",
                    "json",
                    str(video),
                ],
                capture_output=True,
                timeout=10,
                check=True,
            ).stdout
        )
        picture = next(s for s in observed["streams"] if s["codec_type"] == "video")
        sound = next(s for s in observed["streams"] if s["codec_type"] == "audio")
        duration = float(observed["format"]["duration"])
        if (
            picture["codec_name"] != "h264"
            or sound["codec_name"] != "aac"
            or (picture["width"], picture["height"]) != (720, 1280)
            or picture["pix_fmt"] != "yuv420p"
            or picture["r_frame_rate"] != "24/1"
            or abs(duration - seconds) > 0.1
        ):
            raise ValueError("render_invalid")
        subprocess.run(
            [
                binary,
                "-v",
                "error",
                "-xerror",
                "-threads",
                "1",
                "-i",
                str(video),
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-threads",
                "1",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            timeout=180,
            check=True,
        )
    return {
        "ok": True,
        "encoder": version,
        "language": lang,
        "duration": duration,
        "bytes": result["bytes"],
        "seconds_elapsed": round(time.monotonic() - started, 2),
        "video": "h264 / yuv420p / 720x1280 / 24fps",
        "audio": "aac / synthetic silence",
        "full_decode": True,
        "provider_calls": 0,
        "database_access": False,
        "children_peak_rss_kib": children_peak_rss(),
    }


def children_peak_rss():
    """Linux child peak only, not the host's or application's total memory."""
    if sys.platform != "linux":
        return None
    import resource

    return int(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--language", choices=("fr", "en"), default="fr")
    parser.add_argument("--seconds", type=int, choices=(8, 30, 60), default=30)
    args = parser.parse_args(argv)
    try:
        result = check(args.language, args.seconds)
    except ValueError as exc:
        from backend import own_social_media_render as render

        code = str(exc)
        allowed = render.LOCAL_ERRORS | {
            "probe_required",
            "renderer_required",
            "font_required",
            "captures_changed",
        }
        result = {
            "ok": False,
            "error": code if code in allowed else "check_failed",
            "provider_calls": 0,
        }
    except (OSError, subprocess.SubprocessError, KeyError, StopIteration):
        result = {"ok": False, "error": "check_failed", "provider_calls": 0}
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
