"""Fixed branded frames and local FFmpeg. No arbitrary URLs, paths, HTML or commands."""

import base64
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import wave

from PIL import Image, ImageDraw, ImageFont

WEB = Path(__file__).resolve().parents[1]
CAPTURES = WEB / "content/social-demo"
MAX_FILE = 12_000_000


def sha(value):
    return hashlib.sha256(value).hexdigest()


def captures():
    try:
        manifest = json.loads((CAPTURES / "manifest.json").read_text(encoding="utf-8"))
        expected_files = {
            lang + "-" + scene + ".png"
            for lang in ("fr", "en")
            for scene in ("score", "explanation", "issues")
        }
        if set(manifest["files"]) != expected_files or not manifest["sources"]:
            raise ValueError("captures_changed")
        for path, expected in manifest["sources"].items():
            if sha((WEB / path).read_bytes()) != expected:
                raise ValueError("captures_changed")
        for name, expected in manifest["files"].items():
            if sha((CAPTURES / name).read_bytes()) != expected:
                raise ValueError("captures_changed")
        return sha(json.dumps(manifest, sort_keys=True).encode())
    except (OSError, KeyError, TypeError, AttributeError, json.JSONDecodeError):
        raise ValueError("captures_changed") from None


def executable():
    # Test seam only; deployment uses its installed binary, never request input.
    value = shutil.which("ffmpeg")
    if not value:
        raise ValueError("renderer_required")
    font(21)
    return value


def font(size, bold=False):
    names = (
        ("DejaVuSans-Bold.ttf", "arialbd.ttf")
        if bold
        else ("DejaVuSans.ttf", "arial.ttf")
    )
    for name in names:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    raise ValueError("font_required")


def wrapped(draw, text, xy, *, size=28, width=600, fill="#f3f5f3", bold=False):
    face = font(size, bold)
    lines, line = [], ""
    for word in text.split():
        candidate = (line + " " + word).strip()
        if draw.textlength(candidate, font=face) > width and line:
            lines.append(line)
            line = word
        else:
            line = candidate
    lines.append(line)
    if len(lines) > 7:
        raise ValueError("frame_limit")
    draw.multiline_text(xy, "\n".join(lines), font=face, fill=fill, spacing=9)


def frame(packet, index):
    lang = packet["lang"]
    scene = packet["scenes"][index]
    canvas = Image.new("RGB", (720, 1280), "#101311")
    draw = ImageDraw.Draw(canvas)
    draw.text((54, 100), "Noyaru", font=font(39, True), fill="#a8d0b5")
    draw.text((560, 111), f"{index + 1} / 3", font=font(24), fill="#a8d0b5")
    wrapped(draw, scene["heading"], (54, 184), size=34, bold=True)
    with Image.open(CAPTURES / (lang + "-" + scene["capture"] + ".png")) as screenshot:
        screenshot.thumbnail((612, 470))
        canvas.paste(
            screenshot.convert("RGB"),
            ((720 - screenshot.width) // 2, 338 + (470 - screenshot.height) // 2),
        )
    wrapped(draw, scene["text"], (54, 850), size=27)
    draw.rounded_rectangle((54, 1110, 666, 1116), radius=3, fill="#28372c")
    draw.rounded_rectangle(
        (54, 1110, 54 + 204 * (index + 1), 1116), radius=3, fill="#a8d0b5"
    )
    note = (
        "AI-generated voice · Fictional project"
        if lang == "en"
        else "Voix générée par IA · Projet fictif"
    )
    draw.text((54, 1138), note, font=font(21), fill="#b5c5ba")
    result = io.BytesIO()
    canvas.save(result, format="PNG")
    return result.getvalue()


def audio_duration(value):
    try:
        with wave.open(io.BytesIO(value), "rb") as audio:
            duration = audio.getnframes() / audio.getframerate()
            if (
                audio.getnchannels() not in {1, 2}
                or audio.getsampwidth() != 2
                or not 8 <= duration <= 60
            ):
                raise ValueError("audio_invalid")
            return duration
    except (wave.Error, EOFError, ZeroDivisionError):
        raise ValueError("audio_invalid") from None


def render(packet, audio):
    captures()
    duration = audio_duration(audio)
    with tempfile.TemporaryDirectory(prefix="noyaru-render-") as directory:
        root = Path(directory)
        (root / "voice.wav").write_bytes(audio)
        weights = [len(s["text"].split()) for s in packet["scenes"]]
        lines = []
        for i, weight in enumerate(weights):
            (root / f"{i}.png").write_bytes(frame(packet, i))
            lines.extend(
                [f"file '{i}.png'", f"duration {duration * weight / sum(weights):.6f}"]
            )
        lines.append("file '2.png'")
        (root / "scenes.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        command = [
            executable(),
            "-v",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "concat",
            "-safe",
            "1",
            "-i",
            "scenes.txt",
            "-i",
            "voice.wav",
            "-vf",
            "fps=24,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-threads",
            "1",
            "-b:v",
            "900k",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-t",
            str(duration),
            "-shortest",
            "-movflags",
            "+faststart",
            "-fs",
            str(MAX_FILE),
            "video.mp4",
        ]
        try:
            result = subprocess.run(
                command,
                cwd=root,
                capture_output=True,
                timeout=180,
                check=False,
                env={**os.environ, "OMP_NUM_THREADS": "1"},
            )
        except (subprocess.TimeoutExpired, OSError):
            raise ValueError("render_failed") from None
        path = root / "video.mp4"
        if (
            result.returncode
            or not path.is_file()
            or not 1000 < path.stat().st_size < MAX_FILE
        ):
            raise ValueError("render_failed")
        video = path.read_bytes()
        if b"ftyp" not in video[:32]:
            raise ValueError("render_failed")
    return {
        "video": base64.b64encode(video).decode("ascii"),
        "sha256": sha(video),
        "bytes": len(video),
        "duration": round(duration, 2),
        "width": 720,
        "height": 1280,
        "captions": [s["text"] for s in packet["scenes"]],
    }
