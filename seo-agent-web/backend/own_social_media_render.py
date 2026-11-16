"""Fixed branded frames and local FFmpeg. No arbitrary URLs, paths, HTML or commands."""

import base64
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import wave

from PIL import Image, ImageDraw, ImageFont

WEB = Path(__file__).resolve().parents[1]
CAPTURES = WEB / "content/social-demo"
MAX_FILE = 12_000_000
LOCAL_ERRORS = frozenset(
    {
        "render_failed",
        "render_timeout",
        "render_resources",
        "render_interrupted",
        "render_unavailable",
        "render_invalid",
    }
)


def process_error(result):
    """Classify fixed encoder failures without exposing stderr or inferring OOM."""
    if result.returncode in {-9, -15}:
        return "render_interrupted"
    message = (result.stderr or b"")[-8192:].lower()
    if any(
        part in message
        for part in (
            b"cannot allocate memory",
            b"out of memory",
            b"resource temporarily unavailable",
            b"pthread_create() failed",
        )
    ):
        return "render_resources"
    return "render_failed"


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


def read_audio(value):
    """Validate actual PCM frames and finalize bounded streaming WAV sizes.

    A streaming writer can leave RIFF/data sizes at 0xffffffff or 0x7fffffff.
    Those are not durations. Repair only these explicit sentinels; a finite
    declaration beyond the received bytes remains a truncated, rejected file.
    """
    if not isinstance(value, bytes) or not 44 <= len(value) <= 9_000_000:
        raise ValueError("audio_invalid")
    if value[:4] != b"RIFF" or value[8:12] != b"WAVE":
        raise ValueError("audio_format")
    pending = {0xFFFFFFFF, 0x7FFFFFFF}
    declared = struct.unpack_from("<I", value, 4)[0]
    if declared not in pending and declared != len(value) - 8:
        raise ValueError("audio_incomplete")
    fixed = bytearray(value)
    struct.pack_into("<I", fixed, 4, len(value) - 8)
    offset, data_size, formats = 12, None, 0
    while offset < len(value):
        if offset + 8 > len(value):
            raise ValueError("audio_incomplete")
        tag, size = struct.unpack_from("<4sI", value, offset)
        start = offset + 8
        if tag == b"data" and size in pending:
            size = len(value) - start
            struct.pack_into("<I", fixed, offset + 4, size)
        if start + size + (size % 2) > len(value):
            raise ValueError("audio_incomplete")
        if tag == b"fmt ":
            formats += 1
        if tag == b"data":
            if data_size is not None or formats != 1:
                raise ValueError("audio_format")
            data_size = size
        offset = start + size + (size % 2)
    if data_size is None or formats != 1:
        raise ValueError("audio_format")
    try:
        with wave.open(io.BytesIO(fixed), "rb") as audio:
            channels, width, rate, frames, compression, _ = audio.getparams()
            if (
                channels not in {1, 2}
                or width != 2
                or not 8000 <= rate <= 96000
                or compression != "NONE"
                or data_size % (channels * width)
                or frames * channels * width != data_size
            ):
                raise ValueError("audio_format")
            duration = frames / rate
            if not 8 <= duration <= 60:
                raise ValueError("audio_duration")
            pcm = audio.readframes(frames)
            if len(pcm) != data_size:
                raise ValueError("audio_incomplete")
        # Canonical header for FFmpeg and saved local retries; samples unchanged.
        output = io.BytesIO()
        with wave.open(output, "wb") as audio:
            audio.setparams((channels, width, rate, 0, "NONE", "not compressed"))
            audio.writeframes(pcm)
        return output.getvalue(), duration
    except (wave.Error, EOFError, ZeroDivisionError, struct.error):
        raise ValueError("audio_format") from None


def audio_duration(value):
    return read_audio(value)[1]


def render(packet, audio):
    captures()
    audio, duration = read_audio(audio)
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
            "-filter_threads",
            "1",
            "-v",
            "error",
            "-nostdin",
            "-y",
            "-f",
            "concat",
            "-safe",
            "1",
            "-threads",
            "1",
            "-i",
            "scenes.txt",
            "-threads",
            "1",
            "-i",
            "voice.wav",
            "-vf",
            # Convert three stills before duplicating them, not every RGB copy.
            "format=yuv420p,fps=24",
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
        except subprocess.TimeoutExpired:
            raise ValueError("render_timeout") from None
        except OSError as exc:
            raise ValueError(
                "render_resources"
                if exc.errno in {errno.ENOMEM, errno.EAGAIN}
                else "render_unavailable"
            ) from None
        path = root / "video.mp4"
        if result.returncode:
            raise ValueError(process_error(result))
        if not path.is_file() or not 1000 < path.stat().st_size < MAX_FILE:
            raise ValueError("render_invalid")
        video = path.read_bytes()
        if b"ftyp" not in video[:32]:
            raise ValueError("render_invalid")
    return {
        "video": base64.b64encode(video).decode("ascii"),
        "sha256": sha(video),
        "bytes": len(video),
        "duration": round(duration, 2),
        "width": 720,
        "height": 1280,
        "captions": [s["text"] for s in packet["scenes"]],
    }
