"""Encoder failures stay local and expose fixed diagnoses, never raw stderr."""

import errno
from pathlib import Path
import shutil
import subprocess
import sys
import time

import pytest

from backend import own_social_media as media, own_social_media_render as render
from test_own_social_audio import wav


def packet():
    return {
        "lang": "fr",
        "scenes": [
            {"heading": h, "capture": c, "text": t} for h, c, t in media.SCENES["fr"]
        ],
    }


@pytest.mark.parametrize(
    "failure,expected",
    [
        (subprocess.TimeoutExpired("fixture", 180), "render_timeout"),
        (OSError(errno.ENOMEM, "private detail"), "render_resources"),
        (OSError(errno.EAGAIN, "private detail"), "render_resources"),
        (PermissionError(errno.EACCES, "private detail"), "render_unavailable"),
        ((-9, b"private detail"), "render_interrupted"),
        ((-15, b"private detail"), "render_interrupted"),
        ((1, b"Cannot allocate memory"), "render_resources"),
        ((1, b"pthread_create() failed"), "render_resources"),
        ((1, b"Resource temporarily unavailable"), "render_resources"),
        ((1, b"private path or other details"), "render_failed"),
        ((0, b""), "render_invalid"),
    ],
    ids=[
        "timeout",
        "memory",
        "process-limit",
        "permissions",
        "killed",
        "terminated",
        "malloc",
        "threads",
        "resources",
        "encoder",
        "missing-file",
    ],
)
def test_fixed_encoder_diagnoses(monkeypatch, failure, expected):
    monkeypatch.setattr(render, "executable", lambda: "/fixture/ffmpeg")

    def run(*args, **kwargs):
        if isinstance(failure, Exception):
            raise failure
        return subprocess.CompletedProcess(args[0], failure[0], b"", failure[1])

    monkeypatch.setattr(render.subprocess, "run", run)
    with pytest.raises(ValueError, match="^" + expected + "$"):
        render.render(packet(), wav(8))


@pytest.mark.skipif(
    sys.platform != "linux"
    or not shutil.which("ffmpeg")
    or not shutil.which("ffprobe"),
    reason="Local Linux encoder/decoder resource proof needs FFmpeg and ffprobe",
)
def test_real_video_is_decodable_with_a_small_encoder_memory_budget(monkeypatch):
    from ops import social_media_doctor as doctor

    original = subprocess.run
    peaks = []

    def bounded(command, **kwargs):
        if "scenes.txt" not in command:
            return original(command, **kwargs)
        # Expose 24 CPUs to reproduce automatic thread pools even on a small runner.
        command = [command[0], "-cpucount", "24", *command[1:]]
        timeout = kwargs.pop("timeout")
        kwargs.pop("check")
        kwargs.pop("capture_output")
        started = time.monotonic()
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        ) as child:
            try:
                while True:
                    try:
                        status = Path(f"/proc/{child.pid}/status").read_text(
                            encoding="utf-8"
                        )
                        rss = next(
                            (
                                int(line.split()[1])
                                for line in status.splitlines()
                                if line.startswith("VmRSS:")
                            ),
                            0,
                        )
                        peaks.append(rss)
                        assert rss < 256 * 1024, "encoder exceeded 256 MiB RSS"
                    except FileNotFoundError:
                        pass
                    assert time.monotonic() - started < timeout
                    try:
                        stdout, stderr = child.communicate(timeout=0.02)
                        break
                    except subprocess.TimeoutExpired:
                        continue
            finally:
                if child.poll() is None:
                    child.kill()
                    child.communicate()
        return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)

    monkeypatch.setattr(render.subprocess, "run", bounded)
    result = doctor.check(seconds=8)
    assert result["full_decode"] and result["provider_calls"] == 0
    assert result["database_access"] is False and peaks and max(peaks) > 0


def test_a_headerless_file_is_not_a_success(monkeypatch):
    monkeypatch.setattr(render, "executable", lambda: "/fixture/ffmpeg")

    def run(command, *, cwd, **kwargs):
        (Path(cwd) / "video.mp4").write_bytes(b"0" * 2000)
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(render.subprocess, "run", run)
    with pytest.raises(ValueError, match="^render_invalid$"):
        render.render(packet(), wav(8))


def test_doctor_never_prints_an_unrecognized_exception(monkeypatch, capsys):
    from ops import social_media_doctor as doctor

    def fail(*args):
        raise ValueError("fictional confidential diagnostic")

    monkeypatch.setattr(doctor, "check", fail)
    assert doctor.main([]) == 1
    output = capsys.readouterr().out
    assert '"error": "check_failed"' in output and "confidential" not in output
