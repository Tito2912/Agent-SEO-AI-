"""WAV regression fixtures use synthetic PCM only, never a paid voice service."""

import io
import struct
import wave

import pytest

from backend import own_social_media_render as render


def wav(seconds=24, *, channels=1, width=2, rate=24000):
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setparams((channels, width, rate, 0, "NONE", "not compressed"))
        # A nonempty pattern lets assertions detect any discarded/added sample.
        audio.writeframes(b"\x15" * int(seconds * rate) * channels * width)
    return output.getvalue()


def streaming(value, riff=0xFFFFFFFF, data=0xFFFFFFFF):
    value = bytearray(value)
    struct.pack_into("<I", value, 4, riff)
    struct.pack_into("<I", value, 40, data)
    return bytes(value)


@pytest.mark.parametrize("seconds", [8, 24, 60], ids=["min", "normal", "max"])
def test_actual_pcm_duration_and_samples_are_preserved(seconds):
    value = wav(seconds)
    normalized, duration = render.read_audio(value)
    assert duration == seconds and normalized == value


@pytest.mark.parametrize(
    "riff,data",
    [
        (0xFFFFFFFF, 0xFFFFFFFF),
        (0x7FFFFFFF, 0x7FFFFFFF),
        (0xFFFFFFFF, 0x7FFFFFFF),
        (0xFFFFFFFF, 24 * 24000 * 2),
        (24 * 24000 * 2 + 36, 0xFFFFFFFF),
    ],
    ids=["unsigned-pending", "signed-pending", "mixed", "riff-only", "data-only"],
)
def test_streaming_sizes_use_received_pcm_not_declared_hours(riff, data):
    expected = wav()
    received = streaming(expected, riff, data)
    normalized, duration = render.read_audio(received)
    assert duration == 24 and normalized == expected
    assert render.audio_duration(received) == 24
    with wave.open(io.BytesIO(normalized), "rb") as reader:
        assert reader.readframes(reader.getnframes()) == expected[44:]


@pytest.mark.parametrize("channels", [1, 2], ids=["mono", "stereo"])
def test_streaming_mono_and_stereo_keep_the_channel_count(channels):
    value = wav(channels=channels)
    assert render.read_audio(streaming(value)) == (value, 24)


def test_padding_and_metadata_are_parsed_without_becoming_audio():
    value = wav()
    junk = b"JUNK" + struct.pack("<I", 3) + b"abc\0"
    tail = b"LIST" + struct.pack("<I", 4) + b"INFO"
    received = bytearray(value[:12] + junk + value[12:] + tail)
    struct.pack_into("<I", received, 4, len(received) - 8)
    assert render.read_audio(bytes(received)) == (value, 24)


def test_extensible_pcm_wav_keeps_samples_in_a_standard_pcm_header():
    value = wav()
    pcm_guid = bytes.fromhex("0100000000001000800000aa00389b71")
    fmt = struct.pack(
        "<HHIIHHHHI16s", 0xFFFE, 1, 24000, 48000, 2, 16, 22, 16, 4, pcm_guid
    )
    received = bytearray(
        b"RIFF"
        + struct.pack("<I", 0xFFFFFFFF)
        + b"WAVEfmt "
        + struct.pack("<I", 40)
        + fmt
        + value[36:]
    )
    struct.pack_into("<I", received, 64, 0xFFFFFFFF)
    assert render.read_audio(bytes(received)) == (value, 24)


@pytest.mark.parametrize("pending", [False, True], ids=["finite-riff", "pending-riff"])
def test_truncated_finite_data_is_rejected_even_with_pending_riff(pending):
    value = bytearray(wav()[:-100])
    struct.pack_into("<I", value, 4, 0xFFFFFFFF if pending else len(value) - 8)
    with pytest.raises(ValueError, match="^audio_incomplete$"):
        render.read_audio(bytes(value))


@pytest.mark.parametrize("seconds", [7.99, 60.01], ids=["short", "long"])
def test_real_duration_limits_apply_to_pending_sizes_too(seconds):
    with pytest.raises(ValueError, match="^audio_duration$"):
        render.read_audio(streaming(wav(seconds)))


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("short-header", "audio_invalid"),
        ("not-wav", "audio_format"),
        ("oversized", "audio_invalid"),
        ("finite-riff-too-big", "audio_incomplete"),
        ("partial-chunk-header", "audio_incomplete"),
        ("partial-frame", "audio_format"),
        ("duplicate-data", "audio_format"),
        ("missing-fmt", "audio_format"),
        ("missing-data", "audio_format"),
        ("unsupported-encoding", "audio_format"),
    ],
    ids=[
        "short",
        "not-wav",
        "size",
        "riff",
        "chunk",
        "frame",
        "two-data",
        "fmt",
        "data",
        "codec",
    ],
)
def test_corrupt_audio_is_not_repaired_as_if_it_were_complete(kind, expected):
    value = bytearray(wav())
    if kind == "short-header":
        value = value[:40]
    elif kind == "not-wav":
        value[:4] = b"nope"
    elif kind == "oversized":
        value = b"0" * 9_000_001
    elif kind == "finite-riff-too-big":
        struct.pack_into("<I", value, 4, len(value) + 200)
    elif kind == "partial-chunk-header":
        value += b"LIST"
        struct.pack_into("<I", value, 4, len(value) - 8)
    elif kind == "partial-frame":
        value += b"a\0"
        struct.pack_into("<I", value, 4, len(value) - 8)
        struct.pack_into("<I", value, 40, len(value) - 45)
    elif kind == "duplicate-data":
        value += b"data" + struct.pack("<I", 2) + b"\0\0"
        struct.pack_into("<I", value, 4, len(value) - 8)
    elif kind == "missing-fmt":
        value[12:16] = b"JUNK"
    elif kind == "missing-data":
        value[36:40] = b"JUNK"
    elif kind == "unsupported-encoding":
        struct.pack_into(
            "<H", value, 20, 3
        )  # IEEE float, not the approved PCM16 format.
    with pytest.raises(ValueError, match="^" + expected + "$"):
        render.read_audio(bytes(value))


@pytest.mark.parametrize(
    "params",
    [{"channels": 3}, {"width": 1}, {"width": 4}, {"rate": 4000}, {"rate": 192000}],
    ids=["channels", "pcm8", "pcm32", "low-rate", "high-rate"],
)
def test_unsupported_pcm_parameters_are_rejected(params):
    with pytest.raises(ValueError, match="^audio_format$"):
        render.read_audio(wav(8, **params))
