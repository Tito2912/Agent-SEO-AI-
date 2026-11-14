"""Real local encoder/decoder proof with synthetic silence, never a paid voice call."""

import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import wave

WEB = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WEB))
from backend import own_social_media_render as r, own_social_media as m  # noqa: E402

output = Path(os.environ.get("SOCIAL_MEDIA_OUTPUT", "/tmp/noyaru-media-proof"))
output.mkdir(parents=True, exist_ok=True)
buffer = io.BytesIO()
with wave.open(buffer, "wb") as voice:
    voice.setnchannels(1)
    voice.setsampwidth(2)
    voice.setframerate(24000)
    voice.writeframes(b"\x00\x00" * 24000 * 30)
observations = []
for lang in ("fr", "en"):
    packet = {
        "lang": lang,
        "scenes": [
            {"heading": h, "capture": c, "text": t} for h, c, t in m.SCENES[lang]
        ],
    }
    for i in range(3):
        (output / f"{lang}-frame-{i}.png").write_bytes(r.frame(packet, i))
    result = r.render(packet, buffer.getvalue())
    video = output / f"{lang}.mp4"
    video.write_bytes(base64.b64decode(result["video"]))
    check = subprocess.run(
        [r.executable(), "-v", "error", "-i", str(video), "-f", "null", "-"],
        capture_output=True,
        timeout=60,
    )
    assert check.returncode == 0, check.stderr
    observations.append(
        {k: v for k, v in result.items() if k != "video"}
        | {
            "lang": lang,
            "full_decode": True,
            "audio": "synthetic silence, no speech quality claim",
        }
    )
(output / "render-results.json").write_text(
    json.dumps(observations, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps({"renders": len(observations), "output": str(output)}))
