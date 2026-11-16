"""Export the approved Noyaru artwork for the web, favicons and installed app.

Only sizing, framing and encoding happen here: typography and colors come from
the approved transparent PNG. No model or network call, and no app/DB import.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import reportlab
from PIL import Image, ImageDraw, ImageFont

WEB = Path(__file__).resolve().parents[1]
VERSION = "5"
BG = (17, 17, 19, 255)
FONT = Path(reportlab.__file__).resolve().parent / "fonts" / "Vera.ttf"


def fitted(art: Image.Image, size: tuple[int, int], inset: int, *, opaque: bool = False) -> Image.Image:
    canvas = Image.new("RGBA", size, BG if opaque else (0, 0, 0, 0))
    scaled = art.copy()
    scaled.thumbnail((size[0] - 2 * inset, size[1] - 2 * inset), Image.Resampling.LANCZOS)
    canvas.alpha_composite(scaled, ((size[0] - scaled.width) // 2, (size[1] - scaled.height) // 2))
    return canvas


def build(source: Path, static: Path) -> dict:
    approved = Image.open(source).convert("RGBA")
    alpha = approved.getchannel("A")
    ink = alpha.point(lambda value: 255 if value >= 160 else 0)
    bounds = ink.getbbox()
    if not bounds:
        raise ValueError("The approved logo has no opaque artwork")
    # Connected artwork at the left is the approved N; letters remain separate.
    seed = (round(approved.width * 350 / 2172), round(approved.height * 350 / 724))
    ImageDraw.floodfill(ink, seed, 128)
    symbol_bounds = ink.point(lambda value: 255 if value == 128 else 0).getbbox()
    if not symbol_bounds or symbol_bounds[2] > approved.width * .34:
        raise ValueError("The N cannot be isolated; inspect the approved source")
    word = approved.crop(bounds)
    symbol = approved.crop(symbol_bounds)
    brand = static / "brand"
    brand.mkdir(parents=True, exist_ok=True)
    logo = fitted(word, (1200, 316), 12)
    logo.save(brand / "noyaru-logo.png", optimize=True)
    logo.save(brand / "noyaru-logo.webp", lossless=True, exact=True, method=6)
    icon = fitted(symbol, (512, 512), 30)
    icon.save(brand / "noyaru-symbol.png", optimize=True)
    for side in (16, 32, 48, 64, 96):
        icon.resize((side, side), Image.Resampling.LANCZOS).save(static / f"favicon-{side}x{side}.png", optimize=True)
    icon.save(static / "favicon.ico", sizes=[(n, n) for n in (16, 32, 48, 64)])
    fitted(symbol, (180, 180), 20, opaque=True).save(static / "apple-touch-icon.png", optimize=True)
    for side in (192, 512):
        icon.resize((side, side), Image.Resampling.LANCZOS).save(static / f"icon-{side}.png", optimize=True)
        # The entire foreground fits the manifest's central safe circle.
        fitted(symbol, (side, side), math.ceil(side * .21), opaque=True).save(
            static / f"icon-maskable-{side}.png", optimize=True,
        )

    # Keep the established social-card copy; only the branding is replaced.
    card = Image.new("RGB", (1200, 630))
    pixels = card.load()
    for y in range(630):
        for x in range(1200):
            glow = max(0., 1. - math.hypot((x - 600) / 720, (y - 220) / 570))
            pixels[x, y] = (17 + round(6 * glow), 17 + round(12 * glow), 19 + round(7 * glow))
    card = card.convert("RGBA")
    large = word.copy()
    large.thumbnail((760, 210), Image.Resampling.LANCZOS)
    card.alpha_composite(large, ((1200 - large.width) // 2, 110))
    draw = ImageDraw.Draw(card)
    parts = [("L'audit SEO qui livre ", "#f1f4f1"), ("le correctif", "#65b588"),
             (", pas la liste de tâches.", "#f1f4f1")]
    size = 42
    headline = ImageFont.truetype(str(FONT), size=size)
    while sum(draw.textlength(text, font=headline) for text, _ in parts) > 1120:
        size -= 1
        headline = ImageFont.truetype(str(FONT), size=size)
    x = (1200 - sum(draw.textlength(text, font=headline) for text, _ in parts)) / 2
    for text, color in parts:
        draw.text((x, 368), text, font=headline, fill=color)
        x += draw.textlength(text, font=headline)
    draw.text((600, 452), "Corrections SEO en pull request, vérifiées par votre CI.",
              font=ImageFont.truetype(str(FONT), size=29), anchor="mm", fill="#929497")
    draw.text((600, 590), "noyaru.com", font=ImageFont.truetype(str(FONT), size=30), anchor="mm", fill="#65b588")
    card.convert("RGB").save(brand / "noyaru-partage.png", optimize=True)
    return {"version": VERSION, "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "source_size": list(approved.size), "word_bounds": list(bounds),
            "symbol_bounds": list(symbol_bounds), "logo_size": [1200, 316],
            "webp": "lossless, exact RGBA", "design_regenerated": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Approved transparent PNG")
    parser.add_argument("--static", type=Path, default=WEB / "static")
    args = parser.parse_args()
    print(json.dumps(build(args.source, args.static), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
