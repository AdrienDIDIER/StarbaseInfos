from __future__ import annotations

import io
import logging
import os
from typing import List, Optional, Sequence, Tuple

from PIL import Image, ImageDraw, ImageFont

CARD_SIZE = (1200, 675)
MARGIN = 64

BG = (11, 15, 23)
PANEL = (20, 27, 40)
TEXT = (236, 240, 247)
MUTED = (140, 152, 173)
DIM = (72, 84, 105)

ACCENTS = {
    "new": (255, 176, 32),
    "updated": (64, 156, 255),
    "cancelled": (255, 92, 92),
    "info": (64, 156, 255),
    "go": (52, 199, 123),
}

_FONT_DIRS = [
    "/usr/share/fonts/truetype/dejavu",
    "C:/Windows/Fonts",
    "/Library/Fonts",
    "/System/Library/Fonts/Supplemental",
]
_FONT_FILES = {
    True: ["DejaVuSans-Bold.ttf", "segoeuib.ttf", "arialbd.ttf", "Arial Bold.ttf"],
    False: ["DejaVuSans.ttf", "segoeui.ttf", "arial.ttf", "Arial.ttf"],
}


def _font(size: int, bold: bool = False):
    for directory in _FONT_DIRS:
        for name in _FONT_FILES[bold]:
            path = os.path.join(directory, name)
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
    # Police bitmap de secours : la carte reste lisible mais petite (installer fonts-dejavu-core)
    logging.warning("[CARDS] No TrueType font found, falling back to the default bitmap font.")
    return ImageFont.load_default()


def _width(draw: ImageDraw.ImageDraw, text: str, font) -> int:
    return int(draw.textlength(text, font=font))


def _fit_font(draw, text: str, max_width: int, size: int, bold: bool, min_size: int = 26):
    font = _font(size, bold)
    while size > min_size and _width(draw, text, font) > max_width:
        size -= 2
        font = _font(size, bold)
    return font


def _wrap(draw, text: str, font, max_width: int, max_lines: int = 2) -> List[str]:
    words, lines, current = text.split(), [], ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if _width(draw, candidate, font) <= max_width or not current:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        while lines[-1] and _width(draw, lines[-1] + "\u2026", font) > max_width:
            lines[-1] = lines[-1][:-1]
        lines[-1] = lines[-1].rstrip() + "\u2026"
    return lines


def _draw_check(draw, x: int, y: int, size: int, done: bool) -> None:
    box = [x, y, x + size, y + size]
    if done:
        draw.ellipse(box, fill=ACCENTS["go"])
        w = max(size // 9, 3)
        draw.line(
            [(x + size * 0.27, y + size * 0.52), (x + size * 0.44, y + size * 0.69), (x + size * 0.74, y + size * 0.33)],
            fill=BG, width=w, joint="curve",
        )
    else:
        draw.ellipse(box, outline=DIM, width=4)


def render_card(
    tag: str,
    title: str,
    rows: Sequence[Tuple[str, str]] = (),
    checklist: Optional[Sequence[Tuple[bool, str]]] = None,
    accent: str = "info",
    footer: Optional[str] = None,
) -> bytes:
    """
    Render a 1200x675 PNG card: a coloured tag, a title, then label/value rows
    and/or a checklist. Returns the PNG bytes.
    """
    color = ACCENTS.get(accent, ACCENTS["info"])
    img = Image.new("RGB", CARD_SIZE, BG)
    draw = ImageDraw.Draw(img)
    width, height = CARD_SIZE
    inner = width - 2 * MARGIN

    draw.rectangle([0, 0, 14, height], fill=color)

    # Tag
    tag_font = _font(30, bold=True)
    tag_text = tag.upper()
    tag_w = _width(draw, tag_text, tag_font) + 40
    draw.rounded_rectangle([MARGIN, MARGIN - 8, MARGIN + tag_w, MARGIN + 46], radius=10, fill=color)
    draw.text((MARGIN + 20, MARGIN + 19), tag_text, font=tag_font, fill=BG, anchor="lm")

    # Titre
    title_font = _fit_font(draw, title, inner, 68, bold=True, min_size=44)
    y = MARGIN + 78
    for line in _wrap(draw, title, title_font, inner, max_lines=2):
        draw.text((MARGIN, y), line, font=title_font, fill=TEXT)
        y += int(title_font.size * 1.18)
    y += 22

    footer_top = height - MARGIN - 34
    items = len(rows) + len(checklist or ())
    row_h = min(104, max(64, (footer_top - y) // max(items, 1)))

    label_font = _font(24, bold=True)
    for label, value in rows:
        draw.text((MARGIN, y), label.upper(), font=label_font, fill=MUTED)
        value_font = _fit_font(draw, value, inner, min(44, row_h - 44), bold=False)
        value_line = _wrap(draw, value, value_font, inner, max_lines=1)[0] if value else ""
        draw.text((MARGIN, y + 32), value_line, font=value_font, fill=TEXT)
        y += row_h

    check_size = 44
    for done, label in checklist or ():
        _draw_check(draw, MARGIN, y + 6, check_size, done)
        font = _fit_font(draw, label, inner - check_size - 24, 38, bold=False)
        draw.text(
            (MARGIN + check_size + 24, y + 6 + check_size // 2), label,
            font=font, fill=TEXT if done else MUTED, anchor="lm",
        )
        y += max(row_h - 24, 62)

    # Pied
    draw.line([(MARGIN, footer_top), (width - MARGIN, footer_top)], fill=PANEL, width=3)
    footer_font = _font(24)
    footer = footer or os.getenv("CARD_FOOTER", "Starbase Infos")
    draw.text((MARGIN, footer_top + 16), footer, font=footer_font, fill=MUTED)

    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def safe_render_card(*args, **kwargs) -> Optional[bytes]:
    """Une carte ratée ne doit jamais bloquer une publication."""
    try:
        return render_card(*args, **kwargs)
    except Exception as e:
        logging.warning(f"[CARDS] Card rendering failed: {e}")
        return None
