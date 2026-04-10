"""
actorsbookmag — Instagram Banner Generator
films.jsonの各作品からInstagram正方形バナー(1080x1080)を生成
ハイプビースト風：黒背景、白テキスト、アクスントカラー
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    import PIL
except ImportError:
    os.system("pip install Pillow --break-system-packages -q")
    from PIL import Image, ImageDraw, ImageFont, ImageFilter

# ── 設定 ──────────────────────────────────────────
SIZE = (1080, 1080)
BG_COLOR = (8, 8, 8)
ACCENT = (232, 255, 0)      # 黄緑アクセント
WHITE = (240, 240, 240)
GRAY = (100, 100, 100)
RED_ACCENT = (255, 59, 0)

DATA_PATH = Path("data/films.json")
OUT_DIR = Path("data/banners")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── フォント ──────────────────────────────────────
def get_font(size, bold=False):
    """システムフォントをフォールバックで取得"""
    candidates = []
    if bold:
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
        ]
    else:
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
        ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except:
                pass
    return ImageFont.load_default()

# ── サムネイル取得 ────────────────────────────────
def fetch_thumbnail(url: str, size=(1080, 600)) -> Image.Image | None:
    if not url:
        return None
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; actorsbookmag/1.0)"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            img = Image.open(resp).convert("RGB")
            # アスュクト比を維持してクロップ
            img_ratio = img.width / img.height
            target_ratio = size[0] / size[1]
            if img_ratio > target_ratio:
                new_w = int(img.height * target_ratio)
                left = (img.width - new_w) // 2
                img = img.crop((left, 0, left + new_w, img.height))
            else:
                new_h = int(img.width / target_ratio)
                top = (img.height - new_h) // 2
                img = img.crop((0, top, img.width, top + new_h))
            return img.resize(size, Image.LANCZOS)
    except Exception as e:
        print(f"  thumbnail fetch failed: {e}", file=sys.stderr)
        return None

# ── テキスト折り返し ──────────────────────────────
def wrap_text(text: str, font, max_width: int, draw: ImageDraw.Draw) -> list[str]:
    words = text.split()
    lines = []
    current = ""
    for word in words:
        test = (current + " " + word).strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] > max_width and current:
            lines.append(current)
            current = word
        else:
            current = test
    if current:
        lines.append(current)
    return lines

# ── バナー生成メイン ──────────────────────────────
def generate_banner(film: dict) -> Path:
    title = film.get("title", "Untitled")
    director = film.get("director", "")
    year = str(film.get("year", "")) if film.get("year") else ""
    source = film.get("source", "").upper()
    country = film.get("country", "")
    comment = film.get("comment", "")
    thumbnail_url = film.get("thumbnail", "")
    film_id = film.get("id", title.lower().replace(" ", "-"))

    out_path = OUT_DIR / f"{film_id}.jpg"

    # ── キャンバス ──
    img = Image.new("RGB", SIZE, BG_COLOR)
    draw = ImageDraw.Draw(img)

    # ── サムネイル背景（上部600px）──
    thumb = fetch_thumbnail(thumbnail_url, size=(1080, 600))
    if thumb:
        # 暗めのオーバーレイ
        overlay = Image.new("RGB", (1080, 600), (0, 0, 0))
        img.paste(thumb, (0, 0))
        img.paste(overlay, (0, 0), Image.fromarray(
            __import__('numpy', fromlist=[]).full((600, 1080), 140, dtype='uint8')
            if False else
            Image.new("L", (1080, 600), 140)
        ))
    else:
        # サムネイルなし：グラデーション風
        for y in range(600):
            alpha = int(30 + (y / 600) * 60)
            draw.line([(0, y), (1080, y)], fill=(alpha, alpha, alpha))

    # ── グラデーション（写真→テキストエリアの境界）──
    for y in range(520, 600):
        alpha = int(255 * (y - 520) / 80)
        draw.line([(0, y), (1080, y)], fill=(8, 8, int(8 * alpha / 255) + 8))

    # ── テキストエリア背景（下部480px）──
    draw.rectangle([(0, 600), (1080, 1080)], fill=BG_COLOR)

    # ── SOURCEタグ ──
    font_tag = get_font(22, bold=True)
    tag_text = source if source else "FILM"
    tag_bbox = draw.textbbox((0, 0), tag_text, font=font_tag)
    tag_w = tag_bbox[2] + 20
    draw.rectangle([(40, 620), (40 + tag_w, 650)], fill=ACCENT)
    draw.text((50, 623), tag_text, font=font_tag, fill=BG_COLOR)

    # ── タイトル ──
    font_title_lg = get_font(72, bold=True)
    font_title_md = get_font(54, bold=True)
    font_title_sm = get_font(42, bold=True)

    # タイトル長さに応じてフォントサイズ調整
    if len(title) <= 20:
        font_title = font_title_lg
    elif len(title) <= 35:
        font_title = font_title_md
    else:
        font_title = font_title_sm

    title_lines = wrap_text(title.upper(), font_title, 1000, draw)
    title_y = 670
    for line in title_lines[:3]:
        draw.text((40, title_y), line, font=font_title, fill=WHITE)
        bbox = draw.textbbox((40, title_y), line, font=font_title)
        title_y = bbox[3] + 8

    # ── ディレクター / 年 ──
    font_sub = get_font(28)
    sub_parts = []
    if director:
        sub_parts.append(f"Dir. {director}")
    if year:
        sub_parts.append(year)
    if country:
        sub_parts.append(country)
    sub_text = "  /  ".join(sub_parts)
    if sub_text:
        draw.text((40, title_y + 16), sub_text, font=font_sub, fill=GRAY)
        title_y += 50

    # ── アクセントライン ──
    draw.rectangle([(40, title_y + 20), (120, title_y + 23)], fill=ACCENT)

    # ── コメント（3行まで）──
    if comment:
        font_comment = get_font(26)
        comment_lines = wrap_text(comment[:120] + ("…" if len(comment) > 120 else ""),
                                   font_comment, 1000, draw)
        cy = title_y + 40
        for line in comment_lines[:3]:
            draw.text((40, cy), line, font=font_comment, fill=(180, 180, 180))
            cy += 36

    # ── サイトロゴ（右下）──
    font_logo = get_font(22, bold=True)
    logo = "ACTORSBOOKMAG"
    logo_bbox = draw.textbbox((0, 0), logo, font=font_logo)
    draw.text((1080 - logo_bbox[2] - 40, 1040), logo, font=font_logo, fill=GRAY)

    # ── 左端アクセントバー ──
    draw.rectangle([(0, 620), (4, 1080)], fill=ACCENT)

    img.save(out_path, "JPEG", quality=92)
    print(f"  ✓ {out_path}")
    return out_path


# ── エントリポイント ──────────────────────────────
def main():
    if not DATA_PATH.exists():
        print(f"ERROR: {DATA_PATH} not found", file=sys.stderr)
        sys.exit(1)

    films = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    print(f"Generating banners for {len(films)} films...")

    generated = []
    for film in films:
        try:
            path = generate_banner(film)
            generated.append(str(path))
        except Exception as e:
            print(f"  ✗ {film.get('title', '?')}: {e}", file=sys.stderr)

    # バナーリストをJSONで保存
    banners_index = OUT_DIR / "index.json"
    banners_index.write_text(
        json.dumps({"banners": generated, "count": len(generated)},
                   ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\nDone: {len(generated)} banners → {OUT_DIR}/")


if __name__ == "__main__":
    main()
