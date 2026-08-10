#!/usr/bin/env python3
"""
actorsbookmag — SNSアセット生成（Instagramカルーセル + TikTok/Reels動画）

1記事から:
  - IGカルーセル画像（1080x1350, 表紙+本文+アウトロ）
  - 縦型動画（1080x1920 MP4, スライドを繋いだTikTok/Reels用）
を生成する。すべてPillow + ffmpegでローカル生成（APIキー不要）。

使い方:
  ./.venv/bin/python scripts/make_assets.py --id stranger-7-3-2ba28a43-1782978609
  ./.venv/bin/python scripts/make_assets.py --id <id> --seconds 3.0
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pipeline  # noqa: E402  _font / _wrap_jp / JP_FONT / LATIN_FONT を再利用

from PIL import Image, ImageDraw  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
NEWS_JSON = ROOT / "data" / "news.json"
RED = (255, 60, 60)
_font = pipeline._font
_wrap = pipeline._wrap_jp


def _find_font(candidates, fallback, globs=()):
    for p in candidates:
        if Path(p).exists():
            return p
    import glob
    for pat in globs:  # Linux等でファイル名が違っても拾えるよう総当たり
        hits = sorted(glob.glob(pat, recursive=True))
        if hits:
            return hits[0]
    return fallback


# macOS(ローカル)とLinux(GitHub Actions)の両方でフォントを解決
JP = _find_font([
    "/System/Library/Fonts/Hiragino Sans GB.ttc",               # macOS
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",      # Linux (fonts-noto-cjk)
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
], pipeline.JP_FONT, globs=[
    "/usr/share/fonts/**/*CJK*.ttc", "/usr/share/fonts/**/*CJK*.otf",
    "/usr/share/fonts/**/NotoSansJP*.*", "/usr/share/fonts/**/*apanese*.*",
])
LAT = _find_font([
    "/System/Library/Fonts/Supplemental/Arial Black.ttf",       # macOS
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",     # Linux
], pipeline.LATIN_FONT, globs=["/usr/share/fonts/**/DejaVuSans-Bold.ttf"])
# ヒラギノは太字がindex 1、それ以外(Noto等)はindex 0
JP_BOLD_IDX = 1 if "Hiragino" in JP else 0


def _base(W, H, dark):
    img = Image.new("RGB", (W, H), (10, 10, 10) if dark else (255, 255, 255))
    return img, ImageDraw.Draw(img)


def _load_bg(rec):
    """記事の背景画像(登録画像)を読み込む。無ければNone。"""
    v = rec.get("background") or ""
    try:
        if v.startswith("/"):
            p = ROOT / v.lstrip("/")
            if p.exists():
                return Image.open(p).convert("RGB")
        elif v.startswith("http"):
            import io
            import requests
            r = requests.get(v, timeout=20); r.raise_for_status()
            return Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception:
        return None
    return None


def _cover_base(rec, W, H):
    """表紙の下地を返す。背景画像があれば『画像＋黒スモーク』、無ければ黒。"""
    bg = _load_bg(rec)
    if bg is None:
        return _base(W, H, True)
    img = pipeline._cover(bg, W, H)                       # object-fit: cover
    veil = Image.new("RGB", (W, H), (0, 0, 0))
    img = Image.blend(img, veil, 0.30)                    # 全体を薄く暗く(黒スモーク)
    grad = Image.new("L", (1, H), 0)                      # 下ほど濃く(見出し可読性)
    for y in range(H):
        grad.putpixel((0, y), int(235 * max(0.0, (y - H * 0.42) / (H * 0.58))))
    img = Image.composite(Image.new("RGB", (W, H), (0, 0, 0)), img, grad.resize((W, H)))
    return img, ImageDraw.Draw(img)


def _header(d, W, dark, cat, num=None, total=None):
    fg = (255, 255, 255) if dark else (17, 17, 17)
    m = int(W * 0.066)
    d.text((m, int(W * 0.06)), "ACTORSBOOK", font=_font(LAT, 30), fill=fg)
    d.text((m, int(W * 0.06) + 40), cat, font=_font(JP, 22, index=JP_BOLD_IDX), fill=RED)
    if num:
        d.text((W - m - 96, int(W * 0.06)), f"{num:02d}/{total:02d}",
               font=_font(LAT, 24), fill=(150, 150, 150))


# カテゴリ由来の巨大ゴースト・ワードマーク（背景の"透かし"。カテゴリごとに絵柄が変わる）
GHOST = {
    "映画": ["CINE", "PHILE"],
    "本・書店": ["BIBLIO", "PHILE"],
    "ストリート": ["STREET"],
    "ファッション": ["MODE"],
}


def _pill(d, x, y, text, font, fill=RED, tcol=(255, 255, 255), padx=20, pady=10):
    """角丸の"ピル"バッジを描き、右端X座標を返す。"""
    l, t, r, b = d.textbbox((0, 0), text, font=font)
    tw, th = r - l, b - t
    x1, y1 = x + tw + padx * 2, y + th + pady * 2
    d.rounded_rectangle([x, y, x1, y1], radius=(y1 - y) // 2, fill=fill)
    d.text((x + padx - l, y + pady - t), text, font=font, fill=tcol)
    return x1


def _draw_ghost(d, cat, W, H):
    """黒背景のとき、カテゴリ由来の巨大ワードマークを極薄で敷く。"""
    words = GHOST.get(cat, ["CINE", "PHILE"])[:2]
    gf = _font(LAT, int(W * 0.175))
    gh = int(W * 0.150)
    m = int(W * 0.060)
    y = int(H * 0.170)
    for w in words:
        d.text((m, y), w, font=gf, fill=(26, 26, 26))
        y += gh


def build_cover(rec, W, H, swipe=False):
    """表紙(=サイトのサムネと共通)。写真があれば写真+黒スモーク、無ければ黒+ゴースト。"""
    bg = _load_bg(rec)
    has_photo = bg is not None
    cat = rec.get("category", "映画")
    if has_photo:
        img, d = _cover_base(rec, W, H)
    else:
        img, d = _base(W, H, True)
        _draw_ghost(d, cat, W, H)
    m = int(W * 0.066)
    # マストヘッド ＋ カテゴリのピル
    d.text((m, int(W * 0.052)), "ACTORSBOOK", font=_font(LAT, int(W * 0.041)),
           fill=(255, 255, 255))
    _pill(d, m, int(W * 0.052) + int(W * 0.041) + 20, cat,
          _font(JP, int(W * 0.023), index=JP_BOLD_IDX))
    # 見出し（下寄せ・特大）
    hf = _font(JP, int(W * 0.074), index=JP_BOLD_IDX)
    lines = _wrap(rec.get("headline", ""), 10)[:5]
    lh = int(W * 0.088)
    dek = (rec.get("dek") or "").strip()
    y = H - int(H * 0.085) - lh * len(lines) - (int(W * 0.050) if dek else 0)
    for ln in lines:
        d.text((m, y), ln, font=hf, fill=(255, 255, 255)); y += lh
    if dek:
        df = _font(JP, int(W * 0.029))
        dl = _wrap(dek, 30)[:1]
        d.text((m, y + 12), (dl[0] if dl else dek), font=df, fill=(198, 198, 198))
    if swipe:
        d.text((W - int(W * 0.25), H - int(H * 0.075)), "SWIPE →",
               font=_font(LAT, 30), fill=(255, 255, 255))
    d.rectangle([0, H - 14, W, H], fill=RED)
    return img, d


def write_thumb(rec):
    """サイト用サムネ(1080x1350)を images/news/<id>.jpg に生成し、パスを返す。"""
    W, H = 1080, 1350
    img, _ = build_cover(rec, W, H, swipe=False)
    out = ROOT / "images" / "news" / f"{rec['id']}.jpg"
    out.parent.mkdir(parents=True, exist_ok=True)
    img.save(out, "JPEG", quality=90)
    return f"/images/news/{rec['id']}.jpg"


def build_slides(rec, size):
    """記事から全スライド画像を生成して返す（表紙→本文→アウトロ）。"""
    W, H = size
    m = int(W * 0.066)
    cat = rec.get("category", "映画")
    paras = [p.strip() for p in (rec.get("body", "")).split("\n\n") if p.strip()]
    total = 1 + len(paras) + 1
    slides = []

    # 表紙（サイトのサムネと共通テンプレ：カテゴリ・ゴースト＋下寄せ見出し。IGはSWIPE表示）
    img, _d = build_cover(rec, W, H, swipe=True)
    slides.append(img)

    # 本文（白・大きく読める）
    for i, p in enumerate(paras):
        img, d = _base(W, H, False)
        _header(d, W, False, cat, i + 2, total)
        tf = _font(JP, int(W * 0.044), index=JP_BOLD_IDX)
        tl = _wrap(p, 17)[:16]
        tlh = int(W * 0.070)
        y = (H - tlh * len(tl)) // 2 - 20
        for ln in tl:
            d.text((int(W * 0.074), y), ln, font=tf, fill=(17, 17, 17)); y += tlh
        d.rectangle([0, H - 14, W, H], fill=RED)
        slides.append(img)

    # アウトロ（黒・ブランド）
    img, d = _base(W, H, True)
    d.text((m, H // 2 - 150), "ACTORSBOOK", font=_font(LAT, int(W * 0.061)), fill=(255, 255, 255))
    d.text((m, H // 2 - 34), "記事はプロフィールのリンクから", font=_font(JP, 34), fill=(230, 230, 230))
    d.text((m, H // 2 + 36), rec["headline"], font=_font(JP, 26), fill=(150, 150, 150))
    d.rectangle([0, H - 14, W, H], fill=RED)
    slides.append(img)
    return slides


def save_slides(slides, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, s in enumerate(slides, 1):
        p = out_dir / f"{i:02d}.jpg"
        s.save(p, "JPEG", quality=90)
        paths.append(p)
    return paths


def make_video(slide_paths, out_path, per=2.8, size=(1080, 1920)):
    """スライド画像を繋いで縦型MP4に（ffmpeg）。"""
    W, H = size
    lines = []
    for p in slide_paths:
        lines.append(f"file '{p.resolve()}'")
        lines.append(f"duration {per}")
    lines.append(f"file '{slide_paths[-1].resolve()}'")  # concat demuxerの仕様で最後をもう一度
    listfile = out_path.parent / "_list.txt"
    listfile.write_text("\n".join(lines), encoding="utf-8")
    vf = (f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
          f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:color=black,fps=30,format=yuv420p")
    cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(listfile),
           "-vf", vf, "-c:v", "libx264", "-pix_fmt", "yuv420p", str(out_path)]
    subprocess.run(cmd, check=True, capture_output=True)
    listfile.unlink(missing_ok=True)
    return out_path


def generate_for(rec, seconds):
    """1記事分のIGカルーセル+動画を social/<id>/ に生成し、news.json用のパスを返す。"""
    pub = ROOT / "social" / rec["id"]        # 公開・コミット対象（サイトから配信）
    tmp = ROOT / "output" / "_vid" / rec["id"]  # 動画の作業用スライド（gitignore）
    ig = save_slides(build_slides(rec, (1080, 1350)), pub / "ig")
    vslides = save_slides(build_slides(rec, (1080, 1920)), tmp)
    make_video(vslides, pub / "tiktok.mp4", per=seconds)
    return {
        "carousel": [f"/social/{rec['id']}/ig/{p.name}" for p in ig],
        "video": f"/social/{rec['id']}/tiktok.mp4",
    }


def main():
    ap = argparse.ArgumentParser(description="SNSアセット生成(カルーセル+動画)")
    ap.add_argument("--id", help="対象記事のid")
    ap.add_argument("--all-published", action="store_true",
                    help="公開済みで未生成の記事を一括生成")
    ap.add_argument("--seconds", type=float, default=2.8, help="動画の1枚あたり秒数")
    ap.add_argument("--thumbs", action="store_true",
                    help="サイト用サムネ(1080x1350)を再生成（--id か 全記事）")
    args = ap.parse_args()

    news = json.loads(NEWS_JSON.read_text(encoding="utf-8"))

    # サイト用サムネの一括再生成（テンプレ改良を既存記事へ反映）
    if args.thumbs:
        pool = news if not args.id else [r for r in news
                                         if r["id"] == args.id or r["id"].startswith(args.id)]
        changed = False
        for rec in pool:
            path = write_thumb(rec)
            # 既にthumbnailを持つ記事のみパス更新（下書きに新フィールドを足してnews.jsonを汚さない）
            if rec.get("thumbnail") and rec.get("thumbnail") != path:
                rec["thumbnail"] = path; changed = True
            print(f"サムネ生成: {rec['headline'][:26]} → {path}")
        if changed:
            NEWS_JSON.write_text(json.dumps(news, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"完了: {len(pool)}件のサムネを再生成。")
        return

    if args.all_published:
        targets = [r for r in news if r.get("status") == "published" and not r.get("assets_ready")]
    elif args.id:
        targets = [r for r in news if r["id"] == args.id or r["id"].startswith(args.id)]
    else:
        raise SystemExit("--id か --all-published を指定してください。")
    if not targets:
        print("対象なし（公開済み・未生成の記事がありません）。")
        return

    for rec in targets:
        print(f"生成: {rec['headline'][:30]}")
        assets = generate_for(rec, args.seconds)
        rec["assets"] = assets
        rec["assets_ready"] = True
        print(f"  → カルーセル{len(assets['carousel'])}枚 + 動画 social/{rec['id']}/")

    NEWS_JSON.write_text(json.dumps(news, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完了: {len(targets)}件。news.jsonに assets を記録。")


if __name__ == "__main__":
    main()
