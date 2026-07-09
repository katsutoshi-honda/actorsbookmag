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


def _find_font(candidates, fallback):
    for p in candidates:
        if Path(p).exists():
            return p
    return fallback


# macOS(ローカル)とLinux(GitHub Actions)の両方でフォントを解決
JP = _find_font([
    "/System/Library/Fonts/Hiragino Sans GB.ttc",               # macOS
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",      # Linux (fonts-noto-cjk)
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
], pipeline.JP_FONT)
LAT = _find_font([
    "/System/Library/Fonts/Supplemental/Arial Black.ttf",       # macOS
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",     # Linux
], pipeline.LATIN_FONT)
# ヒラギノは太字がindex 1、それ以外(Noto等)はindex 0
JP_BOLD_IDX = 1 if "Hiragino" in JP else 0


def _base(W, H, dark):
    img = Image.new("RGB", (W, H), (10, 10, 10) if dark else (255, 255, 255))
    return img, ImageDraw.Draw(img)


def _header(d, W, dark, cat, num=None, total=None):
    fg = (255, 255, 255) if dark else (17, 17, 17)
    m = int(W * 0.066)
    d.text((m, int(W * 0.06)), "ACTORSBOOK", font=_font(LAT, 30), fill=fg)
    d.text((m, int(W * 0.06) + 40), cat, font=_font(JP, 22, index=JP_BOLD_IDX), fill=RED)
    if num:
        d.text((W - m - 96, int(W * 0.06)), f"{num:02d}/{total:02d}",
               font=_font(LAT, 24), fill=(150, 150, 150))


def build_slides(rec, size):
    """記事から全スライド画像を生成して返す（表紙→本文→アウトロ）。"""
    W, H = size
    m = int(W * 0.066)
    cat = rec.get("category", "映画")
    paras = [p.strip() for p in (rec.get("body", "")).split("\n\n") if p.strip()]
    total = 1 + len(paras) + 1
    slides = []

    # 表紙（黒・見出し下寄せ）
    img, d = _base(W, H, True)
    _header(d, W, True, cat)
    hf = _font(JP, int(W * 0.076), index=JP_BOLD_IDX)
    lines = _wrap(rec["headline"], 9)[:5]
    lh = int(W * 0.09)
    y = H - int(H * 0.11) - lh * len(lines)
    for ln in lines:
        d.text((m, y), ln, font=hf, fill=(255, 255, 255)); y += lh
    if rec.get("dek"):
        d.text((m, y + 14), rec["dek"], font=_font(JP, 30), fill=(200, 200, 200))
    d.text((W - int(W * 0.25), H - int(H * 0.07)), "SWIPE →", font=_font(LAT, 30), fill=(255, 255, 255))
    d.rectangle([0, H - 14, W, H], fill=RED)
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
    args = ap.parse_args()

    news = json.loads(NEWS_JSON.read_text(encoding="utf-8"))
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
