#!/usr/bin/env python3
"""
actorsbookmag — 映画ニュース自動パイプライン（薄いE2E縦串 MVP）

Hypebeastの映画版。大衆映画を排除したアートハウス映画ニュースを
    ① 収集 → ② 記事化(Claude) → ③ サイトデータへ掲載 → ④ サムネ自動生成 → ⑤ 投稿ペイロード出力
まで1本で貫通する。⑤の実Instagram投稿は方式未定のため差し込み口(stub)のみ。

使い方:
    # RSSから「大衆映画を排除した」最新1件を拾って全段通す
    ./.venv/bin/python scripts/pipeline.py

    # 特定URLの記事を素材に通す
    ./.venv/bin/python scripts/pipeline.py --url https://thefilmstage.com/...

    # 収集候補だけ確認（記事生成やAPI課金なし）
    ./.venv/bin/python scripts/pipeline.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    """リポジトリ直下の .env を読み込んで環境変数へ（既存の環境変数は上書きしない）。"""
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


_load_dotenv()

DATA_DIR = ROOT / "data"
OUT_DIR = ROOT / "output" / "posts"
NEWS_JSON = DATA_DIR / "news.json"

# ── ① 収集: 日本のインディペンデント映画中心のニュースソース ──
FEEDS = [
    ("映画ナタリー", "https://natalie.mu/eiga/feed/news"),
    ("リアルサウンド映画部", "https://realsound.jp/movie/feed"),
    ("Indie Tokyo", "http://indietokyo.com/?feed=rss2"),
    ("neoneo web", "http://webneo.org/feed"),
    ("webDICE", "http://www.webdice.jp/rss/dice_all/"),
    ("映画.com", "https://eiga.com/rss/news/"),
]

# 大衆映画・大作を弾くための除外キーワード（タイトル/本文に含まれたら捨てる）
EXCLUDE_KEYWORDS = [
    # 海外大作/フランチャイズ
    "marvel", "mcu", "dc comics", "box office", "blockbuster", "franchise",
    "spider-man", "avengers", "star wars", "jurassic", "transformers", "minions",
    # 国内の大衆寄りシグナル
    "興行収入", "週末動員", "動員ランキング", "大ヒット御礼", "実写化",
    "マーベル", "ディズニー", "ハリウッド大作", "超大作", "ジャンプ",
]
# 日本インディペンデント寄りを優先するための加点キーワード
BOOST_KEYWORDS = [
    "ミニシアター", "単館", "自主", "インディー", "インディペンデント",
    "ドキュメンタリー", "自主配給", "自主制作", "PFF", "ぴあフィルム",
    "山形", "特集上映", "レトロスペクティブ", "4k修復", "デジタルリマスター",
    "クラウドファンディング", "アップリンク", "ポレポレ", "イメージフォーラム",
    "ユーロスペース", "テアトル", "kʼs cinema", "ケイズシネマ", "第七藝術",
    "監督インタビュー", "carlovy", "ロカルノ", "ベルリン国際", "カンヌ", "ヴェネチア",
    "実験映画", "短編", "映画祭",
]

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) actorsbookmag-bot/0.1"


# ────────────────────────────────────────────────────────────────────
# ① 収集
# ────────────────────────────────────────────────────────────────────
def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", BeautifulSoup(text or "", "html.parser").get_text(" ")).strip()


def _is_mainstream(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in EXCLUDE_KEYWORDS)


def _arthouse_score(text: str) -> int:
    low = text.lower()
    return sum(1 for k in BOOST_KEYWORDS if k in low)


def collect_candidates(limit: int = 20) -> list[dict]:
    """全フィードを走査し、大衆映画を排除した候補をスコア順で返す。"""
    items: list[dict] = []
    for source, url in FEEDS:
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": USER_AGENT})
        except Exception as e:  # noqa: BLE001
            print(f"  ! {source}: フィード取得失敗 ({e})", file=sys.stderr)
            continue
        for entry in feed.entries[:limit]:
            title = _clean(entry.get("title", ""))
            summary = _clean(entry.get("summary", entry.get("description", "")))
            blob = f"{title} {summary}"
            if not title or _is_mainstream(blob):
                continue
            items.append({
                "source": source,
                "title": title,
                "url": entry.get("link", ""),
                "summary": summary,
                "published": entry.get("published", entry.get("updated", "")),
                "arthouse_score": _arthouse_score(blob),
            })
    items.sort(key=lambda x: x["arthouse_score"], reverse=True)
    return items


def fetch_article_source(url: str) -> dict:
    """記事URLから見出し・本文・og:imageを抽出（Claudeへの素材）。"""
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    def _meta(prop: str) -> str:
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        return (tag.get("content") or "").strip() if tag else ""

    title = _meta("og:title") or (_clean(soup.title.string) if soup.title else "")
    image = _meta("og:image")
    paras = [_clean(p.get_text()) for p in soup.select("article p") or soup.select("p")]
    body = "\n".join(p for p in paras if len(p) > 40)[:6000]
    return {"title": title, "image": image, "body": body}


# ────────────────────────────────────────────────────────────────────
# ② 記事化（Claude）
# ────────────────────────────────────────────────────────────────────
ARTICLE_MODEL = os.environ.get("ARTICLE_MODEL", "claude-sonnet-4-6")

ARTICLE_SYSTEM = (
    "あなたは『actorsbookmag』の編集者。Hypebeastのようにエッジが効いていて、"
    "日本のインディペンデント/ミニシアター映画カルチャーに深くコミットしている。"
    "大衆映画・大作フランチャイズは扱わない。日本語で、短く硬質でスタイリッシュな"
    "カルチャーニュースを書く。"
)

ARTICLE_PROMPT = """以下のソースを元に、日本語の映画ニュース記事を作れ。

# ソース
媒体: {source}
元タイトル: {title}
本文抜粋:
{body}

# 編集からのディレクション（最優先で従う）
{direction}

# 要件
- 完全なでっち上げ厳禁。ソースにある事実だけを使う。
- 日本のインディペンデント映画シーンの視点で書く。
- Hypebeast的なトーン（断定的・熱量・固有名詞重視・無駄がない）。
- 以下のJSONだけを返す（前後に説明文を付けない）。

{{
  "headline": "35字以内の日本語見出し",
  "dek": "60字以内のリード（サブ見出し）",
  "body": "250〜400字の本文。改行は\\nで。",
  "category": "映画/本・書店/ストリート/ファッション のいずれか",
  "hashtags": ["#で始まる日本語/英語タグを5個"]
}}"""


def generate_article(source: dict, direction: str = "") -> dict:
    import anthropic

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit(
            "ANTHROPIC_API_KEY が未設定です。`export ANTHROPIC_API_KEY=...` の上で再実行してください。"
        )
    client = anthropic.Anthropic()
    prompt = ARTICLE_PROMPT.format(
        source=source.get("source", ""),
        title=source.get("title", ""),
        body=(source.get("body") or source.get("summary") or "")[:6000],
        direction=direction.strip() or "（特になし。ソースの要点を素直に、熱量高く）",
    )
    msg = client.messages.create(
        model=ARTICLE_MODEL,
        max_tokens=1200,
        system=ARTICLE_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    m = re.search(r"\{.*\}", raw, re.S)
    if not m:
        raise RuntimeError(f"記事JSONの抽出に失敗:\n{raw[:500]}")
    return json.loads(m.group())


# ────────────────────────────────────────────────────────────────────
# ③ サイトデータへ掲載（data/news.json に追記）
# ────────────────────────────────────────────────────────────────────
def slugify(text: str) -> str:
    import hashlib
    norm = unicodedata.normalize("NFKC", text)
    ascii_only = re.sub(r"[^a-zA-Z0-9]+", "-", norm).strip("-").lower()[:40]
    # 日本語見出し等でASCIIが残らない場合に備え、常に短いハッシュを付けてユニーク化
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    return f"{ascii_only}-{digest}" if ascii_only else f"post-{digest}"


PUBLIC_IMG_DIR = ROOT / "images" / "news"  # git管理対象・Vercelで配信される


def save_to_site(article: dict, source: dict, image_path: str | None) -> dict:
    import shutil
    NEWS_JSON.parent.mkdir(parents=True, exist_ok=True)
    news = []
    if NEWS_JSON.exists():
        news = json.loads(NEWS_JSON.read_text(encoding="utf-8"))
    ts = datetime.now(timezone.utc).isoformat()
    rec_id = f"{slugify(article['headline'])}-{int(datetime.now(timezone.utc).timestamp())}"

    # サムネを公開用フォルダへコピーし、サイトルート基準の相対パスで保存
    web_thumb = ""
    if image_path and Path(image_path).exists():
        PUBLIC_IMG_DIR.mkdir(parents=True, exist_ok=True)
        dst = PUBLIC_IMG_DIR / f"{rec_id}.jpg"
        shutil.copyfile(image_path, dst)
        web_thumb = f"/images/news/{rec_id}.jpg"

    record = {
        "id": rec_id,
        "headline": article["headline"],
        "dek": article.get("dek", ""),
        "body": article.get("body", ""),
        "category": article.get("category", "映画"),
        "hashtags": article.get("hashtags", []),
        "source": source.get("source", ""),
        "source_url": source.get("url", ""),
        "thumbnail": web_thumb,
        "published_at": ts,
    }
    news.insert(0, record)  # 新着順
    NEWS_JSON.write_text(json.dumps(news, ensure_ascii=False, indent=2), encoding="utf-8")
    return record


# ────────────────────────────────────────────────────────────────────
# ④ サムネイル自動生成（Pillow）
# ────────────────────────────────────────────────────────────────────
JP_FONT = "/System/Library/Fonts/Hiragino Sans GB.ttc"
LATIN_FONT = "/System/Library/Fonts/Supplemental/Arial Black.ttf"


def _font(path: str, size: int, index: int = 0):
    from PIL import ImageFont
    try:
        return ImageFont.truetype(path, size, index=index)
    except Exception:
        return ImageFont.load_default()


def _wrap_jp(text: str, max_chars: int) -> list[str]:
    """日本語は1文字ずつ折る。ただし英数字の連なり(STRANGER, 7/3 等)は
    途中で分断せず1トークンとして扱う。"""
    # トークン化: 英数記号の語 / 空白 / それ以外(CJK等)は1文字
    tokens = re.findall(r"[A-Za-z0-9._/&+\-]+|\s+|[^A-Za-z0-9._/&+\-\s]", text)
    lines, cur = [], ""
    for tok in tokens:
        if tok.isspace():
            if cur:
                cur += tok
            continue
        # 長すぎる英単語だけは強制分割(1行に収まらない場合の保険)
        if len(tok) > max_chars:
            if cur:
                lines.append(cur.rstrip())
                cur = ""
            while len(tok) > max_chars:
                lines.append(tok[:max_chars])
                tok = tok[max_chars:]
            cur = tok
            continue
        if len(cur) + len(tok) > max_chars and cur:
            lines.append(cur.rstrip())
            cur = ""
        cur += tok
    if cur.strip():
        lines.append(cur.rstrip())
    return lines


def make_thumbnail(article: dict, source: dict, out_dir: Path) -> Path:
    from PIL import Image, ImageDraw, ImageFilter

    W, H = 1080, 1350  # Instagram フィード縦(4:5)
    canvas = Image.new("RGB", (W, H), (10, 10, 10))

    # 背景: 映画スチール（og:image）を全面に敷き、暗く沈める
    img_url = source.get("image", "")
    if img_url:
        try:
            resp = requests.get(img_url, headers={"User-Agent": USER_AGENT}, timeout=20)
            still = Image.open(_bytesio(resp.content)).convert("RGB")
            still = _cover(still, W, H)
            canvas.paste(still, (0, 0))
        except Exception as e:  # noqa: BLE001
            print(f"  ! スチール取得失敗 → 単色背景で継続 ({e})", file=sys.stderr)

    # 下half に黒グラデーションを重ねて可読性を確保
    grad = Image.new("L", (1, H), 0)
    for y in range(H):
        grad.putpixel((0, y), int(255 * min(1.0, max(0.0, (y - H * 0.35) / (H * 0.65)))))
    alpha = grad.resize((W, H))
    black = Image.new("RGB", (W, H), (8, 8, 8))
    canvas = Image.composite(black, canvas, alpha)

    draw = ImageDraw.Draw(canvas)
    margin = 64

    # 上部: ブランド＋（媒体 or カテゴリのキッカー）
    brand_f = _font(LATIN_FONT, 40)
    draw.text((margin, margin), "ACTORSBOOK", font=brand_f, fill=(255, 255, 255))
    src_f = _font(LATIN_FONT, 26)
    src_name = source.get("source", "")
    is_original = src_name in ("", "manual", "actorsbookmag original")
    kicker = source.get("kicker") or (article.get("category", "") if is_original else f"VIA {src_name.upper()}")
    if kicker:
        # 日本語カテゴリはヒラギノ、英字媒体名はArial Black
        kf = _font(JP_FONT, 28, index=1) if is_original else src_f
        draw.text((margin, margin + 52), kicker, font=kf, fill=(255, 60, 60))

    # 写真なし（オリジナル）の場合は、上部に大きな英字キッカーを足して誌面感を出す
    if not img_url:
        big_f = _font(LATIN_FONT, 120)
        draw.text((margin, margin + 150), "CINE", font=big_f, fill=(28, 28, 28))
        draw.text((margin, margin + 270), "PHILE", font=big_f, fill=(28, 28, 28))

    # 見出し（Hiragino W6, 太字寄り, 下寄せ）
    head_f = _font(JP_FONT, 82, index=1)
    lines = _wrap_jp(article["headline"], 9)[:4]
    line_h = 100
    y = H - margin - line_h * len(lines) - 130
    for ln in lines:
        draw.text((margin, y), ln, font=head_f, fill=(255, 255, 255))
        y += line_h

    # dek（小さめ）
    dek_f = _font(JP_FONT, 34, index=0)
    dek_lines = _wrap_jp(article.get("dek", ""), 22)[:2]
    for ln in dek_lines:
        draw.text((margin, y + 8), ln, font=dek_f, fill=(200, 200, 200))
        y += 46

    # 左下アクセントバー
    draw.rectangle([0, H - 14, W, H], fill=(255, 60, 60))

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "thumbnail.jpg"
    canvas.save(path, "JPEG", quality=90)
    return path


def _bytesio(b: bytes):
    import io
    return io.BytesIO(b)


def _cover(img, w: int, h: int):
    """object-fit: cover 相当でリサイズ＆センタークロップ。"""
    src_ratio = img.width / img.height
    dst_ratio = w / h
    if src_ratio > dst_ratio:
        new_h = h
        new_w = int(h * src_ratio)
    else:
        new_w = w
        new_h = int(w / src_ratio)
    img = img.resize((new_w, new_h))
    left = (new_w - w) // 2
    top = (new_h - h) // 2
    return img.crop((left, top, left + w, top + h))


# ────────────────────────────────────────────────────────────────────
# ⑤ 投稿ペイロード出力 + 実投稿スタブ
# ────────────────────────────────────────────────────────────────────
def build_caption(article: dict, source: dict) -> str:
    tags = " ".join(article.get("hashtags", []))
    src_name = source.get("source", "")
    lines = [article["headline"], "", article.get("body", ""), ""]
    if src_name and src_name not in ("manual", "actorsbookmag original"):
        lines.append(f"— via {src_name}")
        if source.get("url"):
            lines.append(source["url"])
        lines.append("")
    lines.append(tags)
    return "\n".join(lines)


def publish(article: dict, source: dict, thumb: Path, out_dir: Path) -> dict:
    caption = build_caption(article, source)
    (out_dir / "caption.txt").write_text(caption, encoding="utf-8")
    payload = {
        "image_path": str(thumb),
        "caption": caption,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / "payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return payload


def post_to_instagram(payload: dict) -> None:
    """⑤ 実投稿の差し込み口。投稿方式が未定のため未実装。

    方式が決まったらここを実装する:
      - 公式 Graph API: IG Business Account + FB Page 連携 → media コンテナ作成 → publish
      - 半自動: payload をフォルダ/DMに出すだけ（現状の publish() で既に達成済み）
    """
    raise NotImplementedError(
        "Instagram投稿方式が未定です。output/ のpayloadを使って手動投稿するか、"
        "方式決定後にこの関数を実装してください。"
    )


# ────────────────────────────────────────────────────────────────────
# オーケストレーション
# ────────────────────────────────────────────────────────────────────
IDEAS_CACHE = ROOT / "output" / "ideas.json"


def cmd_ideas(limit: int) -> None:
    """① ネタ出し: 候補を集めて番号付きリストで提示（記事生成なし・課金なし）。"""
    print("① ネタ集め: 日本のインディペンデント映画フィードを走査中…\n")
    candidates = collect_candidates()
    if not candidates:
        raise SystemExit("候補が0件でした（フィード側の一時的な問題かもしれません）。")
    top = candidates[:limit]
    IDEAS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    IDEAS_CACHE.write_text(json.dumps(top, ensure_ascii=False, indent=2), encoding="utf-8")
    for i, c in enumerate(top, 1):
        print(f"[{i}] {c['source']}｜スコア{c['arthouse_score']}")
        print(f"    {c['title']}")
    print(f"\n→ 番号を選んで: ./.venv/bin/python scripts/pipeline.py make --pick 番号 --direction \"ひとこと指示\"")


def cmd_make(pick: int, direction: str, url: str | None, auto_post: bool,
             topic: str | None = None, brief: str | None = None) -> None:
    """②〜⑤: 選ばれたネタ＋ディレクションで記事・サムネ・投稿ペイロードを生成。"""
    if topic:
        # シーンのお題から直接オリジナル記事を書く（RSSに依存しない）
        source = {"source": "actorsbookmag original", "title": topic,
                  "url": "", "summary": "", "body": brief or "", "image": ""}
        print(f"お題(オリジナル): {topic}")
    elif url:
        raw = fetch_article_source(url)
        source = {"source": "manual", "title": raw["title"], "url": url,
                  "summary": "", "body": raw["body"], "image": raw["image"]}
    else:
        if not IDEAS_CACHE.exists():
            raise SystemExit("先に `ideas` でネタを出してください。")
        ideas = json.loads(IDEAS_CACHE.read_text(encoding="utf-8"))
        if not (1 <= pick <= len(ideas)):
            raise SystemExit(f"--pick は 1〜{len(ideas)} で指定してください。")
        chosen = ideas[pick - 1]
        print(f"選択: [{pick}] {chosen['source']}｜{chosen['title']}")
        detail = {}
        try:
            detail = fetch_article_source(chosen["url"])
        except Exception as e:  # noqa: BLE001
            print(f"  ! 本文取得失敗、RSS要約で継続 ({e})", file=sys.stderr)
        source = {**chosen, "body": detail.get("body", ""), "image": detail.get("image", "")}

    if direction:
        print(f"ディレクション: {direction}")
    print(f"\n② 記事化: Claude ({ARTICLE_MODEL}) で生成中…")
    article = generate_article(source, direction)
    print(f"   見出し: {article['headline']}")

    out_dir = OUT_DIR / slugify(article["headline"])
    print("④ サムネ生成…")
    thumb = make_thumbnail(article, source, out_dir)
    print("③ サイト掲載: data/news.json に追記…")
    record = save_to_site(article, source, str(thumb))
    print("⑤ 投稿ペイロード出力…")
    payload = publish(article, source, thumb, out_dir)
    if auto_post:
        post_to_instagram(payload)
    print(f"\n✅ 完了:  {out_dir}")
    print(f"   サムネ: thumbnail.jpg / キャプション: caption.txt / id: {record['id']}")


def main() -> None:
    ap = argparse.ArgumentParser(description="actorsbookmag 映画ニュース自動パイプライン")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("ideas", help="ネタを集めて番号付きで提示")
    pi.add_argument("--limit", type=int, default=10, help="提示する件数")

    pm = sub.add_parser("make", help="選んだネタ＋指示で記事・サムネを生成")
    pm.add_argument("--pick", type=int, default=1, help="ideasの番号")
    pm.add_argument("--direction", default="", help="ひとこと編集指示")
    pm.add_argument("--url", help="ネタ番号ではなく特定URLを直接指定")
    pm.add_argument("--topic", help="シーンのお題から直接オリジナル記事を書く")
    pm.add_argument("--brief", help="--topic に渡す確認済みの事実メモ（本文の素材）")
    pm.add_argument("--auto-post", action="store_true", help="⑤実投稿（未実装スタブ）")

    args = ap.parse_args()
    if args.cmd == "ideas":
        cmd_ideas(args.limit)
    else:
        cmd_make(args.pick, args.direction, args.url, args.auto_post,
                 topic=args.topic, brief=args.brief)


if __name__ == "__main__":
    main()
