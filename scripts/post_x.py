#!/usr/bin/env python3
"""
actorsbookmag — X(Twitter)自動投稿

data/news.json のうち「公開(status=published)かつ未投稿(posted_x!=true)」の記事を
Xに投稿する。投稿文は自動生成。サムネ画像も一緒に添付。

使い方:
    # 投稿せず、生成される投稿文だけ確認（APIキー不要・課金なし）
    ./.venv/bin/python scripts/post_x.py --dry-run

    # 実投稿（要: 環境変数のXキー4つ）
    ./.venv/bin/python scripts/post_x.py

必要な環境変数（実投稿時のみ）:
    X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NEWS_JSON = ROOT / "data" / "news.json"
SITE = os.environ.get("SITE_BASE", "https://actorsbookmag.vercel.app")
TCO_LEN = 23           # Xは全URLをt.co(23字)に短縮して数える
X_LIMIT = 280


def article_url(rec: dict) -> str:
    return f"{SITE}/news/article.html?id={rec['id']}"


def _len(text: str, url: str) -> int:
    """X上の実効文字数（URLは23字換算）。"""
    return len(text.replace(url, "x" * TCO_LEN))


def build_x_text(rec: dict) -> str:
    """記事から X 用の投稿文を自動生成（280字以内・URLは23字換算）。"""
    url = article_url(rec)
    tags = " ".join((rec.get("hashtags") or [])[:3])
    head = (rec.get("headline") or "").strip()
    dek = (rec.get("dek") or "").strip()

    def compose(with_dek: bool, with_tags: bool) -> str:
        parts = [head]
        if with_dek and dek:
            parts.append(dek)
        parts.append(f"▶ {url}")
        if with_tags and tags:
            parts.append(tags)
        return "\n\n".join(parts[:-1]) + ("\n" + parts[-1] if len(parts) > 1 else "")

    # 収まるまで段階的に要素を落とす
    for wd, wt in ((True, True), (True, False), (False, True), (False, False)):
        t = compose(wd, wt)
        if _len(t, url) <= X_LIMIT:
            return t
    # それでも長い場合は見出しを詰める
    room = X_LIMIT - TCO_LEN - 4
    return f"{head[:room]}…\n▶ {url}"


def post_to_x(text: str, image_path: Path | None) -> str:
    """tweepyでXに投稿。投稿URLを返す。"""
    import tweepy
    ck = os.environ["X_API_KEY"]; cs = os.environ["X_API_SECRET"]
    at = os.environ["X_ACCESS_TOKEN"]; ats = os.environ["X_ACCESS_SECRET"]
    client = tweepy.Client(consumer_key=ck, consumer_secret=cs,
                           access_token=at, access_token_secret=ats)
    media_ids = None
    if image_path and image_path.exists():
        try:
            api = tweepy.API(tweepy.OAuth1UserHandler(ck, cs, at, ats))
            media = api.media_upload(str(image_path))   # 画像アップロードは v1.1
            media_ids = [media.media_id]
        except Exception as e:  # noqa: BLE001  無料枠等で画像不可でもテキストで投稿
            print(f"  ! 画像添付をスキップ（テキストのみ投稿）: {e}", file=sys.stderr)
    resp = client.create_tweet(text=text, media_ids=media_ids)  # 投稿は v2
    tid = resp.data["id"]
    return f"https://x.com/i/web/status/{tid}"


def local_image(rec: dict) -> Path | None:
    """記事のサムネ(公開画像)のローカルパス。background優先。"""
    for key in ("background", "thumbnail"):
        v = rec.get(key) or ""
        if v.startswith("/"):
            p = ROOT / v.lstrip("/")
            if p.exists():
                return p
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="actorsbookmag X自動投稿")
    ap.add_argument("--dry-run", action="store_true", help="投稿せず生成文だけ表示")
    args = ap.parse_args()

    news = json.loads(NEWS_JSON.read_text(encoding="utf-8"))
    targets = [r for r in news if r.get("status") == "published" and not r.get("posted_x")]

    if not targets:
        print("投稿対象なし（公開済み・未投稿の記事がありません）。")
        return

    X_KEYS = ("X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_SECRET")
    if not args.dry_run and not all(os.environ.get(k) for k in X_KEYS):
        print(f"Xの認証キーが未設定のため投稿をスキップしました（対象 {len(targets)}件）。"
              "キー4つを設定すると、以降の公開分から自動投稿されます。")
        return

    print(f"投稿対象: {len(targets)}件\n")
    posted = 0
    for rec in targets:
        text = build_x_text(rec)
        img = local_image(rec)
        print("─" * 48)
        print(text)
        print(f"[画像: {img.name if img else 'なし'} / {_len(text, article_url(rec))}字]")
        if args.dry_run:
            continue
        try:
            url = post_to_x(text, img)
            rec["posted_x"] = True
            rec["x_url"] = url
            posted += 1
            print(f"→ 投稿成功: {url}")
        except KeyError as e:
            raise SystemExit(f"環境変数 {e} が未設定です。Xキー4つを設定してください。")
        except Exception as e:  # noqa: BLE001
            print(f"→ 投稿失敗: {e}", file=sys.stderr)

    if not args.dry_run and posted:
        NEWS_JSON.write_text(json.dumps(news, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n✅ {posted}件を投稿し、news.jsonに記録しました。")


if __name__ == "__main__":
    main()
