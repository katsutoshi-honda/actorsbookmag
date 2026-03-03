#!/usr/bin/env python3
"""Fetch new film listings from MUBI daily.
Auto-generates Japanese cinephile comments via Claude API.

Sources:
  MUBI : https://letterboxd.com/mubi/list/mubi-releases/

Saves results to data/films.json.

Usage:
    pip install requests beautifulsoup4 lxml anthropic
    ANTHROPIC_API_KEY=sk-... python scripts/fetch_films.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = Path("data/films.json")
MAX_FILMS   = 300
CLAUDE_MODEL = "claude-haiku-4-5-20251001"  # ~$0.02 per 100 films

LETTERBOXD_MUBI_LIST  = "https://letterboxd.com/mubi/list/mubi-releases/"
LETTERBOXD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


# ─── Claude: comment generation & scoring ─────────────────────────────────────

def score_film(film: dict) -> float:
    """Score a film for relevance to the Japanese cinephile audience."""
    score = 0.0

    country  = (film.get("country") or "").lower()
    year     = film.get("year") or 0
    synopsis = (film.get("synopsis") or "").lower()

    if any(k in country for k in ("japan", "日本", "japanese")):
        score += 3.0

    if year >= 2023:
        score += 1.5
    elif year >= 2020:
        score += 0.5

    festival_keywords = [
        "cannes", "berlin", "venice", "sundance", "toronto", "rotterdam",
        "カンヌ", "ベルリン", "ヴェネチア", "サンダンス", "トロント",
        "award", "prize", "受賞", "グランプリ", "パルム",
    ]
    if any(kw in synopsis for kw in festival_keywords):
        score += 0.5

    if film.get("director"):
        score += 0.3

    return round(score, 1)


def generate_comment(film: dict, client: anthropic.Anthropic) -> str:
    """Generate a Japanese cinephile comment for a film using Claude API."""
    lines = []
    if film.get("title"):
        lines.append(f"タイトル: {film['title']}")
    loc = film.get("title_locale", "")
    if loc and loc != film.get("title"):
        lines.append(f"原題: {loc}")
    if film.get("year"):
        lines.append(f"製作年: {film['year']}")
    if film.get("director"):
        lines.append(f"監督: {film['director']}")
    if film.get("country"):
        lines.append(f"製作国: {film['country']}")
    if film.get("synopsis"):
        lines.append(f"作品概要: {film['synopsis']}")

    film_info = "\n".join(lines)

    prompt = f"""映画批評家として、以下の映画についてシネフィル向けの批評的コメントを日本語で2〜3文で書いてください。

要件:
- 断定的・批評的なトーンを維持する
- 映画祭での受賞歴や評価に触れる（情報がある場合）
- 監督の作家性・スタイル・文化的背景に言及する
- 「この映画は〜」などの平凡な書き出しを避け、作品の核心を突く書き出しにする
- コメントのみを出力し、前置きや説明は一切不要

{film_info}

コメント:"""

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=250,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


# ─── MUBI（Letterboxd経由）────────────────────────────────────────────────────

def fetch_mubi_films() -> list[dict]:
    """Scrape MUBI releases from https://letterboxd.com/mubi/list/mubi-releases/
    Letterboxd はサーバーサイドレンダリングなので全件取得できる（最大3ページ）。
    """
    films: list[dict] = []

    for page in range(1, 4):
        url = (
            LETTERBOXD_MUBI_LIST
            if page == 1
            else f"{LETTERBOXD_MUBI_LIST}page/{page}/"
        )
        try:
            resp = requests.get(url, headers=LETTERBOXD_HEADERS, timeout=15)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [MUBI] Page {page} fetch error: {e}", file=sys.stderr)
            break

        soup       = BeautifulSoup(resp.text, "html.parser")
        page_films = _parse_letterboxd_page(soup)

        if not page_films:
            print(f"  [MUBI] Page {page}: 0 films — stopping", file=sys.stderr)
            break

        films.extend(page_films)
        print(f"  [MUBI] Page {page}: {len(page_films)} films")

        # 次ページリンクがなければ終了
        if not soup.select_one("a.next"):
            break

        time.sleep(1)

    return films


def _parse_letterboxd_page(soup: BeautifulSoup) -> list[dict]:
    """Letterboxdリストページから映画データを抽出する。

    実際のHTML構造（調査済み）:
      <div class="react-component"
           data-item-slug="film-slug"
           data-item-name="Film Title (2025)"
           data-item-link="/film/film-slug/"
           data-film-id="123456">
    """
    films: list[dict] = []
    seen:  set[str]   = set()

    for div in soup.select("div[data-item-slug][data-item-name]"):
        slug      = div.get("data-item-slug", "")
        full_name = div.get("data-item-name", "")  # "Film Title (2025)" の形式
        href      = div.get("data-item-link", f"/film/{slug}/")

        if not slug or slug in seen:
            continue
        seen.add(slug)

        # タイトルと製作年を分離: "Film Title (2025)" → title="Film Title", year=2025
        year  = None
        title = full_name
        if full_name.endswith(")") and "(" in full_name:
            body, _, year_part = full_name.rpartition("(")
            year_str = year_part.rstrip(")")
            if year_str.isdigit():
                title = body.strip()
                year  = int(year_str)

        films.append(_make_mubi_record(
            slug,
            title,
            year,
            "",  # サムネイルはLetterboxdでは空（プレースホルダーのみ）
            _abs("https://letterboxd.com", href),
        ))

    return films


def _make_mubi_record(film_id, title: str, year, thumbnail: str, url: str) -> dict:
    return {
        "id":         f"mubi_{film_id}",
        "title":      title,
        "year":       year,
        "thumbnail":  thumbnail,
        "url":        url,
        "source":     "MUBI",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _letterboxd_thumb(img) -> str:
    """プレースホルダー画像を除外してサムネイルURLを返す。"""
    if not img:
        return ""
    src = img.get("src") or img.get("data-src", "")
    return "" if ("empty-poster" in src or not src) else src


# ─── Utilities ────────────────────────────────────────────────────────────────

def _abs(base: str, href: str) -> str:
    if not href:
        return base
    return (base + href) if href.startswith("/") else href


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # ── Claude client ──────────────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        claude_client = anthropic.Anthropic(api_key=api_key)
        print(f"Claude API ready (model: {CLAUDE_MODEL})")
    else:
        claude_client = None
        print("Warning: ANTHROPIC_API_KEY not set — comment generation skipped", file=sys.stderr)

    # ── Load existing data (to preserve comments) ─────────────────────────────
    existing: list[dict] = []
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    # Index existing films that already have a comment → skip re-generation
    existing_by_id: dict[str, dict] = {f["id"]: f for f in existing if f.get("id")}

    # ── Scrape ────────────────────────────────────────────────────────────────
    print(f"\nFetching MUBI releases from {LETTERBOXD_MUBI_LIST} ...")
    mubi = fetch_mubi_films()
    print(f"  → {len(mubi)} films total from MUBI")

    # ── Merge ─────────────────────────────────────────────────────────────────
    fresh: list[dict] = mubi
    seen_urls: set[str] = {f["url"] for f in fresh}
    for film in existing:
        url = film.get("url", "")
        if url and url not in seen_urls:
            fresh.append(film)
            seen_urls.add(url)

    # Restore existing comments & scores so they aren't lost on re-scrape
    for film in fresh:
        prev = existing_by_id.get(film["id"])
        if prev:
            if prev.get("comment") and not film.get("comment"):
                film["comment"] = prev["comment"]
            if prev.get("score") is not None and film.get("score") is None:
                film["score"] = prev["score"]

    # ── Score ─────────────────────────────────────────────────────────────────
    for film in fresh:
        film["score"] = score_film(film)

    fresh.sort(key=lambda f: f.get("fetched_at", ""), reverse=True)
    fresh = fresh[:MAX_FILMS]

    # ── Generate comments via Claude ──────────────────────────────────────────
    if claude_client:
        needs_comment = [f for f in fresh if not f.get("comment")]
        already_done  = len(fresh) - len(needs_comment)
        print(f"\nComment generation: {len(needs_comment)} new, {already_done} already have comments (skipped)")

        for i, film in enumerate(needs_comment, 1):
            try:
                film["comment"] = generate_comment(film, claude_client)
                print(f"  [{i}/{len(needs_comment)}] {film['title']}")
            except Exception as e:
                print(f"  [{i}/{len(needs_comment)}] {film['title']}: {e}", file=sys.stderr)
                film["comment"] = ""
            time.sleep(0.5)  # Rate-limit: ~2 req/s

    # ── Save ──────────────────────────────────────────────────────────────────
    OUTPUT_PATH.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(fresh)} films → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
