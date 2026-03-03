#!/usr/bin/env python3
"""Fetch new film listings from MUBI and U-NEXT daily.
Auto-generates Japanese cinephile comments via Claude API.

Sources:
  MUBI   : https://mubi.com/en/jp/showing  (HTML scrape + API fallback)
  U-NEXT : https://video.unext.jp/list/new?content_type=movie
           → fallback: https://video.unext.jp/genre/movie

Saves merged results to data/films.json.

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

# ── Full browser headers to avoid bot detection ───────────────────────────────
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

LETTERBOXD_MUBI_LIST  = "https://letterboxd.com/mubi/list/mubi-releases/"
LETTERBOXD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}
UNEXT_NEW_URL    = "https://video.unext.jp/list/new?content_type=movie"
UNEXT_GENRE_URL  = "https://video.unext.jp/genre/movie"


# ─── Claude: comment generation & scoring ─────────────────────────────────────

def score_film(film: dict) -> float:
    """
    Score a film for relevance to the Japanese cinephile audience.

    Scoring criteria:
      +3.0  Japanese origin (製作国が日本)
      +1.5  U-NEXT source  (日本語字幕が保証されている)
      +0.5  MUBI source    (日本向け上映コンテキスト)
      +1.5  Very recent    (2023年以降)
      +0.5  Recent         (2020年以降)
      +0.5  Festival mention in synopsis
      +0.3  Director info present
    """
    score = 0.0

    country  = (film.get("country") or "").lower()
    source   = film.get("source", "")
    year     = film.get("year") or 0
    synopsis = (film.get("synopsis") or "").lower()

    if any(k in country for k in ("japan", "日本", "japanese")):
        score += 3.0

    if source == "U-NEXT":
        score += 1.5
    elif source == "MUBI":
        score += 0.5

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


# ─── U-NEXT ───────────────────────────────────────────────────────────────────

def fetch_unext_films() -> list[dict]:
    """
    Scrape new movies from U-NEXT.
    Primary : https://video.unext.jp/list/new?content_type=movie
    Fallback: https://video.unext.jp/genre/movie
    """
    unext_headers = {**BROWSER_HEADERS, "Referer": "https://video.unext.jp/", "Sec-Fetch-Site": "same-origin"}
    for url in [UNEXT_NEW_URL, UNEXT_GENRE_URL]:
        films = _scrape_unext_page(url, unext_headers)
        if films:
            print(f"  [U-NEXT] {len(films)} films from {url}")
            return films
        print(f"  [U-NEXT] 0 films from {url}, trying next...", file=sys.stderr)
    return []


def _scrape_unext_page(url: str, headers: dict) -> list[dict]:
    """Fetch a U-NEXT page and extract film records."""
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 404:
            print(f"  [U-NEXT] 404: {url}", file=sys.stderr)
            return []
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [U-NEXT] Fetch error ({url}): {e}", file=sys.stderr)
        return []

    soup  = BeautifulSoup(resp.text, "html.parser")
    films: list[dict] = []

    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag:
        try:
            data  = json.loads(tag.string or "{}")
            items = (
                _deep_find(data, "titleList")
                or _deep_find(data, "titleListDtos")
                or _deep_find(data, "contentList")
                or _deep_find(data, "items")
                or []
            )
            if isinstance(items, list) and items:
                for item in items[:60]:
                    if isinstance(item, dict):
                        f = _parse_unext_item(item)
                        if f:
                            films.append(f)
                if films:
                    return films
        except (json.JSONDecodeError, Exception) as e:
            print(f"  [U-NEXT] __NEXT_DATA__ parse error: {e}", file=sys.stderr)

    return _scrape_unext_html(soup)


def _scrape_unext_html(soup: BeautifulSoup) -> list[dict]:
    films: list[dict] = []
    seen:  set[str]   = set()
    for sel in ["a[href*='/title/SID']", "a[href*='/title/']", "[class*='titleCard'] a[href]", "[class*='TitleCard'] a[href]"]:
        for el in soup.select(sel):
            href = el.get("href", "") if el.name == "a" else ""
            if not href:
                a    = el.select_one("a[href*='/title/']")
                href = a.get("href", "") if a else ""
            if not href or "/title/" not in href or href in seen:
                continue
            seen.add(href)
            img      = el.select_one("img") if el.name != "img" else el
            title_el = el.select_one("p, span, h2, h3, [class*='title']")
            title    = title_el.get_text(strip=True) if title_el else (img.get("alt", "") if img else "")
            if not title:
                continue
            films.append({
                "id":         f"unext_{abs(hash(href))}",
                "title":      title,
                "thumbnail":  (img.get("src") or img.get("data-src", "")) if img else "",
                "url":        _abs("https://video.unext.jp", href),
                "source":     "U-NEXT",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })
        if films:
            break
    return films


def _parse_unext_item(item: dict) -> dict | None:
    title = item.get("title") or item.get("name") or item.get("display_name", "")
    if not title:
        return None
    code      = item.get("title_code") or item.get("code") or item.get("id", "")
    thumb_raw = item.get("thumbnail_url") or item.get("image_url") or item.get("thumbnail", "")
    thumb     = thumb_raw if isinstance(thumb_raw, str) else (thumb_raw.get("url", "") if isinstance(thumb_raw, dict) else "")
    return {
        "id":         f"unext_{code}",
        "title":      title,
        "year":       item.get("year") or item.get("release_year"),
        "director":   item.get("director", ""),
        "synopsis":   item.get("synopsis") or item.get("comment", ""),
        "thumbnail":  thumb,
        "url":        f"https://video.unext.jp/title/{code}" if code else UNEXT_NEW_URL,
        "source":     "U-NEXT",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─── Utilities ────────────────────────────────────────────────────────────────

def _abs(base: str, href: str) -> str:
    if not href:
        return base
    return (base + href) if href.startswith("/") else href


def _deep_find(obj, key: str, _depth: int = 0):
    if _depth > 10:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and obj[key]:
            return obj[key]
        for v in obj.values():
            r = _deep_find(v, key, _depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find(item, key, _depth + 1)
            if r is not None:
                return r
    return None


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

    time.sleep(2)

    print(f"\nFetching U-NEXT new movies from {UNEXT_NEW_URL} ...")
    unext = fetch_unext_films()
    print(f"  → {len(unext)} films total from U-NEXT")

    # ── Merge ─────────────────────────────────────────────────────────────────
    fresh: list[dict] = mubi + unext
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

    # ── Generate comments via Gemini ──────────────────────────────────────────
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
