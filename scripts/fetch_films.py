#!/usr/bin/env python3
"""Fetch new film listings from MUBI and U-NEXT daily.
Auto-generates Japanese cinephile comments via Gemini API.

Sources:
  MUBI   : https://mubi.com/en/jp/showing  (HTML scrape + API fallback)
  U-NEXT : https://video.unext.jp/list/new?content_type=movie
           → fallback: https://video.unext.jp/genre/movie

Saves merged results to data/films.json.

Usage:
    pip install requests beautifulsoup4 lxml google-generativeai
    GEMINI_API_KEY=AIza... python scripts/fetch_films.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import google.generativeai as genai
import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = Path("data/films.json")
MAX_FILMS   = 300
GEMINI_MODEL = "gemini-1.5-flash"  # Free tier available

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

MUBI_SHOWING_URL = "https://mubi.com/en/jp/showing"
UNEXT_NEW_URL    = "https://video.unext.jp/list/new?content_type=movie"
UNEXT_GENRE_URL  = "https://video.unext.jp/genre/movie"


# ─── Gemini: comment generation & scoring ─────────────────────────────────────

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


def generate_comment(film: dict, model: genai.GenerativeModel) -> str:
    """Generate a Japanese cinephile comment for a film using Gemini API."""
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

    resp = model.generate_content(prompt)
    return resp.text.strip()


# ─── MUBI ─────────────────────────────────────────────────────────────────────

def fetch_mubi_films() -> list[dict]:
    """Scrape MUBI Now Showing from https://mubi.com/en/jp/showing."""
    films: list[dict] = []

    try:
        resp = requests.get(MUBI_SHOWING_URL, headers=BROWSER_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [MUBI] Fetch error: {e}", file=sys.stderr)
        return _fetch_mubi_api_fallback()

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1. __NEXT_DATA__: extract every film-like object in the JSON tree
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag:
        try:
            data = json.loads(tag.string or "{}")

            # a) marqueeFilm (always present in SSR)
            marquee = (
                data.get("props", {})
                    .get("pageProps", {})
                    .get("marqueeFilm")
            )
            if marquee and isinstance(marquee, dict):
                f = _parse_mubi_object(marquee)
                if f:
                    films.append(f)

            # b) Deep-search entire JSON for objects that look like MUBI films
            found = _collect_mubi_film_objects(data)
            seen_ids = {f["id"] for f in films}
            for obj in found:
                parsed = _parse_mubi_object(obj)
                if parsed and parsed["id"] not in seen_ids:
                    films.append(parsed)
                    seen_ids.add(parsed["id"])

        except (json.JSONDecodeError, Exception) as e:
            print(f"  [MUBI] __NEXT_DATA__ parse error: {e}", file=sys.stderr)

    if films:
        print(f"  [MUBI] {len(films)} film(s) from __NEXT_DATA__")

    # 2. HTML card scraping (in case MUBI adds SSR film cards in future)
    html_films = _scrape_mubi_html_cards(soup)
    seen_ids = {f["id"] for f in films}
    for f in html_films:
        if f["id"] not in seen_ids:
            films.append(f)
            seen_ids.add(f["id"])

    if not films:
        print("  [MUBI] 0 films from page — trying API fallback...", file=sys.stderr)
        films = _fetch_mubi_api_fallback()

    return films


def _collect_mubi_film_objects(obj, _depth: int = 0) -> list[dict]:
    """Recursively collect dict objects that look like MUBI film records."""
    results: list[dict] = []
    if _depth > 12:
        return results
    if isinstance(obj, dict):
        if (
            obj.get("title")
            and (obj.get("year") or obj.get("still_url") or obj.get("web_url"))
        ):
            results.append(obj)
        for v in obj.values():
            results.extend(_collect_mubi_film_objects(v, _depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_mubi_film_objects(item, _depth + 1))
    return results


def _parse_mubi_object(f: dict) -> dict | None:
    """Normalise a MUBI film dict into our unified schema."""
    title = f.get("title") or f.get("original_title", "")
    if not title:
        return None

    film_id = f.get("id") or f.get("slug") or abs(hash(title))
    slug    = f.get("slug") or f.get("film_slug", "")
    web_url = (
        f.get("web_url")
        or f.get("canonical_url")
        or (f"https://mubi.com/films/{slug}" if slug else MUBI_SHOWING_URL)
    )

    thumb = f.get("still_url", "")
    if not thumb:
        stills = f.get("stills") or f.get("still") or {}
        if isinstance(stills, dict):
            thumb = stills.get("standard") or stills.get("url") or stills.get("medium", "")
        elif isinstance(stills, list) and stills:
            thumb = (stills[0] or {}).get("url", "") if isinstance(stills[0], dict) else ""

    directors = f.get("directors") or []
    director  = directors[0].get("name", "") if directors and isinstance(directors[0], dict) else ""
    countries = f.get("countries") or []
    country   = countries[0].get("name", "") if countries and isinstance(countries[0], dict) else ""

    return {
        "id":           f"mubi_{film_id}",
        "title":        title,
        "title_locale": f.get("title_locale") or f.get("original_title", ""),
        "year":         f.get("year"),
        "director":     director,
        "country":      country,
        "duration":     f.get("duration"),
        "synopsis":     f.get("short_synopsis") or f.get("synopsis", ""),
        "thumbnail":    thumb,
        "url":          web_url,
        "source":       "MUBI",
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
    }


def _scrape_mubi_html_cards(soup: BeautifulSoup) -> list[dict]:
    """Try to extract film cards directly from rendered MUBI HTML."""
    films: list[dict] = []
    for sel in ["[data-film-id]", "[class*='FilmCard']", "[class*='film-card']", "article[class*='film']"]:
        cards = soup.select(sel)
        if not cards:
            continue
        for card in cards:
            title_el = card.select_one("h2, h3, [class*='title'], [class*='Title']")
            img_el   = card.select_one("img[src], img[data-src]")
            link_el  = card.select_one("a[href]")
            if not title_el:
                continue
            href     = link_el["href"] if link_el else ""
            full_url = _abs("https://mubi.com", href)
            thumb    = (img_el.get("src") or img_el.get("data-src", "")) if img_el else ""
            films.append({
                "id":         f"mubi_{abs(hash(full_url))}",
                "title":      title_el.get_text(strip=True),
                "thumbnail":  thumb,
                "url":        full_url,
                "source":     "MUBI",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })
        if films:
            break
    return films


def _fetch_mubi_api_fallback() -> list[dict]:
    """Try MUBI's public API (no auth) as last resort."""
    films: list[dict] = []
    api_headers = {**BROWSER_HEADERS, "Accept": "application/json", "Client-Country": "JP", "Client-Version": "4.0.0"}
    for url in [
        "https://api.mubi.com/v3/films?filter[now_showing]=true&country_code=JP&per_page=30",
        "https://api.mubi.com/v3/films?filter[new_arrivals]=true&country_code=JP&per_page=20",
    ]:
        try:
            resp = requests.get(url, headers=api_headers, timeout=15)
            if resp.status_code in (401, 403):
                continue
            resp.raise_for_status()
            data      = resp.json()
            film_list = data.get("films") or data.get("data") or []
            if film_list:
                for f in film_list:
                    parsed = _parse_mubi_object(f)
                    if parsed:
                        films.append(parsed)
                print(f"  [MUBI API] {len(films)} films from {url}")
                return films
        except Exception as e:
            print(f"  [MUBI API] {url}: {e}", file=sys.stderr)
    return films


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

    # ── Gemini client ──────────────────────────────────────────────────────────
    api_key = os.environ.get("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        gemini_model = genai.GenerativeModel(GEMINI_MODEL)
        print(f"Gemini API ready (model: {GEMINI_MODEL})")
    else:
        gemini_model = None
        print("Warning: GEMINI_API_KEY not set — comment generation skipped", file=sys.stderr)

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
    print(f"\nFetching MUBI now-showing from {MUBI_SHOWING_URL} ...")
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
    if gemini_model:
        needs_comment = [f for f in fresh if not f.get("comment")]
        already_done  = len(fresh) - len(needs_comment)
        print(f"\nComment generation: {len(needs_comment)} new, {already_done} already have comments (skipped)")

        for i, film in enumerate(needs_comment, 1):
            try:
                film["comment"] = generate_comment(film, gemini_model)
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
