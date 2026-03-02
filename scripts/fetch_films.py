#!/usr/bin/env python3
"""Fetch new film listings from MUBI and U-NEXT daily.

Saves merged results to data/films.json.

Usage:
    pip install requests beautifulsoup4 lxml
    python scripts/fetch_films.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = Path("data/films.json")
MAX_FILMS = 300

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
COMMON_HEADERS = {"User-Agent": UA}


# ─── MUBI ─────────────────────────────────────────────────────────────────────

def fetch_mubi_films() -> list[dict]:
    """Fetch new-arrival films from MUBI Japan."""
    films: list[dict] = []

    api_headers = {
        **COMMON_HEADERS,
        "Accept": "application/json",
        "Client-Country": "JP",
        "Client-Version": "4.0.0",
        "Client-Device": "web",
    }

    filter_sets = [
        {"filter[new_arrivals]": "true", "per_page": 20, "page": 1},
        {"filter[new_releases]": "true", "per_page": 20, "page": 1},
        {"filter[now_showing]": "true", "per_page": 20, "page": 1},
    ]

    for params in filter_sets:
        try:
            resp = requests.get(
                "https://api.mubi.com/v3/films",
                headers=api_headers,
                params=params,
                timeout=15,
            )
            if resp.status_code in (401, 403):
                print(f"  [MUBI] {resp.status_code} for {params}, skipping", file=sys.stderr)
                continue
            resp.raise_for_status()
            data = resp.json()
            film_list = data.get("films") or data.get("data") or []
            if film_list:
                for f in film_list:
                    films.append(_parse_mubi_film(f))
                print(f"  [MUBI] Got {len(films)} films via API params={params}")
                return films
        except requests.RequestException as e:
            print(f"  [MUBI] Request error ({params}): {e}", file=sys.stderr)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  [MUBI] Parse error ({params}): {e}", file=sys.stderr)

    # Fallback: scrape MUBI website (Next.js embeds data in __NEXT_DATA__)
    print("  [MUBI] Falling back to website scrape...")
    films = _scrape_mubi_website()
    return films


def _parse_mubi_film(f: dict) -> dict:
    directors = f.get("directors") or []
    dir_name = directors[0].get("name", "") if directors else ""
    countries = f.get("countries") or []
    country = countries[0].get("name", "") if countries else ""
    still = f.get("still") or {}
    if isinstance(still, list):
        still = still[0] if still else {}
    thumb = still.get("url") or still.get("medium") or ""
    slug = f.get("slug") or f.get("id", "")
    return {
        "id": f"mubi_{f.get('id', slug)}",
        "title": f.get("title", ""),
        "title_locale": f.get("title_locale") or f.get("original_title", ""),
        "year": f.get("year"),
        "director": dir_name,
        "country": country,
        "duration": f.get("duration"),
        "synopsis": f.get("short_synopsis") or f.get("synopsis", ""),
        "thumbnail": thumb,
        "url": f"https://mubi.com/films/{slug}",
        "source": "MUBI",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _scrape_mubi_website() -> list[dict]:
    films: list[dict] = []
    try:
        resp = requests.get(
            "https://mubi.com/ja/films",
            headers=COMMON_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Next.js embeds page data in <script id="__NEXT_DATA__">
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if tag:
            data = json.loads(tag.string or "{}")
            film_list = _deep_find(data, "films")
            if isinstance(film_list, list):
                for f in film_list[:20]:
                    if isinstance(f, dict) and f.get("title"):
                        films.append(_parse_mubi_film(f))
                if films:
                    print(f"  [MUBI scrape] Got {len(films)} films from __NEXT_DATA__")
                    return films

        # HTML card fallback
        cards = soup.select("[data-filmid], [class*='FilmCell'], [class*='film-cell']")
        for card in cards[:20]:
            title_el = card.select_one("h2, h3, [class*='title']")
            img_el = card.select_one("img")
            link_el = card.select_one("a[href]")
            if title_el:
                href = link_el["href"] if link_el else ""
                full_url = ("https://mubi.com" + href) if href.startswith("/") else href
                films.append({
                    "id": f"mubi_{abs(hash(full_url))}",
                    "title": title_el.get_text(strip=True),
                    "thumbnail": (img_el.get("src") or img_el.get("data-src", "")) if img_el else "",
                    "url": full_url,
                    "source": "MUBI",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })
        print(f"  [MUBI scrape] Got {len(films)} films from HTML cards")
    except Exception as e:
        print(f"  [MUBI scrape] Error: {e}", file=sys.stderr)
    return films


# ─── U-NEXT ───────────────────────────────────────────────────────────────────

def fetch_unext_films() -> list[dict]:
    """Fetch new-arrival films from U-NEXT."""
    films = _fetch_unext_via_api()
    if not films:
        print("  [U-NEXT] API returned nothing, trying website scrape...")
        films = _scrape_unext_website()
    return films


def _fetch_unext_via_api() -> list[dict]:
    """Try U-NEXT internal API endpoints."""
    films: list[dict] = []
    headers = {
        **COMMON_HEADERS,
        "Accept": "application/json",
        "Origin": "https://video.unext.jp",
        "Referer": "https://video.unext.jp/",
    }
    # Candidate endpoints (U-NEXT internal REST API, observed from network traffic)
    endpoints = [
        "https://api.unext.jp/api/1/content/list?sort=new&content_type=movie&page_size=20&page_num=1",
        "https://api.unext.jp/api/1/search/list?sort=newest&content_type=movie&page_size=20",
        "https://api.unext.jp/api/2/content/new?content_type=movie&page_size=20",
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code in (401, 403, 404):
                continue
            resp.raise_for_status()
            data = resp.json()
            items = (
                data.get("result_list")
                or data.get("items")
                or data.get("contents")
                or data.get("titleList")
                or []
            )
            if items:
                for item in items[:20]:
                    films.append(_parse_unext_item(item))
                print(f"  [U-NEXT API] Got {len(films)} films from {url}")
                return films
        except Exception as e:
            print(f"  [U-NEXT API] {url}: {e}", file=sys.stderr)
    return films


def _scrape_unext_website() -> list[dict]:
    """Scrape U-NEXT new arrivals page."""
    films: list[dict] = []
    try:
        resp = requests.get(
            "https://video.unext.jp/category/new",
            headers=COMMON_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Next.js __NEXT_DATA__ approach
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if tag:
            data = json.loads(tag.string or "{}")
            items = (
                _deep_find(data, "titleList")
                or _deep_find(data, "contentList")
                or _deep_find(data, "items")
                or []
            )
            if isinstance(items, list):
                for item in items[:20]:
                    if isinstance(item, dict):
                        films.append(_parse_unext_item(item))
                if films:
                    print(f"  [U-NEXT scrape] Got {len(films)} films from __NEXT_DATA__")
                    return films

        # HTML card fallback
        selectors = [
            "[data-testid='title-card']",
            ".c-title-card",
            "[class*='titleCard']",
            "[class*='TitleCard']",
            "[class*='title-item']",
        ]
        for sel in selectors:
            cards = soup.select(sel)
            if not cards:
                continue
            for card in cards[:20]:
                title_el = card.select_one("h3, h2, [class*='title'], [class*='Title']")
                img_el = card.select_one("img")
                link_el = card.select_one("a[href]")
                if title_el:
                    href = link_el["href"] if link_el else ""
                    full_url = ("https://video.unext.jp" + href) if href.startswith("/") else href
                    films.append({
                        "id": f"unext_{abs(hash(full_url))}",
                        "title": title_el.get_text(strip=True),
                        "thumbnail": (img_el.get("src") or img_el.get("data-src", "")) if img_el else "",
                        "url": full_url,
                        "source": "U-NEXT",
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                    })
            if films:
                break
        print(f"  [U-NEXT scrape] Got {len(films)} films from HTML")
    except Exception as e:
        print(f"  [U-NEXT scrape] Error: {e}", file=sys.stderr)
    return films


def _parse_unext_item(item: dict) -> dict:
    title = item.get("title") or item.get("name") or item.get("display_name", "")
    code = item.get("title_code") or item.get("code") or item.get("id", "")
    thumb_raw = item.get("thumbnail_url") or item.get("image_url") or item.get("thumbnail", "")
    thumb = thumb_raw if isinstance(thumb_raw, str) else (thumb_raw.get("url", "") if isinstance(thumb_raw, dict) else "")
    return {
        "id": f"unext_{code}",
        "title": title,
        "year": item.get("year") or item.get("release_year"),
        "director": item.get("director", ""),
        "synopsis": item.get("synopsis") or item.get("comment", ""),
        "thumbnail": thumb,
        "url": f"https://video.unext.jp/title/{code}" if code else "https://video.unext.jp/",
        "source": "U-NEXT",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─── Utilities ────────────────────────────────────────────────────────────────

def _deep_find(obj, key, _depth=0):
    """Recursively search a nested dict/list for a key whose value is a non-empty list."""
    if _depth > 10:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and len(obj[key]) > 0:
            return obj[key]
        for v in obj.values():
            result = _deep_find(v, key, _depth + 1)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find(item, key, _depth + 1)
            if result is not None:
                return result
    return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing: list[dict] = []
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    print("Fetching from MUBI...")
    mubi = fetch_mubi_films()
    print(f"  → {len(mubi)} films from MUBI")

    time.sleep(2)

    print("Fetching from U-NEXT...")
    unext = fetch_unext_films()
    print(f"  → {len(unext)} films from U-NEXT")

    # Merge: new results first, then existing (deduplicated by URL)
    fresh = mubi + unext
    seen: set[str] = {f["url"] for f in fresh}
    for film in existing:
        url = film.get("url", "")
        if url and url not in seen:
            fresh.append(film)
            seen.add(url)

    # Sort newest-fetched first, cap at MAX_FILMS
    fresh.sort(key=lambda f: f.get("fetched_at", ""), reverse=True)
    fresh = fresh[:MAX_FILMS]

    OUTPUT_PATH.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved {len(fresh)} films → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
