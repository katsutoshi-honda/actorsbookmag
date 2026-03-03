#!/usr/bin/env python3
"""Fetch new film listings from MUBI (via Letterboxd) and U-NEXT daily.

Sources:
  MUBI   : https://letterboxd.com/mubi/list/mubi-releases/
  U-NEXT : https://video.unext.jp/genre/foreign-movie

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
COMMON_HEADERS = {
    "User-Agent": UA,
    "Accept-Language": "ja,en;q=0.9",
}

LETTERBOXD_LIST = "https://letterboxd.com/mubi/list/mubi-releases/"
UNEXT_GENRE_URL = "https://video.unext.jp/genre/foreign-movie"


# ─── MUBI (via Letterboxd) ────────────────────────────────────────────────────

def fetch_mubi_films() -> list[dict]:
    """Scrape MUBI release list from Letterboxd (up to 3 pages)."""
    films: list[dict] = []

    for page in range(1, 4):
        url = LETTERBOXD_LIST if page == 1 else f"{LETTERBOXD_LIST}page/{page}/"
        try:
            resp = requests.get(url, headers=COMMON_HEADERS, timeout=15)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [MUBI/Letterboxd] Page {page} fetch error: {e}", file=sys.stderr)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_films = _parse_letterboxd_page(soup)

        if not page_films:
            print(f"  [MUBI/Letterboxd] No films found on page {page}", file=sys.stderr)
            break

        films.extend(page_films)
        print(f"  [MUBI/Letterboxd] Page {page}: {len(page_films)} films")

        # Stop if there is no "next page" link
        if not soup.select_one("a.next"):
            break

        time.sleep(1)

    return films


def _parse_letterboxd_page(soup: BeautifulSoup) -> list[dict]:
    """Extract film records from a single Letterboxd list page."""
    films: list[dict] = []

    # Primary: <li class="poster-container"> containing <div data-film-slug>
    items = soup.select("li.poster-container")

    # Fallback: any element that carries data-film-slug directly
    if not items:
        items = [el for el in soup.select("[data-film-slug]") if el.name in ("li", "div")]

    for item in items:
        film_div = item.select_one("div[data-film-slug]")
        if not film_div and item.get("data-film-slug"):
            film_div = item

        if film_div:
            slug = film_div.get("data-film-slug", "")
            title = film_div.get("data-film-name", "")
            year_str = film_div.get("data-film-year", "")

            img = item.select_one("img.image") or item.select_one("img[src]")
            thumb = _letterboxd_thumb(img)

            link = item.select_one("a[href*='/film/']")
            href = link["href"] if link else f"/film/{slug}/"
            film_url = _abs("https://letterboxd.com", href)

            if title or slug:
                films.append({
                    "id": f"mubi_{slug or abs(hash(title))}",
                    "title": title,
                    "year": int(year_str) if year_str and year_str.isdigit() else None,
                    "thumbnail": thumb,
                    "url": film_url,
                    "source": "MUBI",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })
        else:
            # Last-resort fallback: read title from img alt attribute
            img = item.select_one("img[alt]")
            link = item.select_one("a[href]")
            if img and img.get("alt"):
                href = link["href"] if link else ""
                film_url = _abs("https://letterboxd.com", href)
                films.append({
                    "id": f"mubi_{abs(hash(film_url or img['alt']))}",
                    "title": img["alt"],
                    "thumbnail": _letterboxd_thumb(img),
                    "url": film_url,
                    "source": "MUBI",
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })

    return films


def _letterboxd_thumb(img) -> str:
    """Return image URL, skipping Letterboxd placeholder images."""
    if not img:
        return ""
    src = img.get("src") or img.get("data-src", "")
    return "" if "empty-poster" in src else src


# ─── U-NEXT ───────────────────────────────────────────────────────────────────

def fetch_unext_films() -> list[dict]:
    """Scrape foreign movies from U-NEXT genre page."""
    films: list[dict] = []
    try:
        resp = requests.get(
            UNEXT_GENRE_URL,
            headers={
                **COMMON_HEADERS,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=15,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [U-NEXT] Fetch error: {e}", file=sys.stderr)
        return films

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1. Next.js SSR — __NEXT_DATA__ often embeds the full content list
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if tag:
        try:
            data = json.loads(tag.string or "{}")
            items = (
                _deep_find(data, "titleList")
                or _deep_find(data, "titleListDtos")
                or _deep_find(data, "contentList")
                or _deep_find(data, "items")
                or []
            )
            if isinstance(items, list) and items:
                for item in items[:40]:
                    if isinstance(item, dict):
                        films.append(_parse_unext_item(item))
                print(f"  [U-NEXT] {len(films)} films from __NEXT_DATA__")
                return films
        except (json.JSONDecodeError, Exception) as e:
            print(f"  [U-NEXT] __NEXT_DATA__ parse error: {e}", file=sys.stderr)

    # 2. HTML card scraping (for partially SSR pages)
    films = _scrape_unext_html(soup)
    if films:
        print(f"  [U-NEXT] {len(films)} films from HTML cards")
    else:
        print(
            "  [U-NEXT] 0 films found — page may require JavaScript rendering.",
            file=sys.stderr,
        )
    return films


def _scrape_unext_html(soup: BeautifulSoup) -> list[dict]:
    """Extract title cards from U-NEXT HTML (server-rendered fragment)."""
    films: list[dict] = []
    seen: set[str] = set()

    # Try progressively broader selectors
    selectors = [
        "a[href*='/title/SID']",
        "a[href*='/title/']",
        "[class*='titleCard'] a",
        "[class*='TitleCard'] a",
        "[class*='title-card'] a",
    ]
    for sel in selectors:
        for card in soup.select(sel):
            href = card.get("href", "") if card.name == "a" else ""
            if not href:
                link = card.select_one("a[href*='/title/']")
                href = link.get("href", "") if link else ""
            if not href or href in seen:
                continue
            seen.add(href)

            full_url = _abs("https://video.unext.jp", href)
            img = card.select_one("img") if card.name != "img" else card
            title_el = card.select_one("p, span, h3, h2, [class*='title']")
            title = (
                title_el.get_text(strip=True)
                if title_el
                else (img.get("alt", "") if img else "")
            )
            if not title:
                continue

            films.append({
                "id": f"unext_{abs(hash(href))}",
                "title": title,
                "thumbnail": (img.get("src") or img.get("data-src", "")) if img else "",
                "url": full_url,
                "source": "U-NEXT",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

        if films:
            break

    return films


def _parse_unext_item(item: dict) -> dict:
    """Normalize a U-NEXT data item (from __NEXT_DATA__ or API)."""
    title = item.get("title") or item.get("name") or item.get("display_name", "")
    code = item.get("title_code") or item.get("code") or item.get("id", "")
    thumb_raw = item.get("thumbnail_url") or item.get("image_url") or item.get("thumbnail", "")
    thumb = (
        thumb_raw if isinstance(thumb_raw, str)
        else (thumb_raw.get("url", "") if isinstance(thumb_raw, dict) else "")
    )
    return {
        "id": f"unext_{code}",
        "title": title,
        "year": item.get("year") or item.get("release_year"),
        "director": item.get("director", ""),
        "synopsis": item.get("synopsis") or item.get("comment", ""),
        "thumbnail": thumb,
        "url": f"https://video.unext.jp/title/{code}" if code else UNEXT_GENRE_URL,
        "source": "U-NEXT",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─── Utilities ────────────────────────────────────────────────────────────────

def _abs(base: str, href: str) -> str:
    """Convert a relative href to an absolute URL."""
    if not href:
        return base
    return (base + href) if href.startswith("/") else href


def _deep_find(obj, key, _depth=0):
    """Recursively search nested dicts/lists for a key with a non-empty list value."""
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

    print("Fetching MUBI releases from Letterboxd...")
    mubi = fetch_mubi_films()
    print(f"  → {len(mubi)} films total from MUBI")

    time.sleep(2)

    print("Fetching U-NEXT foreign movies...")
    unext = fetch_unext_films()
    print(f"  → {len(unext)} films total from U-NEXT")

    # Merge: fresh results first, then carry-over existing (dedup by URL)
    fresh = mubi + unext
    seen: set[str] = {f["url"] for f in fresh}
    for film in existing:
        url = film.get("url", "")
        if url and url not in seen:
            fresh.append(film)
            seen.add(url)

    fresh.sort(key=lambda f: f.get("fetched_at", ""), reverse=True)
    fresh = fresh[:MAX_FILMS]

    OUTPUT_PATH.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved {len(fresh)} films → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
