#!/usr/bin/env python3
"""Fetch film listings from MUBI (via Letterboxd) and Filmarks daily.
Auto-generates Japanese cinephile comments via Claude API.

Sources:
  MUBI    : https://letterboxd.com/mubi/list/mubi-releases/
  Filmarks: https://filmarks.com/list/trend  (今話題)
            https://filmarks.com/list/now    (上映中)

Scoring (4 axes):
  1. 日本映画         +5.0  (製作国が日本)
  2. 日本で話題       +2.0  (Filmarksソース) +最大1.0 (Filmarks高スコア)
  3. シネフィル注目    +2.0  (映画祭受賞キーワード) +1.0 (アート系監督キーワード)
  4. 製作年ボーナス    +0.5  (2024年以降)

Saves results sorted by score to data/films.json.

Usage:
    pip install requests beautifulsoup4 lxml anthropic
    ANTHROPIC_API_KEY=sk-... python scripts/fetch_films.py
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup

OUTPUT_PATH  = Path("data/films.json")
MAX_FILMS    = 300
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

LETTERBOXD_MUBI_URL   = "https://letterboxd.com/mubi/list/mubi-releases/"
FILMARKS_TREND_URL    = "https://filmarks.com/list/trend"
FILMARKS_NOW_URL      = "https://filmarks.com/list/now"

# Letterboxd: Sec-Fetch-* ヘッダーを含めると縮小HTMLが返るため除外
LETTERBOXD_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

FILMARKS_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer":         "https://filmarks.com/",
}

FESTIVAL_KEYWORDS = [
    "cannes", "berlin", "venice", "sundance", "toronto", "rotterdam", "locarno",
    "カンヌ", "ベルリン", "ヴェネチア", "ヴェニス", "サンダンス", "トロント", "ロカルノ",
    "tiff", "東京国際映画祭", "国際映画祭",
    "award", "prize", "受賞", "グランプリ", "パルム・ドール", "金熊賞", "銀熊賞",
    "golden lion", "palme d'or",
]

ARTHOUSE_KEYWORDS = [
    "是枝", "黒沢清", "濱口竜介", "河瀬直美", "塚本晋也", "深田晃司",
    "ゴダール", "タルコフスキー", "ハネケ", "ケン・ローチ", "アピチャッポン",
    "ウォン・カーウァイ", "ホウ・シャオシェン", "ジャ・ジャンクー",
    "experimental", "avant-garde", "アート系", "実験映画",
]


# ─── Scoring ──────────────────────────────────────────────────────────────────

def score_film(film: dict) -> float:
    """4軸スコアリング。高いほど日本シネフィル向けに価値が高い。"""
    score = 0.0

    country  = (film.get("country") or "").lower()
    source   = film.get("source", "")
    year     = film.get("year") or 0
    synopsis = (film.get("synopsis") or "").lower()
    director = (film.get("director") or "").lower()
    text     = f"{synopsis} {director}"

    # ── 軸1: 日本映画 ────────────────────────────────────────────────────────
    if any(k in country for k in ("japan", "日本", "japanese")):
        score += 5.0

    # ── 軸2: 日本で話題 ──────────────────────────────────────────────────────
    if source == "Filmarks":
        score += 2.0

    filmarks_score = film.get("filmarks_score") or 0.0
    if filmarks_score >= 4.0:
        score += 1.0
    elif filmarks_score >= 3.5:
        score += 0.5

    # ── 軸3: シネフィル注目 ──────────────────────────────────────────────────
    if any(kw in text for kw in FESTIVAL_KEYWORDS):
        score += 2.0
    if any(kw in text for kw in ARTHOUSE_KEYWORDS):
        score += 1.0

    # ── 軸4: 製作年ボーナス ──────────────────────────────────────────────────
    if year >= 2024:
        score += 0.5
    elif year >= 2022:
        score += 0.3

    if film.get("director"):
        score += 0.2

    return round(score, 1)


# ─── Comment generation ───────────────────────────────────────────────────────

def generate_comment(film: dict, client: anthropic.Anthropic) -> str:
    """Claude APIで日本シネフィル向けコメントを生成する。"""
    lines = []
    if film.get("title"):
        lines.append(f"タイトル: {film['title']}")
    if film.get("title_locale") and film["title_locale"] != film.get("title"):
        lines.append(f"原題: {film['title_locale']}")
    if film.get("year"):
        lines.append(f"製作年: {film['year']}")
    if film.get("director"):
        lines.append(f"監督: {film['director']}")
    if film.get("country"):
        lines.append(f"製作国: {film['country']}")
    if film.get("filmarks_score"):
        lines.append(f"Filmarksスコア: {film['filmarks_score']}")
    if film.get("synopsis"):
        lines.append(f"作品概要: {film['synopsis']}")

    film_info = "\n".join(lines)

    prompt = f"""日本のシネフィル向けに、以下の映画がなぜ今注目すべきかを2〜3文で日本語で書いてください。

要件:
- 映画祭での受賞歴・上映歴に触れる（情報がある場合）
- 監督の作家性・これまでの作品との関連に言及する
- 文化的・社会的背景や今日的な意義を示す
- 「この映画は〜」「〜という作品」などの平凡な書き出しを避ける
- コメントのみを出力（前置き・説明・見出し不要）

{film_info}

コメント:"""

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


# ─── MUBI（Letterboxd経由）────────────────────────────────────────────────────

def fetch_mubi_films() -> list[dict]:
    """Letterboxd の MUBI リリースリストをスクレイピング（最大3ページ）。"""
    films: list[dict] = []

    for page in range(1, 4):
        url = LETTERBOXD_MUBI_URL if page == 1 else f"{LETTERBOXD_MUBI_URL}page/{page}/"
        try:
            resp = requests.get(url, headers=LETTERBOXD_HEADERS, timeout=15)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [MUBI] Page {page} error: {e}", file=sys.stderr)
            break

        soup       = BeautifulSoup(resp.text, "html.parser")
        page_films = _parse_letterboxd_page(soup)

        if not page_films:
            print(f"  [MUBI] Page {page}: 0 films — stopping", file=sys.stderr)
            break

        films.extend(page_films)
        print(f"  [MUBI] Page {page}: {len(page_films)} films")

        if not soup.select_one("a.next"):
            break
        time.sleep(1)

    return films


def _parse_letterboxd_page(soup: BeautifulSoup) -> list[dict]:
    """Letterboxdリストページのフィルムデータを抽出。
    実際のHTML: <div data-item-slug="slug" data-item-name="Title (2025)" data-item-link="/film/slug/">
    """
    films: list[dict] = []
    seen:  set[str]   = set()

    for div in soup.select("div[data-item-slug][data-item-name]"):
        slug      = div.get("data-item-slug", "")
        full_name = div.get("data-item-name", "")
        href      = div.get("data-item-link", f"/film/{slug}/")

        if not slug or slug in seen:
            continue
        seen.add(slug)

        year  = None
        title = full_name
        if full_name.endswith(")") and "(" in full_name:
            body, _, year_part = full_name.rpartition("(")
            year_str = year_part.rstrip(")")
            if year_str.isdigit():
                title = body.strip()
                year  = int(year_str)

        films.append({
            "id":         f"mubi_{slug}",
            "title":      title,
            "year":       year,
            "thumbnail":  "",
            "url":        _abs("https://letterboxd.com", href),
            "source":     "MUBI",
            "fetched_at": _now(),
        })

    return films


# ─── Filmarks ─────────────────────────────────────────────────────────────────

def fetch_filmarks_films() -> list[dict]:
    """Filmarksのトレンド・上映中ページをスクレイピング。"""
    films:    list[dict] = []
    seen_ids: set[str]   = set()

    for label, url in [("trend", FILMARKS_TREND_URL), ("now", FILMARKS_NOW_URL)]:
        print(f"  [Filmarks/{label}] {url}")
        try:
            resp = requests.get(url, headers=FILMARKS_HEADERS, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [Filmarks/{label}] error: {e}", file=sys.stderr)
            continue

        soup       = BeautifulSoup(resp.text, "html.parser")
        page_films = _parse_filmarks_page(soup)
        new_films  = [f for f in page_films if f["id"] not in seen_ids]
        for f in new_films:
            seen_ids.add(f["id"])
        films.extend(new_films)
        print(f"  [Filmarks/{label}]: {len(page_films)} found, {len(new_films)} new")
        time.sleep(1)

    return films


def _parse_filmarks_page(soup: BeautifulSoup) -> list[dict]:
    """Filmarks の p-content-cassette カードから映画データを抽出する。

    実際のHTML構造（調査済み）:
      <div class="p-content-cassette">
        <h3 class="p-content-cassette__title">タイトル</h3>
        <div class="p-content-cassette__rate">
          <div class="c-rating__score">4.1</div>
        </div>
        <p class="p-content-cassette__synopsis-desc-text">あらすじ</p>
        <div class="p-content-cassette__other-info">上映日：2026年...／製作国・地域：日本</div>
        <div class="p-content-cassette__people-wrap">監督 ...</div>
      </div>
    """
    films: list[dict] = []
    seen:  set[str]   = set()

    for card in soup.select("div.p-content-cassette"):
        # タイトル
        title_el = card.select_one("h3.p-content-cassette__title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        # 映画ID（/movies/NNNNN 形式のリンクから取得）
        film_id = None
        for a in card.find_all("a", href=True):
            m = re.fullmatch(r"/movies/(\d+)", a.get("href", ""))
            if m:
                film_id = m.group(1)
                break
        if not film_id or film_id in seen:
            continue
        seen.add(film_id)

        # サムネイル（jacket内のimgを優先）
        thumb = ""
        jacket = card.select_one("div.p-content-cassette__jacket")
        img    = (jacket or card).select_one("img")
        if img:
            src = img.get("src") or img.get("data-src", "")
            if src and "placeholder" not in src and "no-image" not in src:
                thumb = src

        # Filmarksスコア（カード直下のrateブロックの最初のスコア）
        filmarks_score = None
        rate_el = card.select_one("div.p-content-cassette__rate .c-rating__score")
        if rate_el:
            m = re.search(r"\d+\.\d+", rate_el.get_text())
            if m:
                filmarks_score = float(m.group())

        # あらすじ
        synopsis_el = card.select_one("p.p-content-cassette__synopsis-desc-text")
        synopsis    = synopsis_el.get_text(strip=True) if synopsis_el else ""

        # 製作国（"製作国・地域：日本" から抽出）
        country = ""
        year    = None
        for info_div in card.select("div.p-content-cassette__other-info"):
            text = info_div.get_text(strip=True)
            if "製作国" in text and not country:
                m = re.search(r"製作国・地域：(.+?)(?:／|$)", text)
                if m:
                    country = m.group(1).strip()
            if "上映日" in text and not year:
                m = re.search(r"(\d{4})年", text)
                if m:
                    year = int(m.group(1))

        # 監督
        director = ""
        for pw in card.select("div.p-content-cassette__people-wrap"):
            term = pw.select_one("h4.p-content-cassette__people-list-term")
            if term and "監督" in term.get_text():
                desc = pw.select_one("li.p-content-cassette__people-list-desc")
                if desc:
                    director = desc.get_text(strip=True)
                break

        films.append({
            "id":             f"filmarks_{film_id}",
            "title":          title,
            "year":           year,
            "director":       director,
            "country":        country,
            "synopsis":       synopsis,
            "thumbnail":      thumb,
            "url":            f"https://filmarks.com/movies/{film_id}",
            "source":         "Filmarks",
            "filmarks_score": filmarks_score,
            "fetched_at":     _now(),
        })

    return films


# ─── Utilities ────────────────────────────────────────────────────────────────

def _abs(base: str, href: str) -> str:
    if not href:
        return base
    return (base + href) if href.startswith("/") else href


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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

    # ── 既存データ読み込み（コメントを保持するため）────────────────────────────
    existing: list[dict] = []
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    existing_by_id: dict[str, dict] = {f["id"]: f for f in existing if f.get("id")}

    # ── スクレイピング ─────────────────────────────────────────────────────────
    print(f"\n[1/2] Fetching MUBI from {LETTERBOXD_MUBI_URL} ...")
    mubi = fetch_mubi_films()
    print(f"  → {len(mubi)} films total\n")

    time.sleep(2)

    print(f"[2/2] Fetching Filmarks ...")
    filmarks = fetch_filmarks_films()
    print(f"  → {len(filmarks)} films total\n")

    # ── マージ（URLで重複排除）───────────────────────────────────────────────
    fresh: list[dict] = mubi + filmarks
    seen_urls: set[str] = {f["url"] for f in fresh}
    for film in existing:
        url = film.get("url", "")
        if url and url not in seen_urls:
            fresh.append(film)
            seen_urls.add(url)

    # 既存コメントを復元（再スクレイプで消えないように）
    for film in fresh:
        prev = existing_by_id.get(film["id"])
        if prev and prev.get("comment") and not film.get("comment"):
            film["comment"] = prev["comment"]

    # ── スコアリング → スコア降順でソート ────────────────────────────────────
    for film in fresh:
        film["score"] = score_film(film)

    fresh.sort(key=lambda f: f.get("score", 0), reverse=True)
    fresh = fresh[:MAX_FILMS]

    if fresh:
        print(f"Score range: {fresh[0]['score']} (top) → {fresh[-1]['score']} (bottom)")

    # ── コメント生成 ───────────────────────────────────────────────────────────
    if claude_client:
        needs_comment = [f for f in fresh if not f.get("comment")]
        already_done  = len(fresh) - len(needs_comment)
        print(f"\nComment generation: {len(needs_comment)} new / {already_done} already done (skipped)")

        for i, film in enumerate(needs_comment, 1):
            try:
                film["comment"] = generate_comment(film, claude_client)
                print(f"  [{i}/{len(needs_comment)}] {film['title']}")
            except Exception as e:
                print(f"  [{i}/{len(needs_comment)}] {film['title']}: {e}", file=sys.stderr)
                film["comment"] = ""
            time.sleep(0.5)

    # ── 保存 ──────────────────────────────────────────────────────────────────
    OUTPUT_PATH.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(fresh)} films → {OUTPUT_PATH}")
    mubi_count     = sum(1 for f in fresh if f.get("source") == "MUBI")
    filmarks_count = sum(1 for f in fresh if f.get("source") == "Filmarks")
    print(f"  MUBI: {mubi_count}  Filmarks: {filmarks_count}")


if __name__ == "__main__":
    main()
