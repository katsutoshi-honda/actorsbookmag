#!/usr/bin/env python3
"""Fetch film listings from MUBI and U-NEXT daily.
Auto-generates Japanese cinephile comments via Anthropic API.

Sources:
  MUBI  : https://mubi.com/en/jp/showing
  U-NEXT: https://video.unext.jp/browse/feature/JFETMAN0006

Scoring:
  1. 日本映画                +5  (製作国が日本)
  2. 日本語字幕/吹替あり     +3  (U-NEXTソース)
  3. 映画祭受賞              +4  (カンヌ/ベルリン/ヴェネチア等)
  4. 注目監督               +3  (濱口竜介/是枝裕和/黒沢清/三宅唱/西川美和/河瀬直美等)
  5. 問題監督               除外 (園子温等)
  6. 製作年ボーナス          +0.5 (2024年以降)
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup

OUTPUT_PATH = Path("data/films.json")
MAX_FILMS_PER_SOURCE = 10
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

MUBI_URL = "https://mubi.com/en/jp/showing"
UNEXT_URL = "https://video.unext.jp/browse/feature/JFETMAN0006"

MUBI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}

UNEXT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://video.unext.jp/",
}

FESTIVAL_KEYWORDS = [
    "cannes", "berlin", "venice", "sundance", "toronto", "rotterdam",
    "locarno", "カンヌ", "ベルリン", "ヴェネチア", "ヴェニス", "サンダンス",
    "トロント", "ロカルノ", "tiff", "東京国際映画祭", "国際映画祭",
    "award", "prize", "受賞", "グランプリ", "パルムードール", "金熊賞",
    "銀熊賞", "golden lion", "palme d'or", "golden bear", "silver bear",
]

ARTHOUSE_DIRECTORS = [
    "濱口竜介", "是枝裕和", "黒沢清", "三宅唱", "西川美和", "河瀬直美",
    "塚本晋也", "深田晃司", "石井裕也", "大森立嗣",
    "ゴダール", "タルコフスキー", "ハネケ", "ケン・ローチ",
    "アピチャッポン", "ウォン・カーウァイ", "ホウ・シャオシェン",
    "ジャ・ジャンクー", "ポン・ジュノ", "パク・チャヌク",
    "kore-eda", "kurosawa", "hamaguchi", "miyake", "nishikawa",
]

EXCLUDED_DIRECTORS = ["園子温", "sono sion", "sono, sion"]

COUNTRY_FLAGS = {
    "japan": "🇯🇵", "japanese": "🇯🇵", "日本": "🇯🇵",
    "france": "🇫🇷", "french": "🇫🇷", "フランス": "🇫🇷",
    "south korea": "🇰🇷", "korea": "🇰🇷", "韓国": "🇰🇷",
    "italy": "🇮🇹", "italian": "🇮🇹", "イタリア": "🇮🇹",
    "germany": "🇩🇪", "german": "🇩🇪", "ドイツ": "🇩🇪",
    "united kingdom": "🇬🇧", "uk": "🇬🇧", "britain": "🇬🇧", "イギリス": "🇬🇧",
    "united states": "🇺🇸", "usa": "🇺🇸", "アメリカ": "🇺🇸",
    "china": "🇨🇳", "chinese": "🇨🇳", "中国": "🇨🇳",
    "taiwan": "🇹🇼", "taiwanese": "🇹🇼", "台湾": "🇹🇼",
    "iran": "🇮🇷", "iranian": "🇮🇷", "イラン": "🇮🇷",
    "romania": "🇷🇴", "ルーマニア": "🇷🇴",
    "poland": "🇵🇱", "polish": "🇵🇱", "ポーランド": "🇵🇱",
    "denmark": "🇩🇰", "danish": "🇩🇰", "デンマーク": "🇩🇰",
    "sweden": "🇸🇪", "swedish": "🇸🇪", "スウェーデン": "🇸🇪",
    "spain": "🇪🇸", "spanish": "🇪🇸", "スペイン": "🇪🇸",
    "brazil": "🇧🇷", "brazilian": "🇧🇷", "ブラジル": "🇧🇷",
    "thailand": "🇹🇭", "thai": "🇹🇭", "タイ": "🇹🇭",
    "argentina": "🇦🇷", "アルゼンチン": "🇦🇷",
    "mexico": "🇲🇽", "mexican": "🇲🇽", "メキシコ": "🇲🇽",
    "india": "🇮🇳", "indian": "🇮🇳", "インド": "🇮🇳",
    "russia": "🇷🇺", "russian": "🇷🇺", "ロシア": "🇷🇺",
    "austria": "🇦🇹", "austrian": "🇦🇹", "オーストリア": "🇦🇹",
    "belgium": "🇧🇪", "belgian": "🇧🇪", "ベルギー": "🇧🇪",
    "turkey": "🇹🇷", "turkish": "🇹🇷", "トルコ": "🇹🇷",
    "israel": "🇮🇱", "israeli": "🇮🇱", "イスラエル": "🇮🇱",
    "portugal": "🇵🇹", "portuguese": "🇵🇹", "ポルトガル": "🇵🇹",
    "greece": "🇬🇷", "greek": "🇬🇷", "ギリシャ": "🇬🇷",
    "hungary": "🇭🇺", "hungarian": "🇭🇺", "ハンガリー": "🇭🇺",
    "czech": "🇨🇿", "チェコ": "🇨🇿",
    "hong kong": "🇭🇰", "香港": "🇭🇰",
    "philippines": "🇵🇭", "フィリピン": "🇵🇭",
    "indonesia": "🇮🇩", "インドネシア": "🇮🇩",
    "vietnam": "🇻🇳", "ベトナム": "🇻🇳",
}


def get_country_flag(country: str) -> str:
    if not country:
        return ""
    c = country.lower().strip()
    for key, flag in COUNTRY_FLAGS.items():
        if key in c:
            return flag
    return ""


def score_film(film: dict) -> float:
    score = 0.0
    country = (film.get("country") or "").lower()
    director = (film.get("director") or "").lower()
    synopsis = (film.get("synopsis") or "").lower()
    year = film.get("year") or 0
    text = f"{synopsis} {director}"

    for excl in EXCLUDED_DIRECTORS:
        if excl.lower() in director:
            return -999.0

    if any(k in country for k in ("japan", "日本", "japanese")):
        score += 5.0

    if film.get("source") == "U-NEXT":
        score += 3.0

    if any(kw in text for kw in FESTIVAL_KEYWORDS):
        score += 4.0

    for kw in ARTHOUSE_DIRECTORS:
        if kw.lower() in director:
            score += 3.0
            break

    if year >= 2024:
        score += 0.5
    elif year >= 2022:
        score += 0.3

    if film.get("director"):
        score += 0.2

    return round(score, 1)


def generate_comment(film: dict, client: anthropic.Anthropic) -> str:
    lines = []
    if film.get("title"):
        lines.append(f"タイトル: {film['title']}")
    if film.get("year"):
        lines.append(f"製作年: {film['year']}")
    if film.get("director"):
        lines.append(f"監督: {film['director']}")
    if film.get("country"):
        lines.append(f"製作国: {film['country']}")
    if film.get("synopsis"):
        lines.append(f"作品概要: {film['synopsis']}")
    film_info = "\n".join(lines)

    prompt = f"""日本のシネフィル向けに、以下の映画がなぜ今注目すべきかを2〜3文で日本語で書いてください。

要件:
- 映画祭での受賞歴・上映歴に触れる（情報がある場合）
- 監督の作家性・これまでの作品との関連に言及する
- 文化的・社会的背景や今日的な意義を示す
- 「この映画は〜」「〜という作品」などの平凡な書お出しを避ける
- コメントのみを出力（前置き・説明・見出し不要）

{film_info}

コメント:"""

    resp = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


def fetch_mubi_films() -> list[dict]:
    """MUBIの上映中映画を__NEXT_DATAからスクレイピング"""
    print(f"  [MUBI] Fetching {MUBI_URL}")
    try:
        resp = requests.get(MUBI_URL, headers=MUBI_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [MUBI] Error: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script:
        print("  [MUBI] __NEXT_DATA__ not found", file=sys.stderr)
        return []

    try:
        data = json.loads(script.string)
    except json.JSONDecodeError as e:
        print(f"  [MUBI] JSON parse error: {e}", file=sys.stderr)
        return []

    films_raw = []
    try:
        props = data.get("props", {}).get("pageProps", {})
        for key in ("currentShowings", "showingFilms", "films", "filmList", "items"):
            candidate = props.get(key)
            if candidate and isinstance(candidate, list):
                films_raw = candidate
                break
        if not films_raw:
            for key, val in props.items():
                if isinstance(val, dict):
                    for k2, v2 in val.items():
                        if isinstance(v2, list) and len(v2) > 0 and isinstance(v2[0], dict):
                            if any(fk in v2[0] for fk in ("film", "title", "name")):
                                films_raw = v2
                                break
    except Exception as e:
        print(f"  [MUBI] Structure parse error: {e}", file=sys.stderr)

    films = []
    now = _now()
    for item in films_raw:
        film_data = item.get("film", item)
        title = (film_data.get("title") or film_data.get("name") or "").strip()
        if not title:
            continue

        film_id = str(film_data.get("id") or film_data.get("slug") or title)
        slug = film_data.get("slug") or re.sub(r"[^\w-]", "-", title.lower())
        year = film_data.get("year") or film_data.get("release_year")
        director = ""
        directors = film_data.get("directors") or film_data.get("director") or []
        if isinstance(directors, list) and directors:
            d = directors[0]
            director = (d.get("name") or d.get("full_name") or "") if isinstance(d, dict) else str(d)
        elif isinstance(directors, str):
            director = directors

        country = film_data.get("country") or ""
        synopsis = (film_data.get("excerpt") or film_data.get("synopsis") or "").strip()
        thumbnail = ""
        still = film_data.get("still_url") or film_data.get("still") or {}
        if isinstance(still, dict):
            thumbnail = still.get("url") or still.get("retina") or still.get("standard") or ""
        elif isinstance(still, str):
            thumbnail = still
        if not thumbnail:
            poster = film_data.get("poster") or {}
            if isinstance(poster, dict):
                thumbnail = poster.get("url") or poster.get("medium") or ""

        expires_at = item.get("available_until") or item.get("expires_at") or ""

        films.append({
            "id": f"mubi_{film_id}",
            "title": title,
            "year": year,
            "director": director,
            "country": country,
            "country_flag": get_country_flag(country),
            "synopsis": synopsis,
            "thumbnail": thumbnail,
            "url": f"https://mubi.com/en/jp/films/{slug}",
            "source": "MUBI",
            "expires_at": expires_at,
            "fetched_at": now,
        })

    for f in films:
        if not f.get("expires_at"):
            try:
                fetched = datetime.fromisoformat(f["fetched_at"].replace("Z", "+00:00"))
                f["expires_at"] = (fetched + timedelta(days=30)).isoformat()
            except Exception:
                pass

    print(f"  [MUBI] {len(films)} films found")
    return films[:MAX_FILMS_PER_SOURCE]


def fetch_unext_films() -> list[dict]:
    """U-NEXTの新着映画をHTMLスクレイピング"""
    print(f"  [U-NEXT] Fetching {UNEXT_URL}")
    try:
        resp = requests.get(UNEXT_URL, headers=UNEXT_HEADERS, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [U-NEXT] Error: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    films = []
    seen = set()
    now = _now()

    for link in soup.select("a[href*='/title/']"):
        href = link.get("href", "")
        m = re.search(r"/title/(SID\d+)", href)
        if not m:
            continue
        sid = m.group(1)
        if sid in seen:
            continue
        seen.add(sid)

        title_el = link.select_one("h3") or link.select_one("[class*='title']")
        if title_el:
            title = title_el.get_text(strip=True)
        else:
            title = link.get_text(strip=True).split("\n")[0].strip()

        title = re.sub(r"^New\s*", "", title, flags=re.IGNORECASE).strip()
        if not title:
            continue

        img = link.select_one("img")
        thumbnail = ""
        if img:
            thumbnail = img.get("src") or img.get("data-src") or ""

        try:
            fetched = datetime.fromisoformat(now.replace("Z", "+00:00"))
            expires_at = (fetched + timedelta(days=30)).isoformat()
        except Exception:
            expires_at = ""

        films.append({
            "id": f"unext_{sid}",
            "title": title,
            "year": None,
            "director": "",
            "country": "",
            "country_flag": "",
            "synopsis": "",
            "thumbnail": thumbnail,
            "url": f"https://video.unext.jp/title/{sid}",
            "source": "U-NEXT",
            "expires_at": expires_at,
            "fetched_at": now,
        })

    print(f"  [U-NEXT] {len(films)} films found")
    return films[:MAX_FILMS_PER_SOURCE]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        claude_client = anthropic.Anthropic(api_key=api_key)
        print(f"Claude API ready (model: {CLAUDE_MODEL})")
    else:
        claude_client = None
        print("Warning: ANTHROPIC_API_KEY not set — comment generation skipped", file=sys.stderr)

    existing: list[dict] = []
    if OUTPUT_PATH.exists():
        try:
            existing = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    existing_by_id: dict[str, dict] = {f["id"]: f for f in existing if f.get("id")}

    print("\n[1/2] Fetching MUBI...")
    mubi = fetch_mubi_films()
    print(f" → {len(mubi)} films\n")
    time.sleep(2)

    print("[2/2] Fetching U-NEXT...")
    unext = fetch_unext_films()
    print(f" → {len(unext)} films\n")

    fresh = mubi + unext

    for film in fresh:
        prev = existing_by_id.get(film["id"])
        if prev:
            if prev.get("comment") and not film.get("comment"):
                film["comment"] = prev["comment"]
            for field in ("director", "country", "country_flag", "year", "synopsis"):
                if not film.get(field) and prev.get(field):
                    film[field] = prev[field]
            if not film.get("country_flag") and film.get("country"):
                film["country_flag"] = get_country_flag(film["country"])

    for film in fresh:
        film["score"] = score_film(film)

    fresh = [f for f in fresh if f.get("score", 0) > -999]
    fresh.sort(key=lambda f: f.get("score", 0), reverse=True)

    if fresh:
        print(f"Score range: {fresh[0]['score']} (top) → {fresh[-1]['score']} (bottom)")

    if claude_client:
        needs_comment = [f for f in fresh if not f.get("comment")]
        already_done = len(fresh) - len(needs_comment)
        print(f"\nComment generation: {len(needs_comment)} new / {already_done} already done")
        for i, film in enumerate(needs_comment, 1):
            try:
                film["comment"] = generate_comment(film, claude_client)
                print(f"  [{i}/{len(needs_comment)}] {film['title']}")
            except Exception as e:
                print(f"  [{i}/{len(needs_comment)}] {film['title']}: {e}", file=sys.stderr)
                film["comment"] = ""
            time.sleep(0.5)

    OUTPUT_PATH.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved {len(fresh)} films → {OUTPUT_PATH}")
    mubi_count = sum(1 for f in fresh if f.get("source") == "MUBI")
    unext_count = sum(1 for f in fresh if f.get("source") == "U-NEXT")
    print(f"  MUBI: {mubi_count}  U-NEXT: {unext_count}")


if __name__ == "__main__":
    main()
