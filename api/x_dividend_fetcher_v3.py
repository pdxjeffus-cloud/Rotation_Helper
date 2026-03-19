#!/usr/bin/env python3
"""
X dividend fetcher (NY-day strict)

Rules:
- Target day is NY calendar date (default: NY today)
- Optional override: DIVIDEND_ASOF_DATE=YYYY-MM-DD (NY date)
- Fetch window is NY midnight -> min(next NY midnight, now_utc - 15s)
  (X API requires end_time at least 10s before request time)
- Inclusion is STRICTLY by tweet.created_at converted to NY date == target_date
- Writes canonical CSV: data_dividends.csv (unless OUT_CSV overridden)

YieldMax behavior:
- If a qualifying YieldMax tweet links a GlobeNewswire article, the article is the source of truth.
- If no GlobeNewswire link exists, tweet text parsing is used as fallback.
"""

import os
import re
import csv
import html
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

import requests
import tweepy

NY = ZoneInfo("America/New_York")

# YieldMax line examples:
# $ABNY – $0.2495
# ABNY - 0.2495
RE_YM_TEXT_LINE = re.compile(r"\$?([A-Z]{2,8})\s*[–—-]\s*\$?\s*([0-9]+(?:\.[0-9]+)?)")
RE_YM_GENERIC_PAIR = re.compile(r"\b([A-Z]{2,8})\b[^0-9\n]{0,14}\$?\s*([0-9]+\.[0-9]+)\b")
RE_HTTP_URL = re.compile(r"https?://\S+")

BAD_TEXT = (
    "tomorrow",
    "next week",
    "will be announced",
    "preview",
    "here’s a look",
    "heres a look",
)

DEFAULT_QUERY = (
    '(from:YieldMaxETFs OR from:roundhill) '
    '(distribution OR distributions OR "Distribution Announcement") '
    '-is:retweet'
)

REQUEST_TIMEOUT = float((os.getenv("YIELDMAX_IMG_TIMEOUT") or "20").strip())
AMOUNT_MIN = float((os.getenv("YIELDMAX_AMOUNT_MIN") or "0.0001").strip())
AMOUNT_MAX = float((os.getenv("YIELDMAX_AMOUNT_MAX") or "25.0").strip())


def ny_now() -> datetime:
    return datetime.now(NY)


def parse_target_ny_date() -> date:
    """
    Target announcement day in NY time.
    Default: NY today
    Optional: DIVIDEND_ASOF_DATE=YYYY-MM-DD (force exact NY date)
    """
    override = (os.getenv("DIVIDEND_ASOF_DATE") or "").strip()
    if override:
        try:
            return date.fromisoformat(override)
        except Exception:
            pass
    return ny_now().date()


def ny_midnight_utc(d: date) -> datetime:
    ny_mid = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=NY)
    return ny_mid.astimezone(timezone.utc)


def looks_noise(txt: str) -> bool:
    t = (txt or "").lower()
    return any(x in t for x in BAD_TEXT)


def get_bearer() -> str:
    return (
        os.getenv("TWITTER_BEARER_TOKEN")
        or os.getenv("X_BEARER_TOKEN")
        or os.getenv("BEARER_TOKEN")
        or ""
    ).strip()


def safe_end_utc(candidate_end: datetime) -> datetime:
    """
    X API requires end_time <= now - 10 seconds.
    We'll clamp to now - 15 seconds to be safe.
    """
    now_utc = datetime.now(timezone.utc)
    hard_cap = now_utc - timedelta(seconds=15)
    return candidate_end if candidate_end <= hard_cap else hard_cap


def write_csv(rows, out_path: str):
    cols = ["ticker", "amount", "ex_date", "pay_date", "asof_date", "source"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def extract_pairs_from_text(text: str) -> dict[str, float]:
    found: dict[str, float] = {}
    cleaned = (text or "").upper().replace("—", "-").replace("–", "-")

    # Highest-confidence pattern first: ticker - amount
    for sym, amt in RE_YM_TEXT_LINE.findall(cleaned):
        try:
            val = float(amt)
        except Exception:
            continue
        if AMOUNT_MIN <= val <= AMOUNT_MAX:
            found[sym] = val

    # Fallback generic extraction from OCR/article-like lines.
    if not found:
        for line in cleaned.splitlines():
            ln = re.sub(r"[^A-Z0-9\.$\-\s]", " ", line)
            m = RE_YM_GENERIC_PAIR.search(ln)
            if not m:
                continue
            sym = m.group(1).strip()
            amt_s = m.group(2).strip()
            try:
                val = float(amt_s)
            except Exception:
                continue
            if AMOUNT_MIN <= val <= AMOUNT_MAX:
                found[sym] = val

    return found


def fetch_announcements(client: tweepy.Client, query: str, start_utc: datetime, end_utc: datetime):
    """
    Use search_recent_tweets with media + author expansions.
    """
    resp = client.search_recent_tweets(
        query=query,
        start_time=start_utc.isoformat().replace("+00:00", "Z"),
        end_time=end_utc.isoformat().replace("+00:00", "Z"),
        max_results=100,
        tweet_fields=["created_at", "text", "author_id", "attachments", "referenced_tweets", "entities"],
        expansions=["attachments.media_keys", "author_id"],
        media_fields=["media_key", "type", "url", "preview_image_url"],
        user_fields=["username"],
    )
    tweets = list(resp.data or [])
    includes = resp.includes or {}
    media = includes.get("media") or []
    users = includes.get("users") or []
    media_by_key = {}
    for m in media:
        key = getattr(m, "media_key", None) or (m.get("media_key") if isinstance(m, dict) else None)
        if key:
            media_by_key[key] = m

    user_by_id = {}
    for u in users:
        uid_raw = getattr(u, "id", None) or (u.get("id") if isinstance(u, dict) else None)
        uid = str(uid_raw or "")
        if uid:
            username = getattr(u, "username", None) or (u.get("username") if isinstance(u, dict) else None)
            user_by_id[uid] = (username or "").lower()

    tweet_by_id = {}
    for t in tweets:
        tid_raw = getattr(t, "id", None) or (t.get("id") if isinstance(t, dict) else None)
        tid = str(tid_raw or "")
        if tid:
            tweet_by_id[tid] = t

    return tweets, media_by_key, user_by_id, tweet_by_id


def _extract_urls_from_entities(tweet) -> list[str]:
    entities = getattr(tweet, "entities", None)
    if not entities:
        return []

    if isinstance(entities, dict):
        urls = entities.get("urls") or []
    else:
        urls = getattr(entities, "urls", None) or []

    out: list[str] = []
    for u in urls:
        if isinstance(u, dict):
            expanded = u.get("expanded_url") or u.get("url") or u.get("unwound_url")
        else:
            expanded = getattr(u, "expanded_url", None) or getattr(u, "url", None) or getattr(u, "unwound_url", None)
        if expanded:
            out.append(str(expanded).strip())
    return out


def _extract_urls_from_text(tweet_text: str) -> list[str]:
    return [u.rstrip(".,)") for u in RE_HTTP_URL.findall(tweet_text or "")]


def _find_globenewswire_url(tweet) -> str | None:
    candidates = _extract_urls_from_entities(tweet)
    candidates.extend(_extract_urls_from_text(getattr(tweet, "text", "") or ""))

    for url in candidates:
        if "globenewswire.com" in url.lower():
            return url

    # Some tweet links are t.co short URLs; resolve them to check target host.
    for url in candidates:
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            final_url = (r.url or "").strip()
            if "globenewswire.com" in final_url.lower():
                return final_url
        except Exception:
            continue

    return None


def _strip_html_to_text(html_doc: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html_doc, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text


def _parse_globenewswire_rows(article_text: str) -> dict[str, float]:
    # Process line-by-line for precision first.
    found: dict[str, float] = {}
    upper_text = (article_text or "").upper().replace("—", "-").replace("–", "-")

    for raw_line in upper_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line_pairs = extract_pairs_from_text(line)
        for sym, val in line_pairs.items():
            found[sym] = val

    # Fallback to whole-text scan if line-by-line did not find anything.
    if not found:
        for sym, amt in RE_YM_TEXT_LINE.findall(upper_text):
            try:
                val = float(amt)
            except Exception:
                continue
            if AMOUNT_MIN <= val <= AMOUNT_MAX:
                found[sym] = val

    return found


def _fetch_globenewswire_pairs(url: str) -> tuple[bool, dict[str, float], str]:
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        return False, {}, f"fetch_failed:{e}"

    text = _strip_html_to_text(resp.text or "")
    pairs = _parse_globenewswire_rows(text)
    return True, pairs, "ok"


def parse_yieldmax_rows(tweets, media_by_key: dict, user_by_id: dict, tweet_by_id: dict, target_day: date):
    """
    YieldMax parser with GlobeNewswire article source-of-truth.

    Rules:
    - Strict NY-date filtering.
    - Only @YieldMaxETFs tweets are parsed.
    - If a GlobeNewswire link exists in the tweet, article rows are source-of-truth.
    - If no GlobeNewswire link exists, fall back to tweet text parsing.
    """
    _ = media_by_key  # kept for dashboard compatibility/signature stability
    _ = tweet_by_id   # kept for dashboard compatibility/signature stability

    rows = []
    seen = {}
    rejected = []

    for t in tweets:
        txt = t.text or ""
        if looks_noise(txt):
            continue

        created_ny = t.created_at.astimezone(NY)
        if created_ny.date() != target_day:
            continue

        uname = user_by_id.get(str(t.author_id), "")
        if uname != "yieldmaxetfs":
            continue

        tweet_id = str(t.id)
        article_url = _find_globenewswire_url(t)
        print(f"YIELDMAX_DEBUG tweet_id={tweet_id} article_url_found={article_url or ''}")

        chosen_pairs: dict[str, float] = {}
        accepted_reason = ""

        if article_url:
            ok, article_pairs, status = _fetch_globenewswire_pairs(article_url)
            print(
                "YIELDMAX_DEBUG "
                f"tweet_id={tweet_id} "
                f"article_fetch_success={'yes' if ok else 'no'} "
                f"article_fetch_status={status}"
            )
            print(
                "YIELDMAX_DEBUG "
                f"tweet_id={tweet_id} "
                f"article_rows_parsed={len(article_pairs)}"
            )

            if not ok:
                reason = f"tweet {tweet_id}: article fetch failed"
                print(f"YIELDMAX_DEBUG tweet_id={tweet_id} final_reason=rejected:{reason}")
                rejected.append(reason)
                continue

            if not article_pairs:
                reason = f"tweet {tweet_id}: article parsed zero ticker/amount rows"
                print(f"YIELDMAX_DEBUG tweet_id={tweet_id} final_reason=rejected:{reason}")
                rejected.append(reason)
                continue

            chosen_pairs = article_pairs
            accepted_reason = "accepted:article_pairs_used"
            source_value = f"GlobeNewswire {article_url} via @YieldMaxETFs tweet {tweet_id}"
        else:
            text_pairs = extract_pairs_from_text(txt)
            print(f"YIELDMAX_DEBUG tweet_id={tweet_id} article_fetch_success=no article_fetch_status=no_link")
            print(f"YIELDMAX_DEBUG tweet_id={tweet_id} article_rows_parsed=0")

            if not text_pairs:
                reason = f"tweet {tweet_id}: no article link and no parseable text pairs"
                print(f"YIELDMAX_DEBUG tweet_id={tweet_id} final_reason=rejected:{reason}")
                rejected.append(reason)
                continue

            chosen_pairs = text_pairs
            accepted_reason = "accepted:text_pairs_used_fallback"
            source_value = f"@YieldMaxETFs tweet {tweet_id}"

        for sym, amount in sorted(chosen_pairs.items()):
            prev_amt = seen.get(sym)
            if prev_amt is not None and abs(prev_amt - amount) > 1e-9:
                reason = f"tweet {tweet_id}: duplicate ticker conflict across tweets for {sym} ({prev_amt} vs {amount})"
                print(f"YIELDMAX_DEBUG tweet_id={tweet_id} final_reason=rejected:{reason}")
                rejected.append(reason)
                continue
            seen[sym] = amount

            rows.append(
                {
                    "ticker": sym,
                    "amount": amount,
                    "ex_date": "",
                    "pay_date": "",
                    "asof_date": target_day.isoformat(),
                    "source": source_value,
                }
            )

        print(f"YIELDMAX_DEBUG tweet_id={tweet_id} final_reason={accepted_reason} count={len(chosen_pairs)}")

    return rows, rejected


def parse_roundhill_stub_rows(tweets, user_by_id: dict, target_day: date):
    """
    Roundhill announcement posts are often images; OCR is handled elsewhere.
    Here we only detect that a qualifying Roundhill announcement exists.
    (No forced fake tickers.)
    """
    hits = []
    for t in tweets:
        txt = t.text or ""
        if "distribution announcement" not in txt.lower():
            continue
        created_ny = t.created_at.astimezone(NY)
        if created_ny.date() != target_day:
            continue
        uname = user_by_id.get(str(t.author_id), "")
        if uname != "roundhill":
            continue
        hits.append(t)
    return [], hits


def main():
    bearer = get_bearer()
    if not bearer:
        raise SystemExit("Missing TWITTER_BEARER_TOKEN (or X_BEARER_TOKEN/BEARER_TOKEN) in .env")

    out_csv = (os.getenv("OUT_CSV") or "data_dividends.csv").strip()
    query = (os.getenv("X_QUERY") or DEFAULT_QUERY).strip()

    target_day = parse_target_ny_date()
    start_utc = ny_midnight_utc(target_day)
    end_candidate = ny_midnight_utc(target_day + timedelta(days=1))
    end_utc = safe_end_utc(end_candidate)

    now_ny = ny_now()
    now_utc = datetime.now(timezone.utc)
    print(f"NY_NOW: {now_ny.isoformat()}")
    print(f"UTC_NOW: {now_utc.isoformat()}")
    print(f"NY_TARGET: {target_day.isoformat()}")
    print(f"FETCH start_utc: {start_utc.isoformat()}")
    print(f"FETCH end_utc:   {end_utc.isoformat()}  (clamped)")

    client = tweepy.Client(bearer_token=bearer, wait_on_rate_limit=True)

    tweets, media_by_key, user_by_id, tweet_by_id = fetch_announcements(
        client,
        query=query,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    ym_rows, ym_rejected = parse_yieldmax_rows(tweets, media_by_key, user_by_id, tweet_by_id, target_day)
    rh_rows, rh_hits = parse_roundhill_stub_rows(tweets, user_by_id, target_day)

    rows = []
    rows.extend(ym_rows)
    rows.extend(rh_rows)

    write_csv(rows, out_csv)

    print(f"tweets returned: {len(tweets)}")
    print(f"YieldMax rows:   {len(ym_rows)}")
    if ym_rejected:
        print(f"YieldMax rejected: {len(ym_rejected)}")
        for msg in ym_rejected:
            print(f"  - {msg}")
    print(f"Roundhill hits:  {len(rh_hits)} (OCR handled elsewhere)")
    print(f"Wrote {len(rows)} rows to {out_csv}")


if __name__ == "__main__":
    main()
