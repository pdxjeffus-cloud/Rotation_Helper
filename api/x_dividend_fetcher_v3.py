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
"""

import os
import re
import csv
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

import tweepy

NY = ZoneInfo("America/New_York")

# YieldMax line examples:
# $ABNY – $0.2495
# $AMDY - $0.5465
RE_YM_LINE = re.compile(r"\$([A-Z]{2,8})\s*[–—-]\s*\$?\s*([0-9]+(?:\.[0-9]+)?)")

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
    # midnight NY -> UTC
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

def fetch_announcements(client: tweepy.Client, query: str, start_utc: datetime, end_utc: datetime):
    """
    Use search_recent_tweets with a safe end_time.
    We fetch up to 100 results and then filter strictly by NY date.
    """
    resp = client.search_recent_tweets(
        query=query,
        start_time=start_utc.isoformat().replace("+00:00", "Z"),
        end_time=end_utc.isoformat().replace("+00:00", "Z"),
        max_results=100,
        tweet_fields=["created_at", "text", "author_id"],
    )
    return resp.data or []

def parse_yieldmax_rows(tweets, target_day: date):
    rows = []
    seen = set()

    for t in tweets:
        txt = t.text or ""
        if looks_noise(txt):
            continue

        created_ny = t.created_at.astimezone(NY)
        if created_ny.date() != target_day:
            continue

        matches = RE_YM_LINE.findall(txt.upper())
        if not matches:
            continue

        for sym, amt in matches:
            sym = sym.upper().strip()
            try:
                amount = float(amt)
            except Exception:
                continue

            key = ("YieldMaxETFs", target_day.isoformat(), sym)
            if key in seen:
                continue
            seen.add(key)

            rows.append(
                {
                    "ticker": sym,
                    "amount": amount,
                    "ex_date": "",
                    "pay_date": "",
                    "asof_date": target_day.isoformat(),
                    "source": f"@YieldMaxETFs tweet {t.id}",
                }
            )
    return rows

def parse_roundhill_stub_rows(tweets, target_day: date):
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
        hits.append(t)
    # No rows here; OCR scripts can read images/links separately.
    return [], hits

def main():
    bearer = get_bearer()
    if not bearer:
        raise SystemExit("Missing TWITTER_BEARER_TOKEN (or X_BEARER_TOKEN/BEARER_TOKEN) in .env")

    out_csv = (os.getenv("OUT_CSV") or "data_dividends.csv").strip()
    query = (os.getenv("X_QUERY") or DEFAULT_QUERY).strip()

    target_day = parse_target_ny_date()
    start_utc = ny_midnight_utc(target_day)
    end_candidate = ny_midnight_utc(target_day + timedelta(days=1))  # next NY midnight in UTC
    end_utc = safe_end_utc(end_candidate)

    # Debug banner (useful when you are in Thailand)
    now_ny = ny_now()
    now_utc = datetime.now(timezone.utc)
    print(f"NY_NOW: {now_ny.isoformat()}")
    print(f"UTC_NOW: {now_utc.isoformat()}")
    print(f"NY_TARGET: {target_day.isoformat()}")
    print(f"FETCH start_utc: {start_utc.isoformat()}")
    print(f"FETCH end_utc:   {end_utc.isoformat()}  (clamped)")

    client = tweepy.Client(bearer_token=bearer, wait_on_rate_limit=True)

    tweets = fetch_announcements(client, query=query, start_utc=start_utc, end_utc=end_utc)

    ym_rows = parse_yieldmax_rows(tweets, target_day)
    rh_rows, rh_hits = parse_roundhill_stub_rows(tweets, target_day)

    rows = []
    rows.extend(ym_rows)
    rows.extend(rh_rows)

    write_csv(rows, out_csv)

    print(f"tweets returned: {len(tweets)}")
    print(f"YieldMax rows:   {len(ym_rows)}")
    print(f"Roundhill hits:  {len(rh_hits)} (OCR handled elsewhere)")
    print(f"Wrote {len(rows)} rows to {out_csv}")

if __name__ == "__main__":
    main()
