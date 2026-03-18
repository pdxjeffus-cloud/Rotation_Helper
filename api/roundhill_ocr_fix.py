#!/usr/bin/env python3
"""
Roundhill WeeklyPay image OCR -> data_dividends.csv (FAIL CLOSED)

Key behavior:
- Finds Roundhill "WeeklyPay ETFs Distribution Announcement" tweet for NY target date.
- Downloads highest-res image (forces name=orig when possible).
- Preprocesses image for OCR.
- Parses rows (Ticker + Amount).
- VALIDATES hard. If validation fails => writes 0 rows.
"""

import os
import re
import csv
import sys
import time
import tempfile
from datetime import datetime, timezone, timedelta, date

import requests
import tweepy

# OCR deps: pytesseract + pillow required (already used previously in your project)
from PIL import Image, ImageOps, ImageEnhance, ImageFilter
import pytesseract

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


NY = ZoneInfo("America/New_York") if ZoneInfo else None

ROUNDHILL_USER = os.getenv("ROUNDHILL_USER", "roundhill").strip() or "roundhill"

# These phrases appear in the tweet text around the image post.
NEEDLE_ANY = [
    "weeklypay",
    "distribution announcement",
    "etfs distribution",
    "distribution",
]

OUT_CSV = os.getenv("OUT_CSV", "data_dividends.csv").strip() or "data_dividends.csv"
ASOF_ENV = os.getenv("DIVIDEND_ASOF_DATE", "").strip()  # optional override, but you now keep it commented out
BANNER_DEBUG = os.getenv("DEBUG_ROUNDHILL", "0").strip() == "1"

# FAIL-CLOSED thresholds
MIN_ROWS = int(os.getenv("ROUNDHILL_MIN_ROWS", "16"))  # set to 18/20 if you want stricter
AMOUNT_MIN = float(os.getenv("ROUNDHILL_AMOUNT_MIN", "0.01"))
AMOUNT_MAX = float(os.getenv("ROUNDHILL_AMOUNT_MAX", "5.00"))

# Optional whitelist. If provided, only accept tickers in this set.
# Put a file path in ROUNDHILL_TICKERS_FILE (one ticker per line)
WHITELIST_FILE = os.getenv("ROUNDHILL_TICKERS_FILE", "").strip()


def ny_now() -> datetime:
    if not NY:
        return datetime.now()
    return datetime.now(NY)


def target_ny_date() -> date:
    if ASOF_ENV:
        try:
            return date.fromisoformat(ASOF_ENV)
        except Exception:
            pass
    return ny_now().date()


def safe_end_time_iso(end_dt_utc: datetime) -> str:
    """
    X API rejects end_time that is too close to "now". Must be >=10s in the past.
    We clamp to now-15s.
    """
    now_utc = datetime.now(timezone.utc)
    clamp = now_utc - timedelta(seconds=15)
    if end_dt_utc > clamp:
        end_dt_utc = clamp
    # X expects RFC3339/ISO-ish with Z
    return end_dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def start_time_iso(start_dt_utc: datetime) -> str:
    return start_dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_whitelist() -> set[str]:
    wl = set()
    if not WHITELIST_FILE:
        return wl
    try:
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip().upper()
                if not t or t.startswith("#"):
                    continue
                wl.add(t)
    except Exception:
        return set()
    return wl


def get_bearer() -> str:
    return (
        os.getenv("TWITTER_BEARER_TOKEN")
        or os.getenv("X_BEARER_TOKEN")
        or os.getenv("BEARER_TOKEN")
        or ""
    ).strip()


def looks_like_roundhill_post(text: str) -> bool:
    t = (text or "").lower()
    return any(n in t for n in NEEDLE_ANY)


def fetch_roundhill_candidate_tweet(client: tweepy.Client, target_day: date) -> tuple[tweepy.Tweet, list[str]] | None:
    """
    Search recent tweets from @roundhill for today (NY date).
    Use a safe NY-day window in UTC with end_time clamped to now-15s.
    """
    # NY midnight boundaries in UTC
    start_ny = datetime(target_day.year, target_day.month, target_day.day, 0, 0, 0, tzinfo=NY)
    end_ny = start_ny + timedelta(days=1)

    start_utc = start_ny.astimezone(timezone.utc)
    end_utc = end_ny.astimezone(timezone.utc)

    query = f"from:{ROUNDHILL_USER} -is:retweet"
    # ask for attachments/media keys
    resp = client.search_recent_tweets(
        query=query,
        max_results=25,
        tweet_fields=["created_at", "text", "attachments"],
        expansions=["attachments.media_keys"],
        media_fields=["url", "preview_image_url", "width", "height", "type"],
        start_time=start_time_iso(start_utc),
        end_time=safe_end_time_iso(end_utc),
    )

    tweets = list(resp.data or [])
    includes = resp.includes or {}
    media_by_key = {m["media_key"]: m for m in (includes.get("media") or [])}

    # Filter to correct NY date and looks like WeeklyPay distribution post and has image
    hits: list[tuple[tweepy.Tweet, list[str]]] = []
    for tw in tweets:
        created = tw.created_at
        if created is None:
            continue
        created_ny = created.astimezone(NY) if NY else created
        if created_ny.date() != target_day:
            continue
        # Roundhill distribution announcements are typically posted 6am-9:59am ET
        if not (6 <= created_ny.hour < 10):
            continue
        if not looks_like_roundhill_post(tw.text or ""):
            continue

        media_keys = []
        try:
            media_keys = (tw.attachments or {}).get("media_keys") or []
        except Exception:
            media_keys = []
        image_urls = []
        for mk in media_keys:
            m = media_by_key.get(mk)
            if not m:
                continue
            mtype = (m.get("type") or "").lower()
            if mtype not in ("photo", "image"):
                continue
            url = m.get("url") or m.get("preview_image_url")
            if url:
                image_urls.append(url)
        if image_urls:
            hits.append((tw, image_urls))

    if not hits:
        return None

    # Choose the earliest qualifying post in the morning window
    hits.sort(key=lambda x: x[0].created_at)
    tw, urls = hits[0]
    return tw, urls


def force_orig(url: str) -> str:
    """
    For X image URLs like:
      https://pbs.twimg.com/media/XYZ?format=jpg&name=small
    force name=orig to get highest-res.
    """
    if "pbs.twimg.com" not in url:
        return url
    if "name=" in url:
        return re.sub(r"name=[^&]+", "name=orig", url)
    # no name param: append it
    join = "&" if "?" in url else "?"
    return f"{url}{join}name=orig"


def download_image(url: str, timeout=20) -> bytes:
    headers = {"User-Agent": "RotationHelper/1.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content


def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """
    Aggressive preprocessing to stabilize OCR.
    """
    # convert to grayscale
    img = ImageOps.grayscale(img)

    # upscale
    w, h = img.size
    scale = 4
    img = img.resize((w * scale, h * scale), Image.Resampling.LANCZOS)

    # contrast
    img = ImageEnhance.Contrast(img).enhance(2.2)

    # sharpen
    img = img.filter(ImageFilter.UnsharpMask(radius=2, percent=200, threshold=2))

    # threshold (binarize)
    # Use a mid threshold; tweakable by env if needed
    thresh = int(os.getenv("ROUNDHILL_THRESH", "165"))
    img = img.point(lambda p: 255 if p > thresh else 0)

    return img


def ocr_text(img: Image.Image) -> str:
    # psm 6 assumes a block of text; tweakable
    config = "--oem 3 --psm 6"
    return pytesseract.image_to_string(img, config=config)


def parse_pairs(text: str) -> list[tuple[str, float]]:
    """
    Extract (TICKER, AMOUNT) from OCR text.
    We expect tickers like AAPW, AMDW, ... typically ending in W.
    Amounts like 0.131201 etc.

    We parse via regex scanning lines.
    """
    pairs: list[tuple[str, float]] = []

    # Normalize common OCR junk
    norm = text.replace(",", " ").replace("—", "-").replace("–", "-")
    lines = [ln.strip() for ln in norm.splitlines() if ln.strip()]

    # Common pattern in the table: "...  TICKER   0.123456"
    # We'll accept 3-5 letters + W, and a decimal number
    rx = re.compile(r"\b([A-Z]{3,5}W)\b.*?\b([0-9]+\.[0-9]+)\b")

    for ln in lines:
        ln_u = re.sub(r"[^A-Za-z0-9\.\s]", " ", ln).upper()
        m = rx.search(ln_u)
        if not m:
            continue
        ticker = m.group(1).strip()
        amt_s = m.group(2).strip()
        try:
            amt = float(amt_s)
        except Exception:
            continue
        pairs.append((ticker, amt))

    # Deduplicate tickers, keep last occurrence
    dedup: dict[str, float] = {}
    for t, a in pairs:
        dedup[t] = a

    out = sorted(dedup.items(), key=lambda x: x[0])
    return out


def validate_pairs(pairs: list[tuple[str, float]], whitelist: set[str]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if len(pairs) < MIN_ROWS:
        reasons.append(f"too_few_rows={len(pairs)} (min {MIN_ROWS})")

    bad = []
    for t, a in pairs:
        if not re.fullmatch(r"[A-Z]{3,5}W", t or ""):
            bad.append(f"bad_ticker:{t}")
        if not (AMOUNT_MIN <= a <= AMOUNT_MAX):
            bad.append(f"bad_amount:{t}={a}")
        if whitelist and t not in whitelist:
            bad.append(f"not_in_whitelist:{t}")

    if bad:
        # limit noise
        reasons.append("invalid_items=" + ",".join(bad[:12]) + ("..." if len(bad) > 12 else ""))

    return (len(reasons) == 0), reasons


def compute_ex_and_pay(asof_day: date) -> tuple[str, str]:
    """
    Your rule: ex_date = next trading day after announcement day, pay_date = day after ex_date.
    For now we keep the simple +3/+4 day mapping you were using for Friday->Monday ex-date etc?
    You’ve been running with ex/pay precomputed elsewhere; here we keep it consistent with your current output:
      ex_date: +3 days, pay_date: +4 days for Friday announcements
    But to avoid wrong rules, we’ll do a conservative default:
      ex_date = next calendar day, pay_date = next+1
    You can override with your engine later if needed.
    """
    # Conservative calendar-based placeholders; your dashboard logic can recompute.
    ex_d = asof_day + timedelta(days=1)
    pay_d = asof_day + timedelta(days=2)
    return ex_d.isoformat(), pay_d.isoformat()


def append_rows_to_csv(out_csv: str, rows: list[dict]) -> None:
    # Ensure header exists; if file missing create it
    exists = os.path.exists(out_csv)
    fieldnames = ["ticker", "amount", "ex_date", "pay_date", "asof_date", "source"]

    if not exists:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()

    # Append
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for r in rows:
            w.writerow(r)


def main() -> int:
    bearer = get_bearer()
    if not bearer:
        print("Missing TWITTER_BEARER_TOKEN (or X_BEARER_TOKEN/BEARER_TOKEN) in .env", file=sys.stderr)
        return 2

    wl = load_whitelist()
    td = target_ny_date()

    print(f"RH_TARGET_NY: {td.isoformat()}")

    client = tweepy.Client(bearer_token=bearer, wait_on_rate_limit=True)

    try:
        tw = fetch_roundhill_candidate_tweet(client, td)
    except tweepy.errors.BadRequest as e:
        # Usually end_time too close; tell caller to rerun (or fix clamp).
        print(f"Roundhill OCR: X API BadRequest: {e}", file=sys.stderr)
    try:
        result = fetch_roundhill_candidate_tweet(client, td)
    except Exception as e:
        print(f"Roundhill OCR: search failed: {e}", file=sys.stderr)
        return 4

    if not result:
        print("Roundhill OCR: no candidate tweet found for today.")
        return 0

    tw, urls = result

    created_ny = tw.created_at.astimezone(NY) if (tw.created_at and NY) else tw.created_at
    print(f"Roundhill OCR: tweet_id={tw.id} created_ny={created_ny.isoformat() if created_ny else 'unknown'}")

    if not urls:
        print("Roundhill OCR: candidate tweet had no image URLs.")
        return 0

    # prefer first image
    img_url = force_orig(urls[0])
    print(f"Roundhill OCR: images={len(urls)}")
    if BANNER_DEBUG:
        print(f"Roundhill OCR: image_url={img_url}")

    try:
        print("Roundhill OCR: downloading image...")
        content = download_image(img_url)
    except Exception as e:
        print(f"Roundhill OCR: download failed: {e}", file=sys.stderr)
        return 5

    try:
        img = Image.open(tempfile.SpooledTemporaryFile())
    except Exception:
        img = None  # not used

    # Load PIL image from bytes safely
    try:
        from io import BytesIO
        img = Image.open(BytesIO(content))
        img.load()
    except Exception as e:
        print(f"Roundhill OCR: image decode failed: {e}", file=sys.stderr)
        return 6

    try:
        prep = preprocess_for_ocr(img)
        print("Roundhill OCR: running OCR...")
        text = ocr_text(prep)
    except Exception as e:
        print(f"Roundhill OCR: OCR failed: {e}", file=sys.stderr)
        return 7

    pairs = parse_pairs(text)
    print(f"Roundhill OCR: parsed pairs={len(pairs)}")

    ok, reasons = validate_pairs(pairs, wl)
    if not ok:
        print("Roundhill OCR: FAILED_VALIDATION (fail-closed) -> writing 0 rows")
        for r in reasons:
            print(f"  - {r}")
        # optional: dump a debug snippet if needed
        if BANNER_DEBUG:
            print("---- OCR TEXT (first 800 chars) ----")
            print((text or "")[:800])
        return 0

    # Build rows
    ex_date, pay_date = compute_ex_and_pay(td)
    rows = []
    for ticker, amt in pairs:
        rows.append(
            {
                "ticker": ticker,
                "amount": f"{amt:.6f}".rstrip("0").rstrip("."),
                "ex_date": ex_date,
                "pay_date": pay_date,
                "asof_date": td.isoformat(),
                "source": f"@roundhill tweet {tw.id}",
            }
        )

    # Append to main CSV
    append_rows_to_csv(OUT_CSV, rows)

    print(f"Roundhill OCR: added {len(rows)} row(s) for NY date {td.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
