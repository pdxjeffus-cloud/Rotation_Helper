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
- If a qualifying YieldMax tweet has images, image OCR is treated as the source of truth for that tweet.
- If OCR for an imaged tweet is uncertain, that tweet is rejected (fail closed) and the reason is logged.
- For tweets without images, text parsing is used.
"""

import os
import re
import csv
from io import BytesIO
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

import requests
import tweepy
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import pytesseract

NY = ZoneInfo("America/New_York")

# YieldMax line examples:
# $ABNY – $0.2495
# ABNY - 0.2495
RE_YM_TEXT_LINE = re.compile(r"\$?([A-Z]{2,8})\s*[–—-]\s*\$?\s*([0-9]+(?:\.[0-9]+)?)")
RE_YM_GENERIC_PAIR = re.compile(r"\b([A-Z]{2,8})\b[^0-9\n]{0,14}\$?\s*([0-9]+\.[0-9]+)\b")

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

OCR_ENABLED = (os.getenv("YIELDMAX_OCR_ENABLED") or "1").strip() != "0"
OCR_DEBUG = (os.getenv("DEBUG_YIELDMAX_OCR") or "0").strip() == "1"
REQUEST_TIMEOUT = float((os.getenv("YIELDMAX_IMG_TIMEOUT") or "20").strip())
AMOUNT_MIN = float((os.getenv("YIELDMAX_AMOUNT_MIN") or "0.0001").strip())
AMOUNT_MAX = float((os.getenv("YIELDMAX_AMOUNT_MAX") or "25.0").strip())
MAX_IMAGE_PER_TWEET = int((os.getenv("YIELDMAX_MAX_IMAGES") or "4").strip())


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


def force_orig(url: str) -> str:
    if "pbs.twimg.com" not in url:
        return url
    if "name=" in url:
        return re.sub(r"name=[^&]+", "name=orig", url)
    join = "&" if "?" in url else "?"
    return f"{url}{join}name=orig"


def download_image(url: str) -> bytes:
    headers = {"User-Agent": "RotationHelper/1.0"}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    img = ImageOps.grayscale(img)
    w, h = img.size
    scale = 3
    img = img.resize((w * scale, h * scale), Image.Resampling.LANCZOS)
    img = ImageEnhance.Contrast(img).enhance(2.0)
    img = img.filter(ImageFilter.UnsharpMask(radius=1.5, percent=180, threshold=2))
    thresh = int((os.getenv("YIELDMAX_THRESH") or "165").strip())
    return img.point(lambda p: 255 if p > thresh else 0)


def ocr_text(img: Image.Image) -> str:
    config = "--oem 3 --psm 6"
    return pytesseract.image_to_string(img, config=config)


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

    # Fallback generic extraction from OCR-like lines.
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
        tweet_fields=["created_at", "text", "author_id", "attachments"],
        expansions=["attachments.media_keys", "author_id"],
        media_fields=["media_key", "url", "preview_image_url", "type"],
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

    return tweets, media_by_key, user_by_id, len(media)


def tweet_image_urls(t, media_by_key: dict, includes_media_count: int) -> list[str]:
    keys: list[str] = []
    attachments_exist = False
    attachments_raw = None
    try:
        attachments_raw = getattr(t, "attachments", None)
        attachments = attachments_raw or {}
        attachments_exist = bool(attachments)
        if isinstance(attachments, dict):
            keys = attachments.get("media_keys") or []
        else:
            keys = getattr(attachments, "media_keys", None) or []
    except Exception:
        keys = []
        attachments_exist = False

    print(
        "YIELDMAX_DEBUG "
        f"tweet={t.id} "
        f"raw_attachments={attachments_raw} "
        f"attachments_exist={attachments_exist} "
        f"media_keys={keys} "
        f"includes_media_count={includes_media_count}"
    )

    out = []
    for mk in keys:
        m = media_by_key.get(mk)
        if not m:
            continue
        mtype = (
            getattr(m, "type", None)
            or (m.get("type") if isinstance(m, dict) else None)
            or ""
        ).lower()
        if mtype not in ("photo", "image"):
            continue
        url = (
            getattr(m, "url", None)
            or getattr(m, "preview_image_url", None)
            or (m.get("url") if isinstance(m, dict) else None)
            or (m.get("preview_image_url") if isinstance(m, dict) else None)
        )
        if url:
            out.append(force_orig(url))

    print(f"YIELDMAX_DEBUG tweet={t.id} image_urls_count={len(out)}")
    return out


def parse_yieldmax_rows(tweets, media_by_key: dict, user_by_id: dict, includes_media_count: int, target_day: date):
    """
    YieldMax parser with fail-closed image OCR.

    Rules:
    - Strict NY-date filtering.
    - Only @YieldMaxETFs tweets are parsed.
    - If tweet has image(s), OCR output is required and used as source-of-truth for that tweet.
    - If OCR is uncertain (none parsed, conflicts), reject that tweet and explain.
    """
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

        text_pairs = extract_pairs_from_text(txt)
        img_urls = tweet_image_urls(t, media_by_key, includes_media_count)
        has_images = bool(img_urls)
        print(f"YIELDMAX_DEBUG tweet={t.id} ocr_branch_entered={'yes' if has_images else 'no'}")

        chosen_pairs: dict[str, float] = {}

        if has_images:
            if not OCR_ENABLED:
                reason = f"tweet {t.id}: has image(s) but OCR disabled"
                print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                rejected.append(reason)
                continue

            ocr_pairs_agg: dict[str, float] = {}
            ocr_conflict = False
            checked = 0

            for url in img_urls[:MAX_IMAGE_PER_TWEET]:
                checked += 1
                try:
                    content = download_image(url)
                    img = Image.open(BytesIO(content))
                    img.load()
                    prep = preprocess_for_ocr(img)
                    raw = ocr_text(prep)
                    pairs = extract_pairs_from_text(raw)
                except Exception as e:
                    reason = f"tweet {t.id}: OCR image failed ({e})"
                    print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                    rejected.append(reason)
                    ocr_conflict = True
                    break

                print(f"YIELDMAX_DEBUG tweet={t.id} ocr_image_index={checked} ocr_pair_count={len(pairs)}")
                if OCR_DEBUG:
                    print(f"YIELDMAX OCR tweet={t.id} image={checked} pairs={len(pairs)}")

                for sym, val in pairs.items():
                    prev = ocr_pairs_agg.get(sym)
                    if prev is not None and abs(prev - val) > 1e-9:
                        reason = f"tweet {t.id}: OCR conflict for {sym} ({prev} vs {val})"
                        print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                        rejected.append(reason)
                        ocr_conflict = True
                        break
                    ocr_pairs_agg[sym] = val

                if ocr_conflict:
                    break

            if ocr_conflict:
                continue

            if not ocr_pairs_agg:
                reason = f"tweet {t.id}: has image(s) but OCR found no ticker/amount pairs"
                print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                rejected.append(reason)
                continue

            # Cross-check text pairs only for conflicts. Missing in text is allowed.
            conflict = []
            for sym, txt_val in text_pairs.items():
                img_val = ocr_pairs_agg.get(sym)
                if img_val is not None and abs(img_val - txt_val) > 1e-9:
                    conflict.append(f"{sym}:{txt_val}!=img:{img_val}")

            if conflict:
                reason = f"tweet {t.id}: text/image mismatch ({';'.join(conflict[:5])})"
                print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                rejected.append(reason)
                continue

            chosen_pairs = ocr_pairs_agg
            print(f"YIELDMAX_DEBUG tweet={t.id} accepted_reason=ocr_pairs_used count={len(chosen_pairs)}")
        else:
            if not text_pairs:
                reason = f"tweet {t.id}: no image and no parseable text pairs"
                print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
                rejected.append(reason)
                continue
            chosen_pairs = text_pairs
            print(f"YIELDMAX_DEBUG tweet={t.id} accepted_reason=text_pairs_used count={len(chosen_pairs)}")

        for sym, amount in sorted(chosen_pairs.items()):
            prev_amt = seen.get(sym)
            if prev_amt is not None and abs(prev_amt - amount) > 1e-9:
                reason = f"tweet {t.id}: duplicate ticker conflict across tweets for {sym} ({prev_amt} vs {amount})"
                print(f"YIELDMAX_DEBUG tweet={t.id} rejected_reason={reason}")
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
                    "source": f"@YieldMaxETFs tweet {t.id}",
                }
            )

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

    tweets, media_by_key, user_by_id, includes_media_count = fetch_announcements(
        client,
        query=query,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    ym_rows, ym_rejected = parse_yieldmax_rows(tweets, media_by_key, user_by_id, includes_media_count, target_day)
    rh_rows, rh_hits = parse_roundhill_stub_rows(tweets, user_by_id, target_day)

    rows = []
    rows.extend(ym_rows)
    rows.extend(rh_rows)

    write_csv(rows, out_csv)

    print(f"tweets returned: {len(tweets)}")
    print(f"YieldMax rows:   {len(ym_rows)}")
    if ym_rejected:
        print(f"YieldMax rejected: {len(ym_rejected)} (fail-closed)")
        for msg in ym_rejected:
            print(f"  - {msg}")
    print(f"Roundhill hits:  {len(rh_hits)} (OCR handled elsewhere)")
    print(f"Wrote {len(rows)} rows to {out_csv}")


if __name__ == "__main__":
    main()
