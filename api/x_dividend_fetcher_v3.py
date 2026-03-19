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
- OCR is performed on the full image with multiple passes and vertical tiles so lower rows are not missed.
- If OCR for an imaged tweet is uncertain, that tweet is rejected (fail closed) and the reason is logged.
- For tweets without images, text parsing is used.
"""

import os
import re
import csv
from io import BytesIO
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

import requests
import tweepy
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import pytesseract

NY = ZoneInfo("America/New_York")

# High-confidence patterns
RE_YM_TEXT_LINE = re.compile(r"\$?([A-Z]{2,8})\s*[–—-]\s*\$?\s*([0-9]+(?:\.[0-9]+)?)")
RE_YM_TABLE_PAIR = re.compile(r"\b([A-Z]{2,8})\b\s+\$?\s*([0-9]+\.[0-9]+)\b")

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
OCR_DEBUG = (os.getenv("DEBUG_YIELDMAX_OCR") or "1").strip() == "1"
REQUEST_TIMEOUT = float((os.getenv("YIELDMAX_IMG_TIMEOUT") or "20").strip())
AMOUNT_MIN = float((os.getenv("YIELDMAX_AMOUNT_MIN") or "0.0001").strip())
AMOUNT_MAX = float((os.getenv("YIELDMAX_AMOUNT_MAX") or "25.0").strip())
MAX_IMAGE_PER_TWEET = int((os.getenv("YIELDMAX_MAX_IMAGES") or "4").strip())
MAX_OCR_TILES = int((os.getenv("YIELDMAX_MAX_TILES") or "6").strip())


def dlog(msg: str):
    if OCR_DEBUG:
        print(f"YIELDMAX_DEBUG: {msg}")


def obj_get(obj, key: str, default=None):
    """Safely get field from dict-like tweepy objects or dataclass-like models."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)

    data = getattr(obj, "data", None)
    if isinstance(data, dict) and key in data:
        return data.get(key, default)

    if hasattr(obj, key):
        return getattr(obj, key, default)

    return default


def ny_now() -> datetime:
    return datetime.now(NY)


def parse_target_ny_date() -> date:
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


def preprocess_for_ocr(img: Image.Image, threshold: int) -> Image.Image:
    gray = ImageOps.grayscale(img)
    w, h = gray.size
    scale = 3
    gray = gray.resize((w * scale, h * scale), Image.Resampling.LANCZOS)
    gray = ImageEnhance.Contrast(gray).enhance(2.0)
    gray = gray.filter(ImageFilter.UnsharpMask(radius=1.5, percent=180, threshold=2))
    return gray.point(lambda p: 255 if p > threshold else 0)


def ocr_text(img: Image.Image, psm: int) -> str:
    config = f"--oem 3 --psm {psm}"
    return pytesseract.image_to_string(img, config=config)


def tiled_regions(img: Image.Image) -> list[Image.Image]:
    out = [img]
    w, h = img.size
    tile_count = max(1, min(MAX_OCR_TILES, 6))
    if tile_count == 1:
        return out

    tile_h = max(200, h // tile_count)
    overlap = max(40, tile_h // 6)

    y = 0
    while y < h:
        y2 = min(h, y + tile_h + overlap)
        out.append(img.crop((0, y, w, y2)))
        if y2 >= h:
            break
        y = max(y + tile_h - overlap, y + 1)

    return out


def extract_pairs_from_text(text: str) -> dict[str, float]:
    found: dict[str, float] = {}
    cleaned = (text or "").upper().replace("—", "-").replace("–", "-")

    for sym, amt in RE_YM_TEXT_LINE.findall(cleaned):
        try:
            val = float(amt)
        except Exception:
            continue
        if AMOUNT_MIN <= val <= AMOUNT_MAX:
            found[sym] = val

    for line in cleaned.splitlines():
        ln = re.sub(r"[^A-Z0-9\.$\-\s]", " ", line)
        for m in RE_YM_TABLE_PAIR.finditer(ln):
            sym = m.group(1).strip()
            amt_s = m.group(2).strip()
            try:
                val = float(amt_s)
            except Exception:
                continue
            if AMOUNT_MIN <= val <= AMOUNT_MAX:
                found[sym] = val

    return found


def choose_amount_by_votes(votes: dict[str, dict[float, int]]) -> tuple[dict[str, float], list[str]]:
    chosen: dict[str, float] = {}
    issues: list[str] = []

    for sym, amt_counts in votes.items():
        ranked = sorted(amt_counts.items(), key=lambda x: (-x[1], x[0]))
        if not ranked:
            continue
        if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
            issues.append(f"ambiguous_amount:{sym}={ranked[0][0]}|{ranked[1][0]}")
            continue
        chosen[sym] = ranked[0][0]

    return chosen, issues


def extract_pairs_from_image(content: bytes, tweet_id: int) -> tuple[dict[str, float], list[str]]:
    issues: list[str] = []
    votes: dict[str, dict[float, int]] = defaultdict(lambda: defaultdict(int))

    try:
        img = Image.open(BytesIO(content))
        img.load()
    except Exception as e:
        return {}, [f"image_decode_failed:{e}"]

    thresholds = [145, 160, 175]
    extra_t = os.getenv("YIELDMAX_THRESH", "").strip()
    if extra_t:
        try:
            tval = int(extra_t)
            if tval not in thresholds:
                thresholds.append(tval)
        except Exception:
            issues.append(f"invalid_threshold_env:{extra_t}")

    regions = tiled_regions(img)
    dlog(f"tweet={tweet_id} ocr_regions={len(regions)} thresholds={thresholds} psm=[6,11]")

    for r_idx, region in enumerate(regions, start=1):
        for threshold in thresholds:
            try:
                pre = preprocess_for_ocr(region, threshold)
            except Exception as e:
                issues.append(f"preprocess_failed:region{r_idx}:t{threshold}:{e}")
                continue

            for psm in (6, 11):
                try:
                    raw = ocr_text(pre, psm)
                except Exception as e:
                    issues.append(f"ocr_failed:region{r_idx}:t{threshold}:psm{psm}:{e}")
                    continue

                pairs = extract_pairs_from_text(raw)
                dlog(
                    f"tweet={tweet_id} region={r_idx}/{len(regions)} thresh={threshold} "
                    f"psm={psm} extracted_pairs={len(pairs)}"
                )
                for sym, amt in pairs.items():
                    votes[sym][round(float(amt), 6)] += 1

    chosen, vote_issues = choose_amount_by_votes(votes)
    issues.extend(vote_issues)
    return chosen, issues


def fetch_announcements(client: tweepy.Client, query: str, start_utc: datetime, end_utc: datetime):
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

    media_by_key = {}
    for m in (includes.get("media") or []):
        key = str(obj_get(m, "media_key", "") or "")
        if key:
            media_by_key[key] = m

    user_by_id = {}
    for u in (includes.get("users") or []):
        uid = str(obj_get(u, "id", "") or "")
        username = str(obj_get(u, "username", "") or "").lower()
        if uid and username:
            user_by_id[uid] = username

    dlog(
        f"fetch_announcements tweets={len(tweets)} media_includes={len(includes.get('media') or [])} "
        f"mapped_media={len(media_by_key)} users={len(user_by_id)}"
    )

    return tweets, media_by_key, user_by_id


def tweet_image_urls(t, media_by_key: dict) -> tuple[list[str], list[str], bool]:
    attachments = obj_get(t, "attachments", None)
    has_attachments = bool(attachments)

    if isinstance(attachments, dict):
        media_keys = [str(x) for x in (attachments.get("media_keys") or [])]
    else:
        media_keys = [str(x) for x in (obj_get(attachments, "media_keys", []) or [])]

    urls = []
    for mk in media_keys:
        m = media_by_key.get(str(mk))
        if not m:
            continue
        mtype = str(obj_get(m, "type", "") or "").lower()
        if mtype not in ("photo", "image"):
            continue
        url = obj_get(m, "url", None) or obj_get(m, "preview_image_url", None)
        if url:
            urls.append(force_orig(str(url)))

    return urls, media_keys, has_attachments


def parse_yieldmax_rows(tweets, media_by_key: dict, user_by_id: dict, target_day: date):
    rows = []
    seen_amount_by_ticker: dict[str, float] = {}
    rejected: list[str] = []

    for t in tweets:
        txt = obj_get(t, "text", "") or ""
        created_at = obj_get(t, "created_at", None)
        author_id = str(obj_get(t, "author_id", "") or "")
        tweet_id = str(obj_get(t, "id", "") or "")

        if not created_at:
            continue

        created_ny = created_at.astimezone(NY)
        if created_ny.date() != target_day:
            continue

        uname = user_by_id.get(author_id, "")
        if uname != "yieldmaxetfs":
            continue

        if looks_noise(txt):
            dlog(f"tweet={tweet_id} skipped=noise_text")
            continue

        text_pairs = extract_pairs_from_text(txt)
        img_urls, media_keys, has_attachments = tweet_image_urls(t, media_by_key)

        dlog(
            f"tweet={tweet_id} attachments_exist={has_attachments} media_keys={media_keys} "
            f"image_urls={len(img_urls)} text_pairs={len(text_pairs)}"
        )

        if img_urls:
            if not OCR_ENABLED:
                rejected.append(f"tweet {tweet_id}: has images but OCR disabled")
                dlog(f"tweet={tweet_id} ocr_branch_entered=no rejected=ocr_disabled")
                continue

            dlog(f"tweet={tweet_id} ocr_branch_entered=yes")

            vote_bank: dict[str, dict[float, int]] = defaultdict(lambda: defaultdict(int))
            uncertain = []

            for idx, url in enumerate(img_urls[:MAX_IMAGE_PER_TWEET], start=1):
                dlog(f"tweet={tweet_id} ocr_image_index={idx} url={url}")
                try:
                    content = download_image(url)
                except Exception as e:
                    uncertain.append(f"download_failed:{e}")
                    continue

                image_pairs, image_issues = extract_pairs_from_image(content, int(tweet_id or 0))
                uncertain.extend(image_issues)
                dlog(f"tweet={tweet_id} ocr_image_index={idx} ocr_pairs={len(image_pairs)}")

                for sym, amt in image_pairs.items():
                    vote_bank[sym][round(float(amt), 6)] += 1

            merged_pairs, merge_issues = choose_amount_by_votes(vote_bank)
            uncertain.extend(merge_issues)
            dlog(f"tweet={tweet_id} ocr_merged_pairs={len(merged_pairs)} uncertain_issues={len(uncertain)}")

            if not merged_pairs:
                reason = f"tweet {tweet_id}: OCR uncertain/no pairs from full image ({'; '.join(uncertain[:6]) or 'no_pairs'})"
                rejected.append(reason)
                dlog(f"tweet={tweet_id} result=rejected reason={reason}")
                continue

            if text_pairs and len(merged_pairs) < len(text_pairs):
                reason = (
                    f"tweet {tweet_id}: OCR pair count {len(merged_pairs)} < text pair count {len(text_pairs)} "
                    f"(possible partial read)"
                )
                rejected.append(reason)
                dlog(f"tweet={tweet_id} result=rejected reason={reason}")
                continue

            conflicts = []
            for sym, txt_amt in text_pairs.items():
                img_amt = merged_pairs.get(sym)
                if img_amt is not None and abs(float(img_amt) - float(txt_amt)) > 1e-9:
                    conflicts.append(f"{sym}:{txt_amt}!=img:{img_amt}")

            if conflicts:
                reason = f"tweet {tweet_id}: text/image mismatch ({'; '.join(conflicts[:6])})"
                rejected.append(reason)
                dlog(f"tweet={tweet_id} result=rejected reason={reason}")
                continue

            chosen_pairs = merged_pairs
            dlog(f"tweet={tweet_id} result=accepted source=ocr pairs={len(chosen_pairs)}")
        else:
            dlog(f"tweet={tweet_id} ocr_branch_entered=no reason=no_image_urls")
            if not text_pairs:
                reason = f"tweet {tweet_id}: no images and no parseable text pairs"
                rejected.append(reason)
                dlog(f"tweet={tweet_id} result=rejected reason={reason}")
                continue
            chosen_pairs = text_pairs
            dlog(f"tweet={tweet_id} result=accepted source=text pairs={len(chosen_pairs)}")

        for sym, amount in sorted(chosen_pairs.items()):
            prev = seen_amount_by_ticker.get(sym)
            if prev is not None and abs(float(prev) - float(amount)) > 1e-9:
                reason = f"tweet {tweet_id}: duplicate ticker conflict across tweets for {sym} ({prev} vs {amount})"
                rejected.append(reason)
                dlog(f"tweet={tweet_id} result=rejected reason={reason}")
                continue
            seen_amount_by_ticker[sym] = float(amount)

            rows.append(
                {
                    "ticker": sym,
                    "amount": float(amount),
                    "ex_date": "",
                    "pay_date": "",
                    "asof_date": target_day.isoformat(),
                    "source": f"@YieldMaxETFs tweet {tweet_id}",
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
        txt = obj_get(t, "text", "") or ""
        created_at = obj_get(t, "created_at", None)
        author_id = str(obj_get(t, "author_id", "") or "")

        if "distribution announcement" not in txt.lower():
            continue
        if not created_at:
            continue

        created_ny = created_at.astimezone(NY)
        if created_ny.date() != target_day:
            continue

        uname = user_by_id.get(author_id, "")
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
    tweets, media_by_key, user_by_id = fetch_announcements(client, query, start_utc, end_utc)

    ym_rows, ym_rejected = parse_yieldmax_rows(tweets, media_by_key, user_by_id, target_day)
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
