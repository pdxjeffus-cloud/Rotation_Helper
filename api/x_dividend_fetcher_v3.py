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
import html
from io import BytesIO
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

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


def get_media_keys_and_attachment_flag(t) -> tuple[list[str], bool]:
    keys: list[str] = []
    attachments_exist = False
    try:
        attachments = getattr(t, "attachments", None) or {}
        attachments_exist = bool(attachments)
        if isinstance(attachments, dict):
            keys = attachments.get("media_keys") or []
        else:
            keys = getattr(attachments, "media_keys", None) or []
    except Exception:
        keys = []
        attachments_exist = False
    return list(keys), attachments_exist


def tweet_image_urls(t, media_by_key: dict, base_tweet_id: str | None = None) -> list[str]:
    keys, attachments_exist = get_media_keys_and_attachment_flag(t)
    log_tweet_id = base_tweet_id or str(getattr(t, "id", ""))

    print(
        "YIELDMAX_DEBUG "
        f"tweet_id={log_tweet_id} "
        f"ocr_tweet_id={t.id} "
        f"attachments_exist={attachments_exist} "
        f"media_keys={keys} "
        f"includes_media_count={len(media_by_key)}"
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

    print(
        "YIELDMAX_DEBUG "
        f"tweet_id={log_tweet_id} "
        f"ocr_tweet_id={t.id} "
        f"image_urls_count={len(out)}"
    )
    return out


def parse_yieldmax_rows(tweets, media_by_key: dict, user_by_id: dict, tweet_by_id: dict, target_day: date):
    """
    YieldMax parser with fail-closed image OCR.

    Rules:
    - Strict NY-date filtering.
    - Only @YieldMaxETFs tweets are parsed.
    - If tweet has image(s), OCR output is required and used as source-of-truth for that tweet.
    - If OCR is uncertain (none parsed, conflicts), reject that tweet and explain.
    """
    def _tweet_urls(tw) -> list[str]:
        out = []
        entities = getattr(tw, "entities", None) or {}
        if isinstance(entities, dict):
            urls = entities.get("urls") or []
        else:
            urls = getattr(entities, "urls", None) or []
        for u in urls:
            if not isinstance(u, dict):
                continue
            for k in ("expanded_url", "unwound_url", "url"):
                v = (u.get(k) or "").strip()
                if v:
                    out.append(v)
        return out

    def _resolve_url(url: str) -> str:
        headers = {"User-Agent": "RotationHelper/1.0"}
        try:
            resp = requests.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers)
            final = (resp.url or "").strip()
            if final:
                return final
        except Exception:
            pass
        try:
            resp = requests.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT, headers=headers)
            final = (resp.url or "").strip()
            if final:
                return final
        except Exception:
            pass
        return url

    def _is_globenewswire(url: str) -> bool:
        try:
            host = (urlparse(url).netloc or "").lower()
        except Exception:
            host = ""
        return "globenewswire.com" in host

    def _html_to_text(blob: str) -> str:
        txt = re.sub(r"(?is)<script\b.*?</script>", " ", blob or "")
        txt = re.sub(r"(?is)<style\b.*?</style>", " ", txt)
        txt = re.sub(r"(?is)<[^>]+>", " ", txt)
        txt = html.unescape(txt)
        txt = re.sub(r"\s+", " ", txt)
        return txt.strip()

    def _extract_pairs_with_conflict(text: str) -> tuple[dict[str, float], bool]:
        found: dict[str, float] = {}
        conflict = False
        cleaned = (text or "").upper().replace("—", "-").replace("–", "-")

        candidates = []
        for m in RE_YM_TEXT_LINE.finditer(cleaned):
            candidates.append((m.group(1), m.group(2)))
        for m in RE_YM_GENERIC_PAIR.finditer(cleaned):
            candidates.append((m.group(1), m.group(2)))

        for sym, amt in candidates:
            try:
                val = float(amt)
            except Exception:
                continue
            if not (AMOUNT_MIN <= val <= AMOUNT_MAX):
                continue
            prev = found.get(sym)
            if prev is not None and abs(prev - val) > 1e-9:
                conflict = True
                break
            found[sym] = val
        return found, conflict

    def _parse_globenewswire(url: str) -> tuple[dict[str, float], bool]:
        headers = {"User-Agent": "RotationHelper/1.0"}
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        resp.raise_for_status()
        html_blob = resp.text or ""

        blocks = []
        for pattern in (
            r'(?is)"articleBody"\s*:\s*"([^"]+)"',
            r'(?is)<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            r'(?is)<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
            r'(?is)<article\b.*?</article>',
            r'(?is)<main\b.*?</main>',
            r'(?is)<body\b.*?</body>',
        ):
            blocks.extend(re.findall(pattern, html_blob))
        text = "\n".join(_html_to_text(b) for b in blocks if b)
        pairs, conflict = _extract_pairs_with_conflict(text)
        return pairs, conflict

    rows = []
    seen = {}
    rejected = []
    article_used = False

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

        tweet_stamp = created_ny.isoformat()
        chosen_pairs: dict[str, float] = {}
        chosen_source = f"@YieldMaxETFs tweet {t.id} at {tweet_stamp}"

        # Preferred path: parse linked GlobeNewswire article when available.
        resolved_urls = []
        for raw_url in _tweet_urls(t):
            resolved = _resolve_url(raw_url)
            if resolved and resolved not in resolved_urls:
                resolved_urls.append(resolved)
        globe_urls = [u for u in resolved_urls if _is_globenewswire(u)]

        article_parsed = False
        article_rejected = False
        for aurl in globe_urls:
            try:
                article_pairs, article_conflict = _parse_globenewswire(aurl)
            except Exception as e:
                print(f"YIELDMAX_DEBUG tweet_id={t.id} article_url={aurl} article_error={e}")
                continue

            article_parsed = True
            if article_conflict:
                reason = f"tweet {t.id}: ambiguous article rows in {aurl}"
                print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
                rejected.append(reason)
                article_rejected = True
                break
            if not article_pairs:
                print(f"YIELDMAX_DEBUG tweet_id={t.id} article_url={aurl} article_pairs=0")
                continue

            chosen_pairs = article_pairs
            chosen_source = f"GlobeNewswire article {aurl} via tweet {t.id} at {tweet_stamp}"
            article_used = True
            print(f"YIELDMAX_DEBUG tweet_id={t.id} accepted_reason=article_pairs_used count={len(chosen_pairs)}")
            break

        if article_rejected:
            continue

        if not chosen_pairs:
            if article_parsed:
                print(f"YIELDMAX_DEBUG tweet_id={t.id} article_fallback=enabled reason=no_rows")
            text_pairs = extract_pairs_from_text(txt)
            img_urls = tweet_image_urls(t, media_by_key, base_tweet_id=str(t.id))
            has_images = bool(img_urls)
            print(f"YIELDMAX_DEBUG tweet_id={t.id} ocr_branch_entered={'yes' if has_images else 'no'}")

            if has_images:
                if not OCR_ENABLED:
                    reason = f"tweet {t.id}: has image(s) but OCR disabled"
                    print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
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
                        print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
                        rejected.append(reason)
                        ocr_conflict = True
                        break

                    print(f"YIELDMAX_DEBUG tweet_id={t.id} ocr_image_index={checked} ocr_pair_count={len(pairs)}")
                    if OCR_DEBUG:
                        print(f"YIELDMAX OCR tweet={t.id} image={checked} pairs={len(pairs)}")

                    for sym, val in pairs.items():
                        prev = ocr_pairs_agg.get(sym)
                        if prev is not None and abs(prev - val) > 1e-9:
                            reason = f"tweet {t.id}: OCR conflict for {sym} ({prev} vs {val})"
                            print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
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
                    print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
                    rejected.append(reason)
                    continue

                conflict = []
                for sym, txt_val in text_pairs.items():
                    img_val = ocr_pairs_agg.get(sym)
                    if img_val is not None and abs(img_val - txt_val) > 1e-9:
                        conflict.append(f"{sym}:{txt_val}!=img:{img_val}")

                if conflict:
                    reason = f"tweet {t.id}: text/image mismatch ({';'.join(conflict[:5])})"
                    print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
                    rejected.append(reason)
                    continue

                chosen_pairs = ocr_pairs_agg
                print(f"YIELDMAX_DEBUG tweet_id={t.id} accepted_reason=ocr_pairs_used count={len(chosen_pairs)}")
            else:
                if not text_pairs:
                    reason = f"tweet {t.id}: no image and no parseable text pairs"
                    print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
                    rejected.append(reason)
                    continue
                chosen_pairs = text_pairs
                print(f"YIELDMAX_DEBUG tweet_id={t.id} accepted_reason=text_pairs_used count={len(chosen_pairs)}")

        for sym, amount in sorted(chosen_pairs.items()):
            prev_amt = seen.get(sym)
            if prev_amt is not None and abs(prev_amt - amount) > 1e-9:
                reason = f"tweet {t.id}: duplicate ticker conflict across tweets for {sym} ({prev_amt} vs {amount})"
                print(f"YIELDMAX_DEBUG tweet_id={t.id} rejected_reason={reason}")
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
                    "source": chosen_source,
                }
            )

    return rows, rejected, article_used


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

    ym_rows, ym_rejected, article_used = parse_yieldmax_rows(tweets, media_by_key, user_by_id, tweet_by_id, target_day)
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
    print(f"SOURCE article_used: {'yes' if article_used else 'no'}")
    print(f"ROW_COUNTS yieldmax={len(ym_rows)} roundhill={len(rh_rows)} total={len(rows)}")
    print(f"CSV_WRITTEN: {os.path.abspath(out_csv)}")
    print(f"Wrote {len(rows)} rows to {out_csv}")


if __name__ == "__main__":
    main()
