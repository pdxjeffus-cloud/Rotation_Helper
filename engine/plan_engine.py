from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from datetime import datetime
import csv
from pathlib import Path
from massive_dividends import get_dividend_csv_row

ENABLE_LAYERING = True

USE_MASSIVE = True  # flip to False for instant CSV fallback
_massive_cache = {}

# --- Watchlist support (safe add-on) ---
def _load_watchlist_csv(path):
    """
    Reads data_watchlist.csv with headers: day,ticker,issuer
    Returns dict: {"Mon":[{"ticker":"MSST","issuer":"Yieldmax"}, ...], ...}
    """
    out = {k: [] for k in ["Mon", "Tue", "Wed", "Thu", "Fri"]}
    if not path.exists():
        return out

    import csv
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            day = (row.get("day") or "").strip().title()[:3]
            ticker = (row.get("ticker") or "").strip().upper()
            issuer = (row.get("issuer") or "").strip()
            if day in out and ticker:
                out[day].append({"ticker": ticker, "issuer": issuer})
    return out


# --- Massive dividends (live) ---
USE_MASSIVE_BUYLIST = False  # only affects TODAY BUY LIST section
_massive_cache = {}

def _get_massive_row_cached(ticker: str):
    from massive_dividends import get_dividend_csv_row
    t = (ticker or "").strip().upper()
    if not t:
        return None
    if t in _massive_cache:
        return _massive_cache[t]
    row = get_dividend_csv_row(t)  # dict: symbol, dividend, ex_date, pay_date, record_date, declared_date
    _massive_cache[t] = row
    return row


def _get_massive_row(ticker: str):
    t = (ticker or "").strip().upper()
    if not t:
        return None
    if t in _massive_cache:
        return _massive_cache[t]
    row = get_dividend_csv_row(t)  # returns CSV-shaped dict
    _massive_cache[t] = row
    return row


def _safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _load_div_map(path: Path) -> dict[str, str]:
    """
    Reads data_dividends.csv if present.
    Tries to map:
      - div_map[TICKER] = dividend
      - div_map[TICKER + "_ex"] = ex date
      - div_map[TICKER + "_pay"] = pay date
    Works with various column spellings.
    """
    div_map: dict[str, str] = {}
    if not path.exists():
        return div_map

    with path.open("r", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            t = (r.get("ticker") or r.get("Ticker") or r.get("symbol") or r.get("Symbol") or "").strip().upper()
            if not t:
                continue

            div = r.get("dividend") or r.get("Dividend") or r.get("div") or r.get("Div") or ""
            exd = r.get("ex_dt") or r.get("ExDt") or r.get("ex_date") or r.get("ExDate") or r.get("ex") or ""
            pay = r.get("pay_dt") or r.get("PayDt") or r.get("pay_date") or r.get("PayDate") or r.get("pay") or ""

            if str(div).strip() != "":
                div_map[t] = str(div).strip()
            if str(exd).strip() != "":
                div_map[t + "_ex"] = str(exd).strip()
            if str(pay).strip() != "":
                div_map[t + "_pay"] = str(pay).strip()

    return div_map


def run_plan_engine() -> None:
    now = datetime.now()

    base = Path(__file__).resolve().parent.parent  # Rotation_Helper/
    positions_path = base / "data_positions.csv"
    dividends_path = base / "data_dividends.csv"

    div_map = _load_div_map(dividends_path)

    print("\n" + "=" * 88)
    print("DIVIDEND ROTATION — DAILY SELL SHEET (v1)")
    print(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 88)

    print("\nOPEN POSITIONS")
    print("-" * 88)
    print(
        f"{'Ticker':<8} {'Issuer':<10} {'Class':<16} "
        f"{'Shares':>7} {'Buy':>8} {'Div':>6} {'ExDt':>10} {'PayDt':>10} "
        f"{'Forced':>7} {'Exec':>7} {'Exp':>8} {'Acc':>8} "
        f"{'L1':>8} {'L2':>8} {'L3':>8} {'Plan':<22} Reason / Notes"

    )
    print("-" * 88)

    if not positions_path.exists():
        print(f"(missing file) {positions_path}")
        return

    with positions_path.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            ticker = (row.get("ticker") or row.get("Ticker") or "").strip().upper()
            if not ticker:
                continue

            issuer = (row.get("issuer") or row.get("Issuer") or "").strip()
            trade_class = (row.get("trade_class") or row.get("Class") or row.get("class") or "").strip()
            notes = (row.get("notes") or row.get("Notes") or "").strip()

            shares = _safe_int(row.get("shares") or row.get("Shares"), 0)
            buy_price = _safe_float(row.get("buy_price") or row.get("Buy") or row.get("buy"), 0.0)
            dividend = _safe_float(row.get("dividend") or row.get("Div") or row.get("div"), 0.0)

            # --- Massive dividend override (live) ---
            if USE_MASSIVE:
                m = _get_massive_row(ticker)
                if m and m.get("dividend") is not None:
                    dividend = _safe_float(m.get("dividend"), 0.0)
                    ex_dt = m.get("ex_date") or ex_dt
                    pay_dt = m.get("pay_date") or pay_dt
            # --- end Massive override ---


            # override dividend/ex/pay if provided by data_dividends.csv
            d2 = _safe_float(div_map.get(ticker, ""), 0.0)
            if d2 > 0:
                dividend = d2

            ex_dt = div_map.get(ticker + "_ex", row.get("ex_dt") or row.get("ExDt") or "") or ""
            pay_dt = div_map.get(ticker + "_pay", row.get("pay_dt") or row.get("PayDt") or "") or ""

            tc = trade_class.upper()

            # --- decay assumption by trade class ---
            if "CORE INCOME" in tc or "CORE" in tc:
                decay_pct = 0.30
            elif "TREND" in tc:
                decay_pct = 0.55
            elif "HIGH" in tc:
                decay_pct = 0.80
            else:
                decay_pct = 0.60  # default

            expected_exit = buy_price - (dividend * decay_pct)

            # --- forced exit rules (simple placeholder) ---
            # If your positions CSV has day_count like EX-DAY / DAY+1, we can trigger forced exits.
            day_count = (row.get("day_count") or row.get("DayCount") or "").strip().upper()
            forced_exit = "NO"
            reason = notes

            if day_count in {"EX-DAY", "DAY+1", "DAY1"}:
                forced_exit = "YES"
                if reason:
                    reason = f"{reason} | "
                reason += "Auto: forced exit (day_count rule)"

            day_flag = (day_count or "").strip().lower()

            if day_flag == "buy-day":
                forced_exit = "NO"
                reason = "Buy day (planning)"

            elif day_flag == "ex-day":
                forced_exit = "YES"
                reason = "Ex-day rule: must be flat today"

            elif day_flag in ("day+1", "day1", "day1+"):
                forced_exit = "YES"
                reason = "Day+1 after ex-date (NAV decay protection)"


            print(f"{'Ticker':<8} {'Issuer':<10} {'Div':>9} {'ExDt':>10} {'PayDt':>10} {'Declared':>10}")


            # --- execution mode ---
            # YieldMax: often layered; Roundhill: typically single (you can tune later)
            issuer_u = issuer.upper()
            execution_mode = "LAYERED" if ("YIELDMAX" in issuer_u) else "SINGLE"

            # --- acceptable exit buffer ---
            buffer_amt = 0.05 if forced_exit == "YES" else 0.03
            acceptable_exit = expected_exit - buffer_amt

            # --- layering targets ---
            layer_1 = layer_2 = layer_3 = ""
            plan = "100%@Acc"

            if ENABLE_LAYERING and execution_mode == "LAYERED":
                step = round(max(0.05, dividend * 0.25), 2)
                l1 = round(acceptable_exit, 2)
                l2 = round(acceptable_exit + step, 2)
                l3 = round(acceptable_exit + (2 * step), 2)
                layer_1 = f"{l1:.2f}"
                layer_2 = f"{l2:.2f}"
                layer_3 = f"{l3:.2f}"
                plan = "40%@L1 30%@L2 30%@L3"

            print(
                 f"{ticker:<8} {issuer:<10} {trade_class:<16} "
                    f"{shares:>7d} {buy_price:>8.2f} {dividend:>6.3f} {str(ex_dt):>10} {str(pay_dt):>10} "
                    f"{forced_exit:>7} {execution_mode:>7} {expected_exit:>8.2f} {acceptable_exit:>8.2f} "
                    f"{layer_1:>8} {layer_2:>8} {layer_3:>8} {plan:<22} {reason}"
            )

    print("-" * 88)

    # ============================================================
    # TODAY BUY LIST (from watchlist + Massive)
    # ============================================================

    watchlist_path = base / "data_watchlist.csv"
    wl = _load_watchlist_csv(watchlist_path)

    today = datetime.now().strftime("%a")  # Mon/Tue/Wed/Thu/Fri
    today_list = wl.get(today, [])

    print("\n" + "=" * 88)
    print(f"TODAY BUY LIST ({today}) — Massive dividends")
    print(f"Source: {watchlist_path.name}")
    print("-" * 88)

    if not today_list:
        print("No tickers found for today in data_watchlist.csv")
    else:
        print(f"{'Ticker':<8} {'Issuer':<10} {'Div':>9} {'ExDt':>10} {'PayDt':>10} {'Declared':>10}")
        print("-" * 88)

        for item in today_list:
            ticker = (item.get("ticker") or "").strip().upper()
            issuer = (item.get("issuer") or "").strip()
            if not ticker:
                continue

            # local first (no API)
            row = div_map.get(ticker)

            # only call Massive/Polygon if missing locally
            if row is None and USE_MASSIVE_BUYLIST:
                row = _get_massive_row(ticker)

            # OPTION A: skip tickers with no data
            if not row:
                continue

            div = _safe_float(row.get("div") or row.get("Div") or row.get("dividend"))
            ex_dt = (row.get("ex_date") or row.get("ExDt") or row.get("exDate") or "-")
            pay_dt = (row.get("pay_date") or row.get("PayDt") or row.get("payDate") or "-")
            decl_dt = (row.get("declared") or row.get("Declared") or row.get("declaration_date") or "-")

            print(f"{ticker:<8} {issuer:<10} {div:>9.6f} {str(ex_dt):>10} {str(pay_dt):>10} {str(decl_dt):>10}")


    print("Legend:")
    print("  Exp = expected exit based on dividend decay")
    print("  Acc = acceptable exit (Exp minus buffer)")
    print("  L1/L2/L3 used only when Exec=LAYERED")
if __name__ == "__main__":
    run_plan_engine()
