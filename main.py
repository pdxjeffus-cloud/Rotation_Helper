from __future__ import annotations

from datetime import date

import csv
import os
from datetime import datetime


from signals.rules import Config, Rotation, generate_signals
from signals.utils import print_signals

from clients.etrade_live import get_live_quotes
from clients.massive_dividends import get_dividends

import pandas as pd

# Load watchlist and fresh dividends
watchlist = pd.read_csv('data_watchlist.csv')
dividends = pd.read_csv('data_dividends.csv', index_col='ticker') if os.path.exists('data_dividends.csv') else pd.DataFrame()

print(f"Loaded {len(dividends)} fresh dividend announcements")


def load_watchlist_tickers(day_name: str, path: str = "data_watchlist.csv") -> list[str]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))

    tickers = [
        (r.get("ticker") or "").strip().upper()
        for r in rows
        if (r.get("day") or "").strip().lower() == day_name.strip().lower()
    ]

    # de-dupe, preserve order
    seen = set()
    ordered = []
    for t in tickers:
        if t and t not in seen:
            seen.add(t)
            ordered.append(t)

    return ordered



def build_sample_config() -> Config:
    # Tier mapping – we’ll refine as we add more history
    tier = {
        # Mon buy, ex Tue, pay Wed
        "WPAY": "B",
        "MSST": "B",
        "NVIT": "B",
        "TEST": "C",

        # Tue buy, ex Wed, pay Thu
        "CHPY": "B",
        "FEAT": "B",
        "FIVY": "B",
        "GPTY": "B",
        "LFGY": "B",
        "QDTY": "B",
        "RDTY": "B",
        "SDTY": "B",
        "SLTY": "B",
        "ULTY": "B",
        "YBTC": "C",
        "YETH": "A",

        # Wed buy, ex Thu, pay Fri
        "GMEY": "B",
        "RDYY": "B",
        "HIYY": "B",
        "PLTY": "B",
        "CVNY": "B",
        "CONY": "B",
        "HOOY": "B",
        "CRCO": "B",
        "SMCY": "B",
        "RBLY": "B",
        "XYZY": "B",
        "GDXY": "B",
        "YBIT": "B",
        "DRAY": "B",
        "DIPS": "B",

        # Thu buy, ex Fri (MAGY special)
        "MAGY": "C",

        # Fri buy, ex Mon, pay Tue (Roundhill “W” group etc.)
        "COIW": "B",
        "HOOW": "A",
        "AMDW": "B",
        "PLTW": "B",
        "AVGW": "B",
        "ARMW": "B",
        "TSLW": "B",
        "GDXW": "B",
        "BABW": "B",
        "GOOW": "A",
        "METW": "B",
        "UBEW": "B",
        "AMZW": "B",
        "NVDW": "B",
        "GLDW": "B",
    }

    # Rotation mapping by ticker
    rotation_map = {
        # Mon buy, ex Tue, pay Wed
        "WPAY": Rotation.MON_TUE,
        "MSST": Rotation.MON_TUE,
        "NVIT": Rotation.MON_TUE,
        "TEST": Rotation.MON_TUE,

        # Tue buy, ex Wed, pay Thu
        "CHPY": Rotation.TUE_WED,
        "FEAT": Rotation.TUE_WED,
        "FIVY": Rotation.TUE_WED,
        "GPTY": Rotation.TUE_WED,
        "LFGY": Rotation.TUE_WED,
        "QDTY": Rotation.TUE_WED,
        "RDTY": Rotation.TUE_WED,
        "SDTY": Rotation.TUE_WED,
        "SLTY": Rotation.TUE_WED,
        "ULTY": Rotation.TUE_WED,
        "YBTC": Rotation.TUE_WED,
        "YETH": Rotation.TUE_WED,

        # Wed buy, ex Thu, pay Fri
        "GMEY": Rotation.WED_THU,
        "RDYY": Rotation.WED_THU,
        "HIYY": Rotation.WED_THU,
        "PLTY": Rotation.WED_THU,
        "CVNY": Rotation.WED_THU,
        "CONY": Rotation.WED_THU,
        "HOOY": Rotation.WED_THU,
        "CRCO": Rotation.WED_THU,
        "SMCY": Rotation.WED_THU,
        "RBLY": Rotation.WED_THU,
        "XYZY": Rotation.WED_THU,
        "GDXY": Rotation.WED_THU,
        "YBIT": Rotation.WED_THU,
        "DRAY": Rotation.WED_THU,
        "DIPS": Rotation.WED_THU,

        # Thu buy, ex Fri (MAGY special)
        "MAGY": Rotation.THU_FRI,

        # Fri buy, ex Mon, pay Tue
        "COIW": Rotation.FRI_MON,
        "HOOW": Rotation.FRI_MON,
        "AMDW": Rotation.FRI_MON,
        "PLTW": Rotation.FRI_MON,
        "AVGW": Rotation.FRI_MON,
        "ARMW": Rotation.FRI_MON,
        "TSLW": Rotation.FRI_MON,
        "GDXW": Rotation.FRI_MON,
        "BABW": Rotation.FRI_MON,
        "GOOW": Rotation.FRI_MON,
        "METW": Rotation.FRI_MON,
        "UBEW": Rotation.FRI_MON,
        "AMZW": Rotation.FRI_MON,
        "NVDW": Rotation.FRI_MON,
        "GLDW": Rotation.FRI_MON,
    }

    return Config(
        min_yield=0.01,          # 1% minimum yield for BUY
        max_spread_bps=20,       # >20 bps = SKIP
        max_spread_pct_div=0.25, # >25% of dividend = SKIP
        tier=tier,
        rotation_map=rotation_map,
    )


def main() -> None:
    # For our WPAY test:
    # ex-date is Tuesday 2025-12-08, so buy day is Monday 2025-12-01.
    today = date(2025, 12, 5)

    # Tier mapping – we’ll refine as we add more history
    tier = {
        # Mon buy, ex Tue, pay Wed
        "WPAY": "B",
        "MSST": "B",
        "NVIT": "B",
        "TEST": "C",

        # Tue buy, ex Wed, pay Thu
        "CHPY": "B",
        "FEAT": "B",
        "FIVY": "B",
        "GPTY": "B",
        "LFGY": "B",
        "QDTY": "B",
        "RDTY": "B",
        "SDTY": "B",
        "SLTY": "B",
        "ULTY": "B",
        "YBTC": "C",
        "YETH": "A",

        # Wed buy, ex Thu, pay Fri
        "GMEY": "B",
        "RDYY": "B",
        "HIYY": "B",
        "PLTY": "B",
        "CVNY": "B",
        "CONY": "B",
        "HOOY": "B",
        "CRCO": "B",
        "SMCY": "B",
        "RBLY": "B",
        "XYZY": "B",
        "GDXY": "B",
        "YBIT": "B",
        "DRAY": "B",
        "DIPS": "B",

        # Thu buy, ex Fri (MAGY special)
        "MAGY": "C",

        # Fri buy, ex Mon, pay Tue (Roundhill “W” group etc.)
        "COIW": "B",
        "HOOW": "A",
        "AMDW": "B",
        "PLTW": "B",
        "AVGW": "B",
        "ARMW": "B",
        "TSLW": "B",
        "GDXW": "B",
        "BABW": "B",
        "GOOW": "A",
        "METW": "B",
        "UBEW": "B",
        "AMZW": "B",
        "NVDW": "B",
        "GLDW": "B",
    }

    # Rotation mapping by ticker
    rotation_map = {
        # Mon buy, ex Tue, pay Wed
        "WPAY": Rotation.MON_TUE,
        "MSST": Rotation.MON_TUE,
        "NVIT": Rotation.MON_TUE,
        "TEST": Rotation.MON_TUE,

        # Tue buy, ex Wed, pay Thu
        "CHPY": Rotation.TUE_WED,
        "FEAT": Rotation.TUE_WED,
        "FIVY": Rotation.TUE_WED,
        "GPTY": Rotation.TUE_WED,
        "LFGY": Rotation.TUE_WED,
        "QDTY": Rotation.TUE_WED,
        "RDTY": Rotation.TUE_WED,
        "SDTY": Rotation.TUE_WED,
        "SLTY": Rotation.TUE_WED,
        "ULTY": Rotation.TUE_WED,
        "YBTC": Rotation.TUE_WED,
        "YETH": Rotation.TUE_WED,

        # Wed buy, ex Thu, pay Fri
        "GMEY": Rotation.WED_THU,
        "RDYY": Rotation.WED_THU,
        "HIYY": Rotation.WED_THU,
        "PLTY": Rotation.WED_THU,
        "CVNY": Rotation.WED_THU,
        "CONY": Rotation.WED_THU,
        "HOOY": Rotation.WED_THU,
        "CRCO": Rotation.WED_THU,
        "SMCY": Rotation.WED_THU,
        "RBLY": Rotation.WED_THU,
        "XYZY": Rotation.WED_THU,
        "GDXY": Rotation.WED_THU,
        "YBIT": Rotation.WED_THU,
        "DRAY": Rotation.WED_THU,
        "DIPS": Rotation.WED_THU,

        # Thu buy, ex Fri (MAGY special)
        "MAGY": Rotation.THU_FRI,

        # Fri buy, ex Mon, pay Tue
        "COIW": Rotation.FRI_MON,
        "HOOW": Rotation.FRI_MON,
        "AMDW": Rotation.FRI_MON,
        "PLTW": Rotation.FRI_MON,
        "AVGW": Rotation.FRI_MON,
        "ARMW": Rotation.FRI_MON,
        "TSLW": Rotation.FRI_MON,
        "GDXW": Rotation.FRI_MON,
        "BABW": Rotation.FRI_MON,
        "GOOW": Rotation.FRI_MON,
        "METW": Rotation.FRI_MON,
        "UBEW": Rotation.FRI_MON,
        "AMZW": Rotation.FRI_MON,
        "NVDW": Rotation.FRI_MON,
        "GLDW": Rotation.FRI_MON,
    }

    config = build_sample_config()

    # Load fresh dividends from X fetcher
    dividends_file = 'data_dividends.csv'
    if os.path.exists(dividends_file):
        dividends = pd.read_csv(dividends_file, index_col='ticker')
        print(f"[INFO] Loaded {len(dividends)} fresh dividend announcements from X")
    else:
        dividends = pd.DataFrame()
        print("[WARNING] No data_dividends.csv - run x_dividend_fetcher_v2.py first!")

    day_name = os.getenv("ROTATION_DAY") or datetime.now().strftime("%A")
    tickers = load_watchlist_tickers(day_name)

    if day_name not in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
        print(f"[INFO] {day_name} is not a trading/rotation day - no signals generated")
        sys.exit(0)  # Optional: exit early, or just continue with no signals

    print(f"[INFO] Rotation day = {day_name}")
    print(f"[INFO] Loaded {len(tickers)} tickers from data_watchlist.csv")
    print(f"[INFO] Tickers: {tickers}")

    quotes = get_live_quotes(tickers)
    # Only pull dividends for today's rotation tickers
    ex_date = os.getenv("ROTATION_EX_DATE")
    dividends = get_dividends(tickers, ex_date=ex_date)

    print("dividend tickers:", list(dividends.keys()))
    print("quote tickers:", list(quotes.keys()))

    signals = generate_signals(today=today, quotes=quotes, dividends=dividends, config=config)

    print(f"Date: {today.isoformat()}")
    print("Generated signals based on current rules:\n")
    if not signals:
        print("No signals for today (nothing qualifies).")
    else:
        print_signals(signals)

import sys
from engine.plan_engine import run_plan_engine


if __name__ == "__main__":
    if "--plan" in sys.argv:
        run_plan_engine()
        raise SystemExit(0)

    main()
