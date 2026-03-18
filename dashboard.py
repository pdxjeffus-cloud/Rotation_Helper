import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
import streamlit as st

# --- helpers ---
def _to_float(x, default=np.nan):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def parse_etrade_quotes(raw):
    """
    Accepts either:
      - dict with QuoteResponse/QuoteData list (common E*TRADE shape)
      - list of QuoteData dicts
      - dict keyed by symbol (fallback)

    Returns: dict[symbol] -> {Bid, Ask, Last Price, Open, Close, Volume, % Change}
    """
    out = {}

    # Normalize QuoteData list
    qd_list = None
    if isinstance(raw, dict) and "QuoteResponse" in raw:
        qd_list = raw.get("QuoteResponse", {}).get("QuoteData", [])
    elif isinstance(raw, list):
        qd_list = raw
    elif isinstance(raw, dict):
        # maybe dict keyed by symbol
        for sym, item in raw.items():
            if isinstance(item, dict):
                out[str(sym).upper()] = {
                    "Bid": _to_float(item.get("Bid") or item.get("bid")),
                    "Ask": _to_float(item.get("Ask") or item.get("ask")),
                    "Last Price": _to_float(item.get("Last Price") or item.get("last") or item.get("lastTrade") or item.get("lastPrice")),
                    "Open": _to_float(item.get("Open") or item.get("open")),
                    "Close": _to_float(item.get("Previous Close") or item.get("previousClose") or item.get("Close") or item.get("close")),
                    "Volume": _to_float(item.get("Volume") or item.get("volume") or item.get("totalVolume"), 0.0),
                    "% Change": _to_float(item.get("% Change") or item.get("changeClosePercentage") or item.get("pctChange")),
                }
        return out

    if not qd_list:
        return out

    for q in qd_list:
        try:
            prod = q.get("Product", {}) if isinstance(q, dict) else {}
            sym = (prod.get("symbol") or q.get("symbol") or "").upper()
            if not sym:
                continue

            all_blk = q.get("All", {}) if isinstance(q, dict) else {}
            ext = q.get("ExtendedHourQuoteDetail", {}) if isinstance(q, dict) else {}

            bid = _to_float(all_blk.get("bid", q.get("bid")))
            ask = _to_float(all_blk.get("ask", q.get("ask")))
            last = _to_float(
                all_blk.get("lastTrade",
                all_blk.get("lastPrice",
                ext.get("lastPrice",
                q.get("lastTrade",
                q.get("lastPrice", q.get("last"))))))
            )
            vol = _to_float(all_blk.get("totalVolume", q.get("totalVolume", q.get("volume"))), 0.0)
            pct = _to_float(all_blk.get("changeClosePercentage", q.get("changeClosePercentage")))
            open_px = _to_float(all_blk.get("open", ext.get("open", q.get("open"))))
            close_px = _to_float(all_blk.get("previousClose", ext.get("previousClose", q.get("previousClose"))))

            out[sym] = {
                "Bid": bid,
                "Ask": ask,
                "Last Price": last,
                "Open": open_px,
                "Close": close_px,
                "Volume": vol,
                "% Change": pct,
            }
        except Exception:
            continue

    return out

def ny_now_str():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

# --- Streamlit ---
st.set_page_config(page_title="Rotation Helper - Action", layout="wide")
st.title("Daily Rotation Action Board — (NY Time)")
st.caption(f"NY now: {ny_now_str()}")

DIV_CSV = "data_dividends.csv"
if not os.path.exists(DIV_CSV):
    st.error(f"Missing {DIV_CSV}. Run the dividend fetcher first.")
    st.stop()

div_df = pd.read_csv(DIV_CSV)
# expected columns: ticker, amount (or Dividend), announce_date, ex_date, pay_date, source
if "ticker" not in div_df.columns:
    st.error("data_dividends.csv missing 'ticker' column.")
    st.stop()

# normalize dividend column name
if "amount" in div_df.columns:
    div_df["Dividend"] = div_df["amount"].apply(lambda x: abs(_to_float(x, 0.0)))
elif "Dividend" in div_df.columns:
    div_df["Dividend"] = div_df["Dividend"].apply(lambda x: abs(_to_float(x, 0.0)))
else:
    div_df["Dividend"] = 0.0

div_df["Ticker"] = (
    div_df["ticker"]
    .astype(str)
    .str.upper()
    .str.replace(r"[^A-Z]", "", regex=True)
)
tickers = div_df["Ticker"].dropna().unique().tolist()

# --- pull E*TRADE quotes ---
quotes_ok = True
quote_dict = {}

try:
    from etrade.client import get_quote  # uses your existing token/session logic
    raw = get_quote(tickers)
    quote_dict = parse_etrade_quotes(raw)
except Exception as e:
    quotes_ok = False
    st.error("E*TRADE quote pull FAILED. You must run manual OAuth when needed:")
    st.code("PYTHONPATH=. python3 etrade/auth_flow.py", language="bash")
    st.exception(e)

# --- build display df ---
df = pd.DataFrame({"Ticker": tickers}).set_index("Ticker")
df["Dividend"] = df.index.map(lambda t: float(div_df.loc[div_df["Ticker"] == t, "Dividend"].iloc[0]) if (div_df["Ticker"] == t).any() else 0.0)

if quotes_ok and quote_dict:
    df["Bid"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Bid", np.nan))
    df["Ask"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Ask", np.nan))
    df["Last Price"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Last Price", np.nan))
    df["Open"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Open", np.nan))
    df["Close"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Close", np.nan))
    df["Volume"] = df.index.map(lambda t: quote_dict.get(t, {}).get("Volume", 0.0))
    df["% Change"] = df.index.map(lambda t: quote_dict.get(t, {}).get("% Change", np.nan))
else:
    df["Bid"] = np.nan
    df["Ask"] = np.nan
    df["Last Price"] = np.nan
    df["Open"] = np.nan
    df["Close"] = np.nan
    df["Volume"] = 0.0
    df["% Change"] = np.nan

# ensure optional quote fields exist before coercion
if "Open" not in df.columns:
    df["Open"] = np.nan
if "Close" not in df.columns:
    df["Close"] = np.nan

# numeric coercion
for c in ["Dividend","Bid","Ask","Last Price","Open","Close","Volume","% Change"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# display alias
df["Price"] = df["Last Price"]

# guard missing quote fields
if "Open" not in df.columns:
    df["Open"] = np.nan
if "Close" not in df.columns:
    df["Close"] = np.nan

# --- metrics (use Ask for entry cost; fall back to Last Price) ---
px = df["Ask"].where(df["Ask"] > 0, df["Last Price"])
df["Spread"] = (df["Ask"] - df["Bid"]).where((df["Ask"] > 0) & (df["Bid"] > 0), np.nan)

df["Return %"] = np.where(px > 0, (df["Dividend"] / px) * 100.0, 0.0)
df["Net $/sh"] = np.where((df["Ask"] > 0) & (df["Bid"] > 0), df["Dividend"] - (df["Ask"] - df["Bid"]), df["Dividend"])
df["Net %"] = np.where(px > 0, (df["Net $/sh"] / px) * 100.0, 0.0)

df["$10k sh"] = np.where(px > 0, 10000.0 / px, 0.0)
df["$10k Net"] = df["$10k sh"] * df["Net $/sh"]

df["Spread % Div"] = np.where(df["Dividend"] > 0, (df["Spread"] / df["Dividend"]) * 100.0, 999.0)

# spreadsheet-style columns
df["Gap"] = np.where(df["Open"].notna() & df["Close"].notna(), df["Open"] - df["Close"], np.nan)
df["Gap/div"] = np.where(df["Dividend"] > 0, df["Gap"] / df["Dividend"], np.nan)
df["Live Ratio"] = np.where(
    df["Dividend"] > 0,
    (df["Price"] - df["Close"]) / df["Dividend"],
    np.nan
)
df["Recovery"] = np.where(
    df["Close"] > 0,
    ((df["Close"] - df["Price"]) / df["Close"]) * 100.0,
    np.nan
)
df["Score"] = np.where(
    df["Gap/div"].notna(),
    df["Return %"] * np.abs(np.minimum(df["Gap/div"], 0.0)),
    0.0
)

# --- GO rule (simple + usable TODAY; tune later) ---
df["GO"] = (
    (df["Dividend"] > 0)
    & (df["Ask"] > 0)
    & (df["Bid"] > 0)
    & (df["Volume"].fillna(0) >= 500)
    & (df["Net %"] >= 0.60)
    & (df["Spread % Div"] <= 35.0)
)

# --- Signal color (GREEN / YELLOW / RED) ---
# --- Trading brain (Handbook rules) ---
# Spread bps = (Ask - Bid) / mid * 10,000
mid = (df["Bid"] + df["Ask"]) / 2.0
df["Spread bps"] = np.where(mid > 0, (df["Ask"] - df["Bid"]) / mid * 10000.0, 9999.0)

# Spread % of dividend (already computed as Spread % Div earlier in your file)
# GREEN: clean spreads (bps <= 12) and spread/div <= 10%
# YELLOW: acceptable (bps <= 20) and spread/div <= 25%
# RED: otherwise
green_ok = (
    (df["Dividend"] > 0)
    & (df["Ask"] > 0)
    & (df["Bid"] > 0)
    & (df["Spread bps"] <= 12.0)
    & (df["Spread % Div"] <= 10.0)
)

yellow_ok = (
    (df["Dividend"] > 0)
    & (df["Ask"] > 0)
    & (df["Bid"] > 0)
    & (df["Spread bps"] <= 20.0)
    & (df["Spread % Div"] <= 25.0)
)

# Keep GO as a boolean for checkbox + counting (GO = GREEN only)
df["GO"] = green_ok

df["Signal"] = np.select(
    [green_ok, yellow_ok],
    ["GREEN", "YELLOW"],
    default="RED"
)

# colored square indicator
df["Sig"] = df["Signal"].map({"GREEN": "🟩", "YELLOW": "🟨", "RED": "🟥"}).fillna("⬜")



# --- Priority (rank by Return %: highest = 1) ---
df = df.sort_values(by="Return %", ascending=False, kind="mergesort")
df["Priority"] = np.arange(1, len(df) + 1)


# --- display formatting ---
show = df.reset_index()[[
    "GO","Sig","Ticker","Dividend","Price","Score","Bid","Ask","Close","Open","Gap","Gap/div","Live Ratio","Recovery","Spread","Spread bps","Spread % Div","Net $/sh","Net %","Return %","Volume"
]]

# nice formatting for display only
def fmt_money(x): return "" if pd.isna(x) else f"${x:,.4f}" if abs(x) < 1 else f"${x:,.2f}"
def fmt_pct(x): return "" if pd.isna(x) else f"{x:,.2f}%"
def fmt_int(x): return "" if pd.isna(x) else f"{int(x):,}"

disp = show.copy()
disp["Dividend"] = disp["Dividend"].map(lambda v: fmt_money(v))
for c in ["Price","Bid","Ask","Close","Open","Gap","Spread","Net $/sh"]:
    disp[c] = pd.to_numeric(show[c], errors="coerce").map(fmt_money)
for c in ["Score","Recovery","Net %","Return %"]:
    disp[c] = pd.to_numeric(show[c], errors="coerce").map(fmt_pct)
for c in ["Gap/div","Live Ratio","Spread % Div"]:
    disp[c] = pd.to_numeric(show[c], errors="coerce").map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
disp["Volume"] = pd.to_numeric(show["Volume"], errors="coerce").map(fmt_int)

go_count = int(show["GO"].sum())
st.subheader(f"GO trades: {go_count} / {len(show)}")

st.dataframe(disp, use_container_width=True, hide_index=True)

st.caption("If Bid/Ask/Volume are blank: run manual E*TRADE OAuth, then refresh the page.")
