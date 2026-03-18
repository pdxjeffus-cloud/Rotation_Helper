#!/usr/bin/env bash
set -euo pipefail

ROOT="/root/Rotation_Helper"
VENV="$ROOT/.venv"
ENVFILE="$ROOT/.env"
LOG="$ROOT/dashboard.log"
CSV="$ROOT/data_dividends.csv"

cd "$ROOT"

log() { echo "$*" | tee -a "$LOG"; }

: > "$LOG"

log "============================"
log "START_DAY UTC: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"

python3 - <<'PY' | tee -a "$LOG" || true
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
    print("NY_NOW:", datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S %Z"))
except Exception as e:
    print("NY_NOW unavailable:", e)
PY

# venv + env
source "$VENV/bin/activate"
log "VENV: $VENV"
log "PYTHON: $(which python3)"

if [[ -f "$ENVFILE" ]]; then
  set -a
  source "$ENVFILE"
  set +a
  log ".env loaded: $ENVFILE"
else
  log "WARNING: .env not found at $ENVFILE"
fi

# --- Canonical daily CSV reset (ALWAYS first) ---
log "Dividend CSV reset: $CSV"
echo "ticker,amount,ex_date,pay_date,asof_date,source" > "$CSV"

# --- YieldMax (X -> CSV truth). Roundhill is OCR-only and runs AFTER this. ---
log "YieldMax fetcher: x_dividend_fetcher_v3.py"
set +e
python3 -u x_dividend_fetcher_v3.py >>"$LOG" 2>&1
YM_RC=$?
set -e
log "YieldMax fetcher exit code: $YM_RC"

# --- Roundhill OCR (ALWAYS) — must run AFTER reset + AFTER YieldMax so it cannot be wiped ---
log "Roundhill OCR (OCR-only rule): roundhill_ocr_fix.py"
set +e
python3 -u roundhill_ocr_fix.py >>"$LOG" 2>&1
RH_RC=$?
set -e
log "Roundhill OCR exit code: $RH_RC"

log "Dividend CSV lines: $(wc -l < "$CSV" 2>/dev/null || echo 0)"
log "Dividend CSV preview:"
head -n 60 "$CSV" | tee -a "$LOG" || true

# --- E*TRADE preflight: if token rejected, force manual OAuth ---
log "E*TRADE preflight..."
set +e
python3 - <<'PY' >>"$LOG" 2>&1
import sys
from etrade.client import get_quote
try:
    get_quote(["SPY"])  # tiny call just to test auth
    print("ETRADE_PREFLIGHT_OK")
    sys.exit(0)
except Exception as e:
    print("ETRADE_PREFLIGHT_FAILED:", e)
    sys.exit(1)
PY
ET_RC=$?
set -e

if [[ "$ET_RC" -ne 0 ]]; then
  log "E*TRADE token rejected -> launching manual OAuth now..."
  log "When prompted, open the URL, login, paste the verification code back here."
  set +e
  PYTHONPATH=. python3 -m etrade.auth_flow 2>&1 | tee -a "$LOG"
  AUTH_RC=${PIPESTATUS[0]}
  set -e
  log "auth_flow exit code: $AUTH_RC"
fi

# --- Restart Streamlit dashboard ---
log "Restarting Streamlit dashboard..."
pkill -f "streamlit run" >/dev/null 2>&1 || true
nohup streamlit run dashboard.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  >>"$LOG" 2>&1 &

sleep 1
log "Dashboard started. Tail logs: tail -n 200 $LOG"
log "============================"
