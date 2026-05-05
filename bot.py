import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import os
import gspread

from decimal import Decimal, ROUND_DOWN, ROUND_UP, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY PARAMETERS  (Trendline Break + Anti-Fakeout — LONG ONLY)
#
# TIMEFRAME ARCHITECTURE:
#   - Trendline / S-R levels   : 1h closed candles
#   - Entry confirmation       : 15m closed candles
#   - Scan interval            : 15 minutes
# =============================================================================

# ─── TRENDLINE CALC (1h) ─────────────────────────────────────────────────────
SWING_LOOKBACK   = 14          # pivot lookback (each side)
SLOPE_MULT       = 1.0         # slope multiplier
ATR_PERIOD_TL    = 14          # ATR period for trendline slope (1h)

# ─── RISK ────────────────────────────────────────────────────────────────────
RR_RATIO         = 2.0         # reward:risk
SL_BUFFER_PCT    = 0.1         # extra % below lastPL for SL

# ─── ANTI-FAKEOUT FILTERS (applied on 15m entry candle) ─────────────────────
USE_BODY_BREAK   = True
USE_STRONG_BAR   = True
MIN_BODY_PCT     = 50.0        # body ≥ X% of range
USE_VOLUME       = True
VOL_MULT         = 1.2         # vol > SMA(20) × mult
VOL_SMA_PERIOD   = 20
USE_ATR_DIST     = True
ATR_MULT         = 0.25        # break ≥ X × ATR(14) beyond TL
ATR_PERIOD_15M   = 14
USE_COOLDOWN     = True
COOLDOWN_BARS    = 5           # 15m bars between entries

# ─── ORDER PLACEMENT ────────────────────────────────────────────────────────
# Limit BUY price = signal_close × (1 + LIMIT_BUFFER_PCT/100)
# Small upward buffer ensures the order fills even if price drifted up
# in the seconds between candle close and order placement.
LIMIT_BUFFER_PCT = 0.15

# ─── CANDLE COUNTS ───────────────────────────────────────────────────────────
TL_CANDLES_1H    = 500         # 1h candles fetched for trendline calc
ENTRY_CANDLES    = 60          # 15m candles for entry confirmation

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY    = "60"            # 1h
RESOLUTION_ENTRY      = "15"            # 15m
CANDLE_SECONDS        = 60 * 60         # 1h in seconds
ENTRY_CANDLE_SECONDS  = 15 * 60         # 15m in seconds
SCAN_INTERVAL         = 15 * 60         # 15 min

# ─── REQUEST TIMEOUTS ────────────────────────────────────────────────────────
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10

# ─── GSPREAD RE-AUTH INTERVAL ────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60

# ─── LOCAL STATE FILE ────────────────────────────────────────────────────────
STATE_FILE           = "bot_state.json"
# =============================================================================


# =====================================================
# GOOGLE SHEETS
# =====================================================

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_sheet          = None
_last_auth_time = 0


def get_sheet():
    global _sheet, _last_auth_time
    now = time.time()
    if _sheet is None or (now - _last_auth_time) > GSHEET_REAUTH_INTERVAL:
        try:
            creds           = Credentials.from_service_account_file("service_account.json", scopes=SCOPE)
            client          = gspread.authorize(creds)
            _sheet          = client.open_by_key(SHEET_ID).sheet1
            _last_auth_time = now
            print("[GSHEET] Re-authenticated successfully")
        except Exception as e:
            print(f"[GSHEET] Re-auth failed: {e}")
    return _sheet


def get_sheet_data():
    try:
        sheet = get_sheet()
        if sheet is None:
            return pd.DataFrame()
        data = sheet.get_all_values()
        df   = pd.DataFrame(data)
        if df.shape[1] < 3:
            for col in range(df.shape[1], 3):
                df[col] = ""
        return df
    except Exception as e:
        print("Sheet read error:", e)
        return pd.DataFrame()


def update_sheet_tp(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update_acell(f"B{row + 1}", str(value))
        print(f"[SHEET] Row {row + 1} col B -> {value}")
    except Exception as e:
        print("Sheet update error:", e)


def update_sheet_sl(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update_acell(f"C{row + 1}", str(value))
        print(f"[SHEET] Row {row + 1} col C (SL) -> {value}")
    except Exception as e:
        print("Sheet SL update error:", e)


def _cell_blank(df, row, col):
    """True if the sheet cell at (row, col) is empty/whitespace."""
    if df.shape[1] <= col:
        return True
    val = str(df.iloc[row, col]).strip()
    return val == ""


def sync_sheet_levels(row, df, st):
    """
    If state has tp_level/sl_price but the sheet cells are blank,
    push them. Protects against earlier silent gspread failures.
    """
    tp_state = st.get("tp_level")
    sl_state = st.get("sl_price")

    if tp_state is not None and _cell_blank(df, row, 1):
        print(f"[SHEET-RETRY] Row {row + 1} — TP cell blank, pushing {tp_state}")
        update_sheet_tp(row, tp_state)
    if sl_state is not None and _cell_blank(df, row, 2):
        print(f"[SHEET-RETRY] Row {row + 1} — SL cell blank, pushing {sl_state}")
        update_sheet_sl(row, sl_state)


# =====================================================
# LOCAL STATE PERSISTENCE
# =====================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"[STATE] Load error: {e} — starting fresh")
            return {}
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE] Save error: {e}")


def init_symbol_state():
    return {
        "in_position":   False,
        "entry_path":    None,
        "entry_price":   None,
        "tp_level":      None,
        "sl_price":      None,
        "last_entry_ts": 0,        # ms timestamp of last entry (cooldown anchor)
    }


# =====================================================
# SYMBOL HELPERS
# =====================================================

def normalize_symbol(symbol):
    symbol = str(symbol).upper().strip()
    if "USDT" in symbol:
        return symbol.split("USDT")[0] + "USDT"
    return symbol


def fut_pair(symbol):
    return f"B-{symbol.replace('USDT', '')}_USDT"


# =====================================================
# SIGN REQUEST
# =====================================================

def sign_request(body):
    payload   = json.dumps(body, separators=(",", ":"))
    signature = hmac.new(
        bytes(COINDCX_SECRET, encoding="utf-8"),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type":     "application/json",
        "X-AUTH-APIKEY":    COINDCX_KEY,
        "X-AUTH-SIGNATURE": signature,
    }
    return payload, headers


# =====================================================
# TELEGRAM
# =====================================================

def send_telegram(message):
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }
        requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
    except Exception as e:
        print(f"[TELEGRAM] Failed to send message: {e}")


# =====================================================
# INSTRUMENT METADATA (cached) — tick size & lot size
# =====================================================

_instrument_cache = {}  # symbol -> dict
_INSTRUMENT_CACHE_TTL = 6 * 3600  # 6h


def _decimals_from_step(step: Decimal) -> int:
    """Decimal places implied by step e.g. Decimal('0.0001') -> 4."""
    s = format(step.normalize(), "f")
    if "." in s:
        return len(s.split(".")[1].rstrip("0"))
    return 0


def get_instrument_meta(symbol):
    """Fetch (and cache) tick size, qty step, min qty for a symbol."""
    now = time.time()
    cached = _instrument_cache.get(symbol)
    if cached and (now - cached["ts"]) < _INSTRUMENT_CACHE_TTL:
        return cached

    pair = fut_pair(symbol)
    url  = (
        "https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
        f"?pair={pair}&margin_currency_short_name=USDT"
    )
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        data     = response.json()
        inst     = data["instrument"]

        qty_step = Decimal(str(inst.get("quantity_increment", "1")))
        min_qty  = Decimal(str(inst.get("min_quantity", qty_step)))

        # CoinDCX may expose this under different keys depending on version
        price_step_raw = (
            inst.get("price_increment")
            or inst.get("tick_size")
            or inst.get("min_price_increment")
        )
        price_step = Decimal(str(price_step_raw)) if price_step_raw is not None else None

        meta = {
            "price_step": price_step,
            "qty_step":   qty_step,
            "min_qty":    min_qty,
            "ts":         now,
        }
        _instrument_cache[symbol] = meta
        return meta
    except Exception as e:
        print(f"[INSTRUMENT] {symbol} fetch error: {e}")
        return {
            "price_step": None,
            "qty_step":   Decimal("1"),
            "min_qty":    Decimal("1"),
            "ts":         now - _INSTRUMENT_CACHE_TTL + 60,  # short retry
        }


def round_price(price, step: Decimal, mode: str = "down") -> Decimal:
    """Round a price to the exchange tick. mode: 'down' | 'up' | 'nearest'."""
    p = Decimal(str(price))
    if step is None or step == 0:
        return p
    units = p / step
    if mode == "down":
        units = units.to_integral_value(rounding=ROUND_DOWN)
    elif mode == "up":
        units = units.to_integral_value(rounding=ROUND_UP)
    else:
        units = units.quantize(Decimal("1"))
    return (units * step).quantize(step)


def fmt_price(price_decimal: Decimal, step: Decimal) -> str:
    """Format a price for the API at exactly the decimal places of step."""
    if step is None:
        return str(price_decimal)
    decimals = _decimals_from_step(step)
    return format(price_decimal, f".{decimals}f")


# =====================================================
# INDICATORS
# =====================================================

def compute_atr(highs, lows, closes, period):
    """Wilder's ATR (matches Pine's ta.atr / RMA smoothing)."""
    n = len(closes)
    if n == 0:
        return []
    tr = [highs[0] - lows[0]]
    for i in range(1, n):
        tr.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        ))
    atr = [None] * n
    if n < period:
        return atr
    atr[period - 1] = sum(tr[:period]) / period
    for i in range(period, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def compute_trendlines(highs, lows, closes, length, mult):
    """
    LuxAlgo-style Trendlines with Breaks (long-side fields).
    Walks every bar, replicating Pine semantics:
      slope_ph := ph ? slope : slope_ph
      upper    := ph ? ph    : upper - slope_ph

    Returns final-bar values:
        upper_lvl = upper - slope_ph * length     # projected break level
        last_ph   = most recent confirmed pivot high
        last_pl   = most recent confirmed pivot low (used for SL)
    """
    n = len(closes)
    atr_arr = compute_atr(highs, lows, closes, ATR_PERIOD_TL)

    cur_upper = 0.0
    cur_lower = 0.0
    cur_slope_ph = 0.0
    cur_slope_pl = 0.0
    cur_last_ph = None
    cur_last_pl = None
    have_upper = False
    have_lower = False

    for i in range(n):
        ph = None
        pl = None

        # Pivot at index (i-length) is confirmed at bar i
        if i >= 2 * length:
            c   = i - length
            h_c = highs[c]
            l_c = lows[c]
            is_ph = True
            is_pl = True
            for k in range(1, length + 1):
                if highs[c - k] >= h_c or highs[c + k] >= h_c:
                    is_ph = False
                if lows[c - k]  <= l_c or lows[c + k]  <= l_c:
                    is_pl = False
                if not is_ph and not is_pl:
                    break
            if is_ph:
                ph = h_c
            if is_pl:
                pl = l_c

        slope = (atr_arr[i] / length * mult) if (atr_arr[i] is not None) else 0.0

        if ph is not None:
            cur_slope_ph = slope
            cur_upper    = ph
            cur_last_ph  = ph
            have_upper   = True
        elif have_upper:
            cur_upper -= cur_slope_ph

        if pl is not None:
            cur_slope_pl = slope
            cur_lower    = pl
            cur_last_pl  = pl
            have_lower   = True
        elif have_lower:
            cur_lower += cur_slope_pl

    upper_lvl = (cur_upper - cur_slope_ph * length) if have_upper else None
    lower_lvl = (cur_lower + cur_slope_pl * length) if have_lower else None

    return {
        "upper_lvl": upper_lvl,
        "lower_lvl": lower_lvl,
        "last_ph":   cur_last_ph,
        "last_pl":   cur_last_pl,
    }


# =====================================================
# CANDLE FETCH
# =====================================================

def fetch_candles(symbol, num_candles_needed, resolution_str=None, candle_seconds=None):
    if resolution_str is None:
        resolution_str = RESOLUTION_PRIMARY
    if candle_seconds is None:
        candle_seconds = CANDLE_SECONDS

    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    fetch_seconds = (num_candles_needed + 50) * candle_seconds

    params = {
        "pair":       pair_api,
        "from":       now - fetch_seconds,
        "to":         now,
        "resolution": resolution_str,
        "pcode":      "f",
    }
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data     = response.json().get("data", [])
        candles  = sorted(data, key=lambda x: x["time"])
        return candles
    except Exception as e:
        print(f"[CANDLES {resolution_str}] {symbol} fetch error: {e}")
        return []


# =====================================================
# RECENT HIGH (TP wick detection)
# =====================================================

def get_recent_high(symbol):
    try:
        pair_api = fut_pair(symbol)
        url  = "https://public.coindcx.com/market_data/candlesticks"
        now  = int(time.time())
        params = {
            "pair":       pair_api,
            "from":       now - SCAN_INTERVAL,
            "to":         now,
            "resolution": "1",
            "pcode":      "f",
        }
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = response.json().get("data", [])
        if not candles:
            return None
        highs = [float(c["high"]) for c in candles]
        return max(highs)
    except Exception as e:
        print(f"[RECENT HIGH] {symbol} error: {e}")
        return None


# =====================================================
# POSITIONS & ORDERS
# =====================================================

def get_open_positions():
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "page":                       "1",
            "size":                       "50",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/positions"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        positions = response.json()
        if not isinstance(positions, list):
            return []
        return [p for p in positions if float(p.get("active_pos", 0)) != 0]
    except Exception as e:
        print("get_open_positions error:", e)
        return []


def get_position_by_pair(symbol):
    positions = get_open_positions()
    pair = fut_pair(symbol)
    for p in positions:
        if p.get("pair") == pair:
            return p
    return None


def has_open_order(symbol):
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "status":                     "open,partially_filled",
            "side":                       "buy",
            "page":                       "1",
            "size":                       "50",
            "margin_currency_short_name": ["USDT"],
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        orders   = response.json()

        if not isinstance(orders, list):
            print(f"[has_open_order] {symbol} unexpected response (request rejected?): {orders}")
            return False

        pair = fut_pair(symbol)
        for o in orders:
            if o.get("pair") == pair:
                return True
        return False

    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


# =====================================================
# QUANTITY
# =====================================================

def compute_qty(entry_price, symbol):
    meta     = get_instrument_meta(symbol)
    qty_step = meta["qty_step"]
    min_qty  = meta["min_qty"]
    step     = max(qty_step, min_qty)

    capital  = Decimal(str(CAPITAL_USDT))
    leverage = Decimal(str(LEVERAGE))
    exposure = capital * leverage
    raw_qty  = exposure / Decimal(str(entry_price))
    units    = (raw_qty / step).to_integral_value(rounding=ROUND_DOWN)
    qty      = units * step
    if qty < min_qty:
        qty = min_qty
    return qty.quantize(step)


# =====================================================
# PLACE LONG ORDER  (tick-aware rounding + validation + buffer)
# =====================================================

def place_long_order(symbol, signal_close, tp_raw, sl_raw, entry_path, signal_info=None):
    """
    signal_close : the 15m close that fired the breakout (entry anchor)
    tp_raw / sl_raw : float values pre-rounding
    Returns (placed: bool, entry_dec, tp_dec, sl_dec) — Decimals on success, Nones on failure.
    """
    meta       = get_instrument_meta(symbol)
    price_step = meta["price_step"]

    # ── 1. Limit BUY price with upward buffer ─────────────────────────────
    entry_target = (
        Decimal(str(signal_close))
        * (Decimal("1") + Decimal(str(LIMIT_BUFFER_PCT)) / Decimal("100"))
    )

    if price_step is not None:
        entry_dec = round_price(entry_target, price_step, "up")
        sl_dec    = round_price(sl_raw,        price_step, "down")  # SL further from entry
        tp_dec    = round_price(tp_raw,        price_step, "up")    # TP further from entry
    else:
        # Fallback: derive a tick from the close string
        s = str(signal_close)
        decimals = len(s.split(".")[1]) if "." in s else 0
        q = Decimal(10) ** -decimals if decimals > 0 else Decimal("1")
        entry_dec  = entry_target.quantize(q, rounding=ROUND_UP)
        sl_dec     = Decimal(str(sl_raw)).quantize(q, rounding=ROUND_DOWN)
        tp_dec     = Decimal(str(tp_raw)).quantize(q, rounding=ROUND_UP)
        price_step = q

    # ── 2. Validation: SL < entry < TP, with at least 1 tick gap ──────────
    min_gap = price_step
    if not (sl_dec + min_gap <= entry_dec and entry_dec + min_gap <= tp_dec):
        msg = (f"invalid levels SL={sl_dec} entry={entry_dec} TP={tp_dec} "
               f"(need SL < entry < TP, ≥1 tick apart)")
        print(f"[VALIDATION] {symbol} {msg}")
        send_telegram(
            f"⚠️ <b>ORDER SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry : <code>{entry_dec}</code>\n"
            f"🎯 TP    : <code>{tp_dec}</code>\n"
            f"🛑 SL    : <code>{sl_dec}</code>\n"
            f"⚠️ Reason: {msg}"
        )
        return False, None, None, None

    # ── 3. Quantity ───────────────────────────────────────────────────────
    qty_dec = compute_qty(float(entry_dec), symbol)
    if qty_dec <= 0:
        send_telegram(
            f"⚠️ <b>ORDER SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ Reason: computed qty ≤ 0 (capital/leverage too small for tick size?)"
        )
        return False, None, None, None

    # ── 4. Format strings exactly to step decimals ────────────────────────
    entry_str = fmt_price(entry_dec, price_step)
    tp_str    = fmt_price(tp_dec,    price_step)
    sl_str    = fmt_price(sl_dec,    price_step)
    qty_str   = format(qty_dec, "f")

    tp_pct = float((tp_dec   - entry_dec) / entry_dec * 100)
    sl_pct = float((entry_dec - sl_dec)   / entry_dec * 100)

    print(
        f"[LONG TRADE] {symbol} BUY ({entry_path}) | "
        f"signal_close={signal_close} +{LIMIT_BUFFER_PCT}% buffer → "
        f"Entry {entry_str} | TP {tp_str} (+{tp_pct:.2f}%) | "
        f"SL {sl_str} (-{sl_pct:.2f}%) | Qty {qty_str}"
    )

    # ── 5. Build & send order ─────────────────────────────────────────────
    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "buy",
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry_str,
            "total_quantity":    qty_str,
            "leverage":          LEVERAGE,
            "take_profit_price": tp_str,
            "stop_loss_price":   sl_str,
        },
    }

    payload, headers = sign_request(body)
    try:
        response = requests.post(
            BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
            data=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        result = response.json()
    except Exception as e:
        print(f"[ERROR] {symbol} order request failed: {e}")
        send_telegram(
            f"❌ <b>LONG ORDER NETWORK ERROR — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ Error : <code>{str(e)[:200]}</code>"
        )
        return False, None, None, None

    print(f"[API] {symbol} response: {result}")

    # Robust success detection
    success = False
    if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
        success = "id" in result[0] or "client_order_id" in result[0]
    elif isinstance(result, dict):
        if "orders" in result and isinstance(result["orders"], list) and len(result["orders"]) > 0:
            success = True
        elif "order" in result and isinstance(result["order"], dict):
            success = True

    if not success:
        if isinstance(result, dict):
            err_msg = result.get("message") or result.get("error") or str(result)[:300]
        else:
            err_msg = str(result)[:300]

        print(f"[ERROR] {symbol} long order not placed: {err_msg}")
        send_telegram(
            f"❌ <b>LONG ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path    : <code>{entry_path}</code>\n"
            f"📍 Entry   : <code>{entry_str}</code>\n"
            f"🎯 TP      : <code>{tp_str}</code>\n"
            f"🛑 SL      : <code>{sl_str}</code>\n"
            f"📦 Qty     : <code>{qty_str}</code>\n"
            f"⚠️ Reason  : <code>{str(err_msg)[:300]}</code>"
        )
        return False, None, None, None

    # ── 6. Success telegram with signal info ──────────────────────────────
    sig_block = ""
    if signal_info:
        sig_block = (
            f"\n━━━━━━━━━━━━━━━━━━\n"
            f"<b>📊 Signal that fired:</b>\n"
            f"📍 c15        : <code>{signal_info.get('c15')}</code>\n"
            f"⏪ prev_c15   : <code>{signal_info.get('prev_c15')}</code>\n"
            f"📈 upperLvl   : <code>{signal_info.get('upper_lvl')}</code>\n"
            f"🟢 lastPL     : <code>{signal_info.get('last_pl')}</code>\n"
            f"🔴 lastPH     : <code>{signal_info.get('last_ph')}</code>\n"
            f"<b>✅ Filters passed:</b>\n"
            f"• fresh cross : <code>True</code> "
            f"(prev≤{signal_info.get('upper_lvl')} &amp; c15&gt;{signal_info.get('upper_lvl')})\n"
            f"• body break  : <code>True</code> "
            f"(min(o,c)={signal_info.get('min_oc')} &gt; upperLvl)\n"
            f"• strong bar  : <code>True</code> "
            f"(body={signal_info.get('body_pct')}% ≥ {signal_info.get('min_body_pct')}%)\n"
            f"• volume      : <code>True</code> "
            f"(vol={signal_info.get('vol')} &gt; SMA20×{signal_info.get('vol_mult')}={signal_info.get('vol_threshold')})\n"
            f"• ATR distance: <code>True</code> "
            f"(c15 &gt; upperLvl + {signal_info.get('atr_mult')}×ATR = {signal_info.get('atr_threshold')})\n"
            f"• cooldown    : <code>True</code> "
            f"({signal_info.get('bars_since_last')} bars since last entry, need ≥{signal_info.get('cooldown_bars')})"
        )

    send_telegram(
        f"🟢 <b>NEW LONG ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry_str}</code> "
        f"(<i>signal {signal_close}, +{LIMIT_BUFFER_PCT}% buffer</i>)\n"
        f"🎯 TP      : <code>{tp_str}</code>  (+{tp_pct:.2f}%)\n"
        f"🛑 SL      : <code>{sl_str}</code>  (-{sl_pct:.2f}%)\n"
        f"📦 Qty     : <code>{qty_str}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
        f"{sig_block}"
    )
    return True, entry_dec, tp_dec, sl_dec


# =====================================================
# MAIN PER-SYMBOL LOGIC  (Trendline Break — long only)
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    # ─── Fetch 1h for trendline / S-R ────────────────────────
    candles_1h = fetch_candles(symbol, TL_CANDLES_1H, RESOLUTION_PRIMARY, CANDLE_SECONDS)
    min_1h_needed = SWING_LOOKBACK * 2 + ATR_PERIOD_TL + 5

    if len(candles_1h) < min_1h_needed:
        print(f"[SKIP] {symbol} — insufficient 1h candles ({len(candles_1h)})")
        return

    # Drop in-progress 1h bar
    now_ms = int(time.time() * 1000)
    if len(candles_1h) >= 1 and (now_ms - int(candles_1h[-1]["time"])) < CANDLE_SECONDS * 1000:
        candles_1h = candles_1h[:-1]
    if len(candles_1h) < min_1h_needed:
        print(f"[SKIP] {symbol} — insufficient closed 1h candles ({len(candles_1h)})")
        return

    highs_1h  = [float(c["high"])  for c in candles_1h]
    lows_1h   = [float(c["low"])   for c in candles_1h]
    closes_1h = [float(c["close"]) for c in candles_1h]

    tl = compute_trendlines(highs_1h, lows_1h, closes_1h, SWING_LOOKBACK, SLOPE_MULT)
    upper_lvl = tl["upper_lvl"]
    last_pl   = tl["last_pl"]
    last_ph   = tl["last_ph"]

    if upper_lvl is None or last_pl is None or last_ph is None:
        print(f"[SKIP] {symbol} — trendline / pivots not ready yet")
        return

    last_close_1h = closes_1h[-1]

    # Display precision (logging only — order placement uses tick-rounding)
    s = str(candles_1h[-1]["close"])
    precision = len(s.split(".")[1]) if "." in s else 0

    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # =========================================================================
    # TP COMPLETED MONITORING
    # =========================================================================
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED marker in sheet, not re-entering")
        save_state(all_state)
        return

    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        tp_hit = False
        hit_kind = None
        hit_price = None

        if last_close_1h >= tp_stored:
            tp_hit    = True
            hit_kind  = "close"
            hit_price = last_close_1h

        if not tp_hit:
            recent_high = get_recent_high(symbol)
            if recent_high is not None and recent_high >= tp_stored:
                tp_hit    = True
                hit_kind  = "wick"
                hit_price = recent_high

        if tp_hit:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} — {hit_kind} {hit_price} ≥ stored TP {tp_stored}")
            send_telegram(
                f"🎯 <b>TP HIT ({hit_kind}) — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 {hit_kind.capitalize():8}: <code>{hit_price}</code>\n"
                f"🎯 TP       : <code>{tp_stored}</code>\n"
                f"✅ Marked <b>TP COMPLETED</b> in sheet — no further entries on this coin"
            )
            if st.get("in_position"):
                prev_last_entry = st.get("last_entry_ts", 0)
                all_state[symbol] = init_symbol_state()
                all_state[symbol]["last_entry_ts"] = prev_last_entry
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close_1h)
            st["in_position"] = True
            st["entry_path"]  = st.get("entry_path") or "tl_break"
            st["entry_price"] = entry_px
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")
        sync_sheet_levels(row, df, st)
        save_state(all_state)
        return

    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")
        send_telegram(
            f"✅ <b>POSITION CLOSED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path     : <code>{st.get('entry_path')}</code>\n"
            f"📍 Entry    : <code>{st.get('entry_price')}</code>\n"
            f"🎯 TP was   : <code>{st.get('tp_level')}</code>\n"
            f"🛑 SL was   : <code>{st.get('sl_price')}</code>"
        )
        prev_last_entry = st.get("last_entry_ts", 0)
        all_state[symbol] = init_symbol_state()
        all_state[symbol]["last_entry_ts"] = prev_last_entry
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        sync_sheet_levels(row, df, st)
        save_state(all_state)
        return

    # =========================================================================
    # FETCH 15m FOR ENTRY CONFIRMATION
    # =========================================================================
    candles_15m = fetch_candles(symbol, ENTRY_CANDLES, RESOLUTION_ENTRY, ENTRY_CANDLE_SECONDS)
    min_15m_needed = max(VOL_SMA_PERIOD, ATR_PERIOD_15M) + 5

    if len(candles_15m) < min_15m_needed:
        print(f"[SKIP] {symbol} — insufficient 15m candles ({len(candles_15m)})")
        return

    if len(candles_15m) >= 1 and (now_ms - int(candles_15m[-1]["time"])) < ENTRY_CANDLE_SECONDS * 1000:
        candles_15m = candles_15m[:-1]
    if len(candles_15m) < min_15m_needed:
        return

    last15 = candles_15m[-1]
    prev15 = candles_15m[-2]

    o15 = float(last15["open"])
    h15 = float(last15["high"])
    l15 = float(last15["low"])
    c15 = float(last15["close"])
    v15 = float(last15.get("volume", 0))
    ts15 = int(last15["time"])
    prev_c15 = float(prev15["close"])

    # ─── 15m filter calcs ────────────────────────────────────
    bar_range = h15 - l15
    bar_body  = abs(c15 - o15)
    body_pct  = (bar_body / bar_range * 100) if bar_range > 0 else 0

    vols    = [float(c.get("volume", 0)) for c in candles_15m[-VOL_SMA_PERIOD:]]
    vol_sma = sum(vols) / len(vols) if vols else 0

    highs_15m  = [float(c["high"])  for c in candles_15m]
    lows_15m   = [float(c["low"])   for c in candles_15m]
    closes_15m = [float(c["close"]) for c in candles_15m]
    atr_arr_15 = compute_atr(highs_15m, lows_15m, closes_15m, ATR_PERIOD_15M)
    atr_15     = atr_arr_15[-1] if atr_arr_15[-1] is not None else 0

    # ─── Filter conditions ──────────────────────────────────
    fresh_cross = (c15 > upper_lvl) and (prev_c15 <= upper_lvl)
    body_break  = (not USE_BODY_BREAK) or (min(o15, c15) > upper_lvl)
    strong_bar  = (not USE_STRONG_BAR) or (body_pct >= MIN_BODY_PCT)
    vol_ok      = (not USE_VOLUME)     or (v15 > vol_sma * VOL_MULT)
    atr_ok      = (not USE_ATR_DIST)   or (c15 > upper_lvl + atr_15 * ATR_MULT)

    # Cooldown
    last_entry_ts = st.get("last_entry_ts", 0) or 0
    if USE_COOLDOWN and last_entry_ts > 0:
        bars_since  = (ts15 - last_entry_ts) // (ENTRY_CANDLE_SECONDS * 1000)
        cooldown_ok = bars_since >= COOLDOWN_BARS
    else:
        cooldown_ok = True
        bars_since  = "n/a"

    # ─── Scan log ───────────────────────────────────────────
    print(
        f"[SCAN] {symbol} | c15={c15} prev_c15={prev_c15} | "
        f"upperLvl={round(upper_lvl, precision)} lastPL={round(last_pl, precision)} "
        f"lastPH={round(last_ph, precision)} | "
        f"fresh={fresh_cross} body={body_break} "
        f"strong={strong_bar}({round(body_pct, 1)}%) "
        f"vol={vol_ok}({round(v15, 2)} vs {round(vol_sma * VOL_MULT, 2)}) "
        f"atr={atr_ok} cd={cooldown_ok}"
    )

    long_sig = fresh_cross and body_break and strong_bar and vol_ok and atr_ok and cooldown_ok

    if not long_sig:
        save_state(all_state)
        return

    # =========================================================================
    # PLACE ENTRY
    # =========================================================================
    sl_raw = last_pl * (1 - SL_BUFFER_PCT / 100)
    risk   = c15 - sl_raw

    if risk <= 0:
        print(f"[SKIP] {symbol} — invalid risk (c15 {c15} ≤ SL {sl_raw})")
        save_state(all_state)
        return

    tp_raw = c15 + risk * RR_RATIO

    # Last-second guards
    if get_position_by_pair(symbol) is not None:
        print(f"[ABORT] {symbol} — position appeared just before placement")
        return
    if has_open_order(symbol):
        print(f"[ABORT] {symbol} — order appeared just before placement")
        return

    signal_info = {
        "c15":             round(c15, precision),
        "prev_c15":        round(prev_c15, precision),
        "upper_lvl":       round(upper_lvl, precision),
        "last_pl":         round(last_pl, precision),
        "last_ph":         round(last_ph, precision),
        "min_oc":          round(min(o15, c15), precision),
        "body_pct":        round(body_pct, 1),
        "min_body_pct":    MIN_BODY_PCT,
        "vol":             round(v15, 2),
        "vol_mult":        VOL_MULT,
        "vol_threshold":   round(vol_sma * VOL_MULT, 2),
        "atr_mult":        ATR_MULT,
        "atr_threshold":   round(upper_lvl + atr_15 * ATR_MULT, precision),
        "bars_since_last": bars_since,
        "cooldown_bars":   COOLDOWN_BARS,
    }

    placed, entry_dec, tp_dec, sl_dec = place_long_order(
        symbol, c15, tp_raw, sl_raw, "tl_break", signal_info
    )

    if placed:
        st["in_position"]   = True
        st["entry_path"]    = "tl_break"
        st["entry_price"]   = float(entry_dec)
        st["tp_level"]      = float(tp_dec)
        st["sl_price"]      = float(sl_dec)
        st["last_entry_ts"] = ts15
        update_sheet_tp(row, st["tp_level"])
        update_sheet_sl(row, st["sl_price"])

    save_state(all_state)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy   : <code>Trendline Break + Anti-Fakeout (LONG only)</code>\n"
    f"⏱ TL / S-R   : <code>1h closed bars (lookback={SWING_LOOKBACK}, slope×{SLOPE_MULT})</code>\n"
    f"⚡ Entry      : <code>15m close confirmation, limit BUY +{LIMIT_BUFFER_PCT}% buffer</code>\n"
    f"🔁 Scan       : <code>Every 15 minutes</code>\n"
    f"🧪 Filters    : <code>body-break, body≥{MIN_BODY_PCT:.0f}%, vol&gt;SMA{VOL_SMA_PERIOD}×{VOL_MULT}, "
    f"break≥{ATR_MULT}×ATR{ATR_PERIOD_15M}, cooldown={COOLDOWN_BARS}×15m</code>\n"
    f"🎯 TP         : <code>entry + {RR_RATIO}×risk</code>\n"
    f"🛑 SL         : <code>lastPL × (1 - {SL_BUFFER_PCT}%)</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in scan interval")
            time.sleep(SCAN_INTERVAL)
            continue

        state  = load_state()
        cycle += 1
        consecutive_errors = 0

        print(f"----- TRADE SCAN — CYCLE {cycle} -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            try:
                check_and_trade(symbol, row, df, state)
            except Exception as e:
                print(f"[ERROR] {symbol} check_and_trade failed: {e}")
                continue

        save_state(state)
        time.sleep(SCAN_INTERVAL)

    except Exception as e:
        consecutive_errors += 1
        print(f"BOT ERROR ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {e}")

        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            send_telegram(
                f"🚨 <b>Bot Crashed — Restarting</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ Error : <code>{str(e)[:200]}</code>\n"
                f"🔁 {consecutive_errors} consecutive errors — triggering restart"
            )
            raise SystemExit(1)

        time.sleep(60)