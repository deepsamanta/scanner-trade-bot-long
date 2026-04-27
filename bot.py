import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import os
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# =============================================================================
# STRATEGY PARAMETERS  (port of Pine Script "200 EMA Dual-Path Strategy")
#
# TIMEFRAME ARCHITECTURE:
#   - Primary analysis candles : 4h (EMA, slope, drop, crossover, trend)
#   - Entry confirmation       : 30m (first 30m close that satisfies trigger)
#   - Path C pivot confluence  : 4h + 12h (synthetic from 4h) + 1D
#   - Scan interval            : 30 minutes
# =============================================================================

# ─── CORE ─────────────────────────────────────────────────────────────────────
EMA_PERIOD           = 200
LOOKBACK             = 200
BELOW_PCT_MIN        = 70.0
TP_PCT               = 5
SL_BELOW_EMA_PCT     = 5.0

# ─── PATH A: REVERSAL RETEST ─────────────────────────────────────────────────
MAX_RETEST_BARS      = 20
PROXIMITY_PCT        = 0.3

# ─── SLOPE FILTER ────────────────────────────────────────────────────────────
USE_SLOPE_FILTER     = True
SLOPE_BARS           = 10
MIN_EMA_SLOPE_PCT    = -0.2

# ─── VOLUME FILTER ───────────────────────────────────────────────────────────
USE_VOLUME_FILTER    = True
VOL_LOOKBACK         = 20
VOL_MULTIPLIER       = 1.0
BREAKOUT_VOL_MULT    = 1.3

# ─── PATH B: MOMENTUM BREAKOUT ───────────────────────────────────────────────
USE_BREAKOUT_PATH    = True
MOMENTUM_LOOKBACK    = 5

# ─── CROSSOVER LOOKBACK (rescue filter) ──────────────────────────────────────
CROSS_LOOKBACK        = 5
MAX_EMA_DISTANCE_PCT  = 2.0

# ─── CROSSDOWN DROP FILTER (Paths A/B) ──────────────────────────────────────
USE_DROP_FILTER      = True
DROP_LOOKBACK        = 200
MIN_DROP_PCT         = 10.0

# ─── PATH C: SUPPORT BOUNCE (multi-timeframe pivot confluence) ──────────────
# Activation gate (4h EMA distance, signed):
#   Case 1 — price ABOVE EMA and within PATH_C_ABOVE_EMA_MAX_PCT
#   Case 2 — price BELOW EMA by at least PATH_C_BELOW_EMA_MIN_PCT
USE_PATH_C                 = True
PATH_C_ABOVE_EMA_MAX_PCT   = 4.0          # above EMA → must be within 4%
PATH_C_BELOW_EMA_MIN_PCT   = 8.0          # below EMA → must be 8% or more
PATH_C_ENABLED_TIMEFRAMES  = ["240", "12H_synth", "1D"]
PATH_C_CANDLES             = 600
PIVOT_STRENGTH             = 3
PIVOT_ZONE_PCT             = 1.0
MIN_TF_CONFLUENCE          = 2
PATH_C_MAX_WAIT_BARS       = 30
PATH_C_TOUCH_TOLERANCE_PCT = 0.5
PATH_C_SL_BELOW_ZONE_PCT   = 2.0

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY   = "240"
RESOLUTION_ENTRY     = "30"
CANDLE_SECONDS       = 4 * 3600
ENTRY_CANDLE_SECONDS = 30 * 60
SCAN_INTERVAL        = 1800

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
        sheet.update(f"B{row + 1}", [[str(value)]])
        print(f"[SHEET] Row {row + 1} col B -> {value}")
    except Exception as e:
        print("Sheet update error:", e)


def update_sheet_sl(row, value):
    try:
        sheet = get_sheet()
        if sheet is None:
            return
        sheet.update(f"C{row + 1}", [[str(value)]])
        print(f"[SHEET] Row {row + 1} col C (SL) -> {value}")
    except Exception as e:
        print("Sheet SL update error:", e)


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
        "waiting_retest":        False,
        "wait_start_candle_ts":  None,
        "path_c_armed":          False,
        "path_c_start_ts":       None,
        "path_c_zone_low":       None,
        "path_c_zone_high":      None,
        "path_c_zone_center":    None,
        "path_c_zone_touched":   False,
        "path_c_tf_count":       None,
        "path_c_gate_reason":    None,
        "in_position":           False,
        "entry_path":            None,
        "entry_price":           None,
        "tp_level":              None,
        "sl_price":              None,
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
# PRECISION
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATORS
# =====================================================

def compute_ema(closes, period):
    if len(closes) < period:
        return [None] * len(closes)
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    pad = [None] * (len(closes) - len(values))
    return pad + values


def had_recent_crossup(closes, emas, lookback):
    n = len(closes)
    if n < 2 or lookback < 1:
        return False, None
    for k in range(lookback):
        i_now  = n - 1 - k
        i_prev = i_now - 1
        if i_prev < 0:
            break
        c_now,  c_prev = closes[i_now], closes[i_prev]
        e_now,  e_prev = emas[i_now],   emas[i_prev]
        if e_now is None or e_prev is None:
            continue
        if c_prev <= e_prev and c_now > e_now:
            return True, k
    return False, None


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


def fetch_candles_tf(symbol, resolution_str, num_candles_needed):
    res_to_seconds = {
        "1":    60,
        "5":    5 * 60,
        "15":   15 * 60,
        "30":   30 * 60,
        "60":   60 * 60,
        "240":  4  * 60 * 60,
        "1D":   24 * 60 * 60,
    }

    if resolution_str == "12H_synth":
        needed_4h = num_candles_needed * 3 + 10
        candles_4h = fetch_candles_tf(symbol, "240", needed_4h)
        if len(candles_4h) < 3:
            return []

        synthetic = []
        for i in range(0, len(candles_4h) - 2, 3):
            group = candles_4h[i:i + 3]
            if len(group) != 3:
                continue
            synthetic.append({
                "time":   int(group[0]["time"]),
                "open":   float(group[0]["open"]),
                "high":   max(float(c["high"])   for c in group),
                "low":    min(float(c["low"])    for c in group),
                "close":  float(group[-1]["close"]),
                "volume": sum(float(c.get("volume", 0)) for c in group),
            })
        if len(synthetic) > num_candles_needed:
            synthetic = synthetic[-num_candles_needed:]
        return synthetic

    seconds_per_candle = res_to_seconds.get(resolution_str, CANDLE_SECONDS)

    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    fetch_seconds = (num_candles_needed + 50) * seconds_per_candle

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
        print(f"[CANDLES-TF {resolution_str}] {symbol} fetch error: {e}")
        return []


# =====================================================
# PIVOT DETECTION
# =====================================================

def find_pivots(highs, lows, strength):
    pivot_prices = []
    n = len(highs)
    if n < 2 * strength + 1:
        return pivot_prices

    for i in range(strength, n - strength):
        h_center = highs[i]
        l_center = lows[i]

        is_pivot_high = True
        for k in range(1, strength + 1):
            if highs[i - k] >= h_center or highs[i + k] >= h_center:
                is_pivot_high = False
                break
        if is_pivot_high:
            pivot_prices.append(h_center)
            continue

        is_pivot_low = True
        for k in range(1, strength + 1):
            if lows[i - k] <= l_center or lows[i + k] <= l_center:
                is_pivot_low = False
                break
        if is_pivot_low:
            pivot_prices.append(l_center)

    return pivot_prices


def cluster_pivots_to_zones(pivots_by_tf, proximity_pct):
    flat = []
    for tf, prices in pivots_by_tf.items():
        for p in prices:
            if p > 0:
                flat.append((p, tf))
    if not flat:
        return []

    flat.sort(key=lambda t: t[0])

    zones = []
    current_pivots = [flat[0]]
    current_center = flat[0][0]

    for price, tf in flat[1:]:
        gap_pct = abs(price - current_center) / current_center * 100.0
        if gap_pct <= proximity_pct:
            current_pivots.append((price, tf))
            current_center = sum(p for p, _ in current_pivots) / len(current_pivots)
        else:
            zones.append(_finalize_zone(current_pivots))
            current_pivots = [(price, tf)]
            current_center = price

    zones.append(_finalize_zone(current_pivots))
    return zones


def _finalize_zone(pivots):
    prices = [p for p, _ in pivots]
    tfs    = {tf for _, tf in pivots}
    return {
        "center": sum(prices) / len(prices),
        "low":    min(prices),
        "high":   max(prices),
        "tfs":    tfs,
        "pivots": pivots,
    }


def find_nearest_support_zone_below(symbol, current_price):
    pivots_by_tf = {}
    for tf in PATH_C_ENABLED_TIMEFRAMES:
        candles = fetch_candles_tf(symbol, tf, PATH_C_CANDLES)
        if len(candles) < 2 * PIVOT_STRENGTH + 1:
            print(f"[PATH-C] {symbol} TF {tf} — insufficient candles ({len(candles)}), skipping this TF")
            continue
        tf_highs = [float(c["high"]) for c in candles]
        tf_lows  = [float(c["low"])  for c in candles]
        pivots   = find_pivots(tf_highs, tf_lows, PIVOT_STRENGTH)
        pivots_by_tf[tf] = pivots

    if not pivots_by_tf:
        return None

    zones = cluster_pivots_to_zones(pivots_by_tf, PIVOT_ZONE_PCT)
    strong_zones = [z for z in zones if len(z["tfs"]) >= MIN_TF_CONFLUENCE]
    if not strong_zones:
        return None

    below_zones = [z for z in strong_zones if z["high"] < current_price]
    if not below_zones:
        return None

    nearest = max(below_zones, key=lambda z: z["center"])
    return nearest


# =====================================================
# 30M ENTRY CONFIRMATION
# =====================================================

def _fetch_closed_30m_candles(symbol, num_candles=6):
    candles = fetch_candles(symbol, num_candles + 2, RESOLUTION_ENTRY, ENTRY_CANDLE_SECONDS)
    if not candles:
        return []
    now_ms = int(time.time() * 1000)
    if len(candles) >= 1:
        last_ts_ms = int(candles[-1]["time"])
        elapsed_ms = now_ms - last_ts_ms
        if elapsed_ms < ENTRY_CANDLE_SECONDS * 1000:
            candles = candles[:-1]
    return candles


def confirm_30m_close_above(symbol, level):
    candles = _fetch_closed_30m_candles(symbol, 6)
    if not candles:
        return None
    last = candles[-1]
    if float(last["close"]) > level:
        return last
    return None


def confirm_30m_touch_and_close_above(symbol, zone_low, zone_high,
                                       touch_tolerance_pct=None):
    if touch_tolerance_pct is None:
        touch_tolerance_pct = PATH_C_TOUCH_TOLERANCE_PCT

    candles = _fetch_closed_30m_candles(symbol, 6)
    if not candles:
        return None

    last = candles[-1]
    last_close = float(last["close"])

    if last_close <= zone_high:
        return None

    touch_ceiling = zone_high * (1 + touch_tolerance_pct / 100.0)
    touch_floor   = zone_low  * (1 - touch_tolerance_pct / 100.0)
    for c in candles:
        c_low = float(c["low"])
        if touch_floor <= c_low <= touch_ceiling:
            return last
    return None


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

def get_quantity_step(symbol):
    try:
        pair = fut_pair(symbol)
        url  = (
            "https://api.coindcx.com/exchange/v1/derivatives/futures/data/instrument"
            f"?pair={pair}&margin_currency_short_name=USDT"
        )
        response   = requests.get(url, timeout=REQUEST_TIMEOUT)
        data       = response.json()
        instrument = data["instrument"]
        quantity_increment = Decimal(str(instrument["quantity_increment"]))
        min_quantity       = Decimal(str(instrument["min_quantity"]))
        return max(quantity_increment, min_quantity)
    except Exception:
        return Decimal("1")


def compute_qty(entry_price, symbol):
    step     = get_quantity_step(symbol)
    capital  = Decimal(str(CAPITAL_USDT))
    leverage = Decimal(str(LEVERAGE))
    exposure = capital * leverage
    raw_qty  = exposure / Decimal(str(entry_price))
    qty = (raw_qty / step).quantize(Decimal("1")) * step
    if qty <= 0:
        qty = step
    qty = qty.quantize(step)
    return float(qty)


# =====================================================
# PLACE LONG ORDER
# =====================================================

def place_long_order(symbol, entry_price, tp_price, sl_price, precision, entry_path):
    entry = round(entry_price, precision)
    tp    = round(tp_price,    precision)
    sl    = round(sl_price,    precision)

    qty = compute_qty(entry_price, symbol)

    tp_pct_display = round(((tp - entry) / entry) * 100, 2) if entry else 0
    sl_pct_display = round(((entry - sl) / entry) * 100, 2) if entry else 0

    print(
        f"[LONG TRADE] {symbol} BUY ({entry_path}) | Entry {entry} | "
        f"TP {tp} (+{tp_pct_display}%) | SL {sl} (-{sl_pct_display}%) | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "buy",
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry,
            "total_quantity":    qty,
            "leverage":          LEVERAGE,
            "take_profit_price": tp,
            "stop_loss_price":   sl,
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
        return False

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} long order not placed: {result}")
        send_telegram(
            f"❌ <b>LONG ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path    : <code>{entry_path}</code>\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>\n"
            f"🛑 SL      : <code>{sl}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return False

    send_telegram(
        f"🟢 <b>NEW LONG ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (+{tp_pct_display}%)\n"
        f"🛑 SL      : <code>{sl}</code>  (-{sl_pct_display}%)\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    pair = fut_pair(symbol)

    candles_needed = EMA_PERIOD + LOOKBACK + 30
    candles = fetch_candles(symbol, candles_needed)

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    # Drop in-progress 4h bar
    if len(candles) >= 2:
        now_ms           = int(time.time() * 1000)
        last_candle_time = int(candles[-1]["time"])
        bar_elapsed_ms   = now_ms - last_candle_time
        if bar_elapsed_ms < CANDLE_SECONDS * 1000:
            candles = candles[:-1]
            print(f"[{symbol}] Dropping in-progress bar ({bar_elapsed_ms/1000:.0f}s elapsed of {CANDLE_SECONDS}s)")

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient closed candles ({len(candles)})")
        return

    precision = get_precision(candles[-1]["close"])
    closes    = [float(c["close"])  for c in candles]
    highs     = [float(c["high"])   for c in candles]
    lows      = [float(c["low"])    for c in candles]
    volumes   = [float(c.get("volume", 0)) for c in candles]

    last_close = closes[-1]
    last_low   = lows[-1]
    last_ts    = int(candles[-1]["time"])

    # Indicators
    ema_values = compute_ema(closes, EMA_PERIOD)
    if (ema_values[-1] is None
            or ema_values[-1 - SLOPE_BARS] is None
            or ema_values[-1 - LOOKBACK] is None):
        print(f"[SKIP] {symbol} — EMA not ready deep enough")
        return

    ema_now    = ema_values[-1]
    ema_prev   = ema_values[-2]
    close_prev = closes[-2]

    # % below EMA
    below_count = 0
    for i in range(LOOKBACK):
        c = closes[-1 - i]
        e = ema_values[-1 - i]
        if e is None:
            continue
        if c < e:
            below_count += 1
    below_pct_actual = (below_count / LOOKBACK) * 100.0
    trend_qualifies  = below_pct_actual >= BELOW_PCT_MIN

    # EMA slope
    ema_slope_ref = ema_values[-1 - SLOPE_BARS]
    ema_slope_pct = ((ema_now - ema_slope_ref) / ema_slope_ref * 100.0) if ema_slope_ref else 0
    slope_ok      = (not USE_SLOPE_FILTER) or (ema_slope_pct >= MIN_EMA_SLOPE_PCT)

    # Volume
    vol_window = volumes[-VOL_LOOKBACK:]
    vol_avg    = (sum(vol_window) / VOL_LOOKBACK) if len(vol_window) == VOL_LOOKBACK else 0
    last_vol   = volumes[-1]
    vol_ok          = (not USE_VOLUME_FILTER) or (vol_avg > 0 and last_vol > vol_avg * VOL_MULTIPLIER)
    breakout_vol_ok = (vol_avg > 0) and (last_vol > vol_avg * BREAKOUT_VOL_MULT)

    # Drop filter (Paths A/B)
    n_candles      = len(closes)
    drop_start_idx = max(0, n_candles - DROP_LOOKBACK)
    window_lows    = lows[drop_start_idx:]
    window_highs   = highs[drop_start_idx:]
    lowest_in_window  = min(window_lows)  if window_lows  else 0
    highest_in_window = max(window_highs) if window_highs else 0

    crossdown_emas = []
    for i in range(drop_start_idx + 1, n_candles):
        e_prev_i = ema_values[i - 1]
        e_now_i  = ema_values[i]
        if e_prev_i is None or e_now_i is None:
            continue
        if closes[i - 1] >= e_prev_i and closes[i] < e_now_i:
            crossdown_emas.append((i, e_now_i))

    if crossdown_emas:
        best_xd_idx, upper_hinge = max(crossdown_emas, key=lambda t: t[1])
        drop_anchor_type   = "crossdown"
        crossdown_count    = len(crossdown_emas)
    else:
        upper_hinge        = highest_in_window
        drop_anchor_type   = "highest_high"
        crossdown_count    = 0

    if upper_hinge is None or upper_hinge <= 0 or lowest_in_window <= 0:
        drop_pct = 0.0
        drop_ok  = (not USE_DROP_FILTER)
    else:
        drop_pct = (upper_hinge - lowest_in_window) / upper_hinge * 100.0
        drop_ok  = (not USE_DROP_FILTER) or (drop_pct >= MIN_DROP_PCT)

    # Momentum
    price_rising = closes[-1] > closes[-1 - MOMENTUM_LOOKBACK]

    # Crossover detection
    cross_up_strict = (close_prev <= ema_prev) and (last_close > ema_now)
    cross_up_recent, cross_bars_ago = had_recent_crossup(closes, ema_values, CROSS_LOOKBACK)

    # Signed EMA distance: + above, − below
    ema_distance_pct  = ((last_close - ema_now) / ema_now * 100.0) if ema_now else 0
    not_overextended  = ema_distance_pct <= MAX_EMA_DISTANCE_PCT
    price_above_ema   = last_close > ema_now

    cross_valid = cross_up_strict or (
        cross_up_recent and price_above_ema and not_overextended
    )

    proximity_level = ema_now * (1 + PROXIMITY_PCT / 100)

    # Per-symbol state
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

        if last_close >= tp_stored:
            tp_hit    = True
            hit_kind  = "close"
            hit_price = last_close

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
                all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close)
            st["in_position"]          = True
            st["entry_path"]           = st.get("entry_path") or "unknown"
            st["entry_price"]          = entry_px
            st["tp_level"]             = round(entry_px * (1 + TP_PCT / 100), precision)
            st["sl_price"]             = round(ema_now  * (1 - SL_BELOW_EMA_PCT / 100), precision)
            st["waiting_retest"]       = False
            st["wait_start_candle_ts"] = None
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")

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
        all_state[symbol] = init_symbol_state()
        st = all_state[symbol]
        save_state(all_state)

    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # STRATEGY EVALUATION
    # =========================================================================
    vol_ratio = round(last_vol / vol_avg, 2) if vol_avg else 0

    if cross_up_strict:
        cross_log = "strict(now)"
    elif cross_up_recent:
        cross_log = f"recent({cross_bars_ago}b ago)"
    else:
        cross_log = "none"

    print(
        f"[SCAN] {symbol} | close={last_close} ema200={round(ema_now, precision)} | "
        f"distEMA={round(ema_distance_pct, 2)}% | "
        f"below%={round(below_pct_actual, 1)} (need >={BELOW_PCT_MIN}) | "
        f"slope%={round(ema_slope_pct, 3)} (need >={MIN_EMA_SLOPE_PCT}) | "
        f"vol={round(last_vol, 2)} avg={round(vol_avg, 2)} ratio={vol_ratio}x | "
        f"drop%={round(drop_pct, 2)} from {drop_anchor_type} (xd_count={crossdown_count}, need >={MIN_DROP_PCT}) | "
        f"cross={cross_log} crossValid={cross_valid} | "
        f"notOver={not_overextended} | "
        f"trendQ={trend_qualifies} slopeOK={slope_ok} volOK={vol_ok} dropOK={drop_ok} "
        f"priceRising={price_rising} waitingRetest={st['waiting_retest']} "
        f"pathC_armed={st.get('path_c_armed', False)}"
    )

    # =========================================================================
    # PATH A — WAITING RETEST
    # =========================================================================
    if st["waiting_retest"]:
        wait_start = st.get("wait_start_candle_ts")
        if wait_start is None:
            st["waiting_retest"] = False
            save_state(all_state)
        else:
            bars_waiting = max(0, int((last_ts - wait_start) // (CANDLE_SECONDS * 1000)))

            if bars_waiting >= 1 and last_close < ema_now:
                print(f"[INVALIDATED] {symbol} — close {last_close} < EMA {round(ema_now, precision)}")
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                save_state(all_state)
                return

            if bars_waiting > MAX_RETEST_BARS:
                print(f"[TIMEOUT] {symbol} — {bars_waiting} bars > max {MAX_RETEST_BARS}, clearing wait")
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                save_state(all_state)
                return

            retest_qualified = (bars_waiting >= 1
                                and last_low <= proximity_level
                                and last_close > ema_now)

            if retest_qualified:
                confirm_bar = confirm_30m_close_above(symbol, ema_now)

                if confirm_bar is None:
                    print(f"[PATH A] {symbol} — 4h retest qualified, awaiting 30m close above EMA")
                    save_state(all_state)
                    return

                print(f"[RETEST CONFIRMED] {symbol} — 4h qualified + 30m close {confirm_bar['close']} > EMA (Path A)")

                if get_position_by_pair(symbol) is not None:
                    print(f"[ABORT] {symbol} — position appeared just before placement")
                    return
                if has_open_order(symbol):
                    print(f"[ABORT] {symbol} — order appeared just before placement")
                    return

                entry_price = float(confirm_bar["close"])
                tp_price    = entry_price * (1 + TP_PCT / 100)
                sl_price    = ema_now     * (1 - SL_BELOW_EMA_PCT / 100)

                placed = place_long_order(symbol, entry_price, tp_price, sl_price, precision, "retest")
                if placed:
                    st["waiting_retest"]       = False
                    st["wait_start_candle_ts"] = None
                    st["in_position"]          = True
                    st["entry_path"]           = "retest"
                    st["entry_price"]          = round(entry_price, precision)
                    st["tp_level"]             = round(tp_price,    precision)
                    st["sl_price"]             = round(sl_price,    precision)
                    update_sheet_tp(row, st["tp_level"])
                    update_sheet_sl(row, st["sl_price"])

                save_state(all_state)
                return

            print(f"[WAIT] {symbol} — bars_waiting={bars_waiting}/{MAX_RETEST_BARS}")

    # =========================================================================
    # PATH A — ARM NEW REVERSAL SETUP
    # =========================================================================
    new_setup = (trend_qualifies
                 and cross_valid
                 and slope_ok
                 and vol_ok
                 and drop_ok
                 and not st["waiting_retest"])

    if new_setup:
        cross_detail = "strict" if cross_up_strict else f"rescued ({cross_bars_ago}b ago)"
        print(
            f"[SETUP ARMED] {symbol} — trendQ ✓ cross:{cross_detail} ✓ "
            f"slope ✓ vol ✓ drop:{round(drop_pct, 2)}% ✓ → waiting retest"
        )
        st["waiting_retest"]       = True
        st["wait_start_candle_ts"] = last_ts
        send_telegram(
            f"🟡 <b>REVERSAL SETUP ARMED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Close     : <code>{last_close}</code>\n"
            f"📊 EMA200    : <code>{round(ema_now, precision)}</code>\n"
            f"📉 Below %   : <code>{round(below_pct_actual, 1)}%</code>\n"
            f"📈 Slope %   : <code>{round(ema_slope_pct, 3)}%</code>\n"
            f"📦 Vol ratio : <code>{vol_ratio}x</code>\n"
            f"📉 Drop %    : <code>{round(drop_pct, 2)}% (from {drop_anchor_type}, {crossdown_count} crossdowns)</code>\n"
            f"🔀 Cross     : <code>{cross_detail}</code>\n"
            f"📏 Dist EMA  : <code>{round(ema_distance_pct, 2)}%</code>\n"
            f"🎯 Proximity : <code>{round(proximity_level, precision)}</code>\n"
            f"⌛ Waiting up to {MAX_RETEST_BARS} × 4h candles for retest"
        )
        save_state(all_state)
        return

    # =========================================================================
    # PATH B — MOMENTUM BREAKOUT
    # =========================================================================
    if USE_BREAKOUT_PATH:
        breakout_qualified = ((not trend_qualifies)
                              and cross_valid
                              and price_rising
                              and breakout_vol_ok
                              and drop_ok)
        if breakout_qualified:
            cross_detail = "strict" if cross_up_strict else f"rescued ({cross_bars_ago}b ago)"

            confirm_bar = confirm_30m_close_above(symbol, ema_now)
            if confirm_bar is None:
                print(f"[BREAKOUT] {symbol} — 4h qualified (cross:{cross_detail}, drop:{round(drop_pct, 2)}%), awaiting 30m close above EMA")
                save_state(all_state)
                return

            print(f"[BREAKOUT] {symbol} — 4h qualified + 30m close {confirm_bar['close']} > EMA (Path B, cross:{cross_detail}, drop:{round(drop_pct, 2)}%)")

            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            entry_price = float(confirm_bar["close"])
            tp_price    = entry_price * (1 + TP_PCT / 100)
            sl_price    = ema_now     * (1 - SL_BELOW_EMA_PCT / 100)

            placed = place_long_order(symbol, entry_price, tp_price, sl_price, precision, "breakout")
            if placed:
                st["waiting_retest"]       = False
                st["wait_start_candle_ts"] = None
                st["in_position"]          = True
                st["entry_path"]           = "breakout"
                st["entry_price"]          = round(entry_price, precision)
                st["tp_level"]             = round(tp_price,    precision)
                st["sl_price"]             = round(sl_price,    precision)
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["sl_price"])

            save_state(all_state)
            return

    # =========================================================================
    # PATH C — SUPPORT BOUNCE
    # =========================================================================
    # Activation gate (4h EMA distance):
    #   Case 1 — price ABOVE EMA and within PATH_C_ABOVE_EMA_MAX_PCT (default 4%)
    #   Case 2 — price BELOW EMA by ≥ PATH_C_BELOW_EMA_MIN_PCT      (default 8%)
    # When gate passes, find nearest multi-TF confluence support zone below
    # current price and wait for a 30m reclaim above zone_high.
    # =========================================================================
    if USE_PATH_C:
        # ── STAGE 1: already armed — watch for bounce ──────────────────────
        if st.get("path_c_armed"):
            zone_low    = st.get("path_c_zone_low")
            zone_high   = st.get("path_c_zone_high")
            zone_center = st.get("path_c_zone_center")
            armed_ts    = st.get("path_c_start_ts")

            if None in (zone_low, zone_high, zone_center, armed_ts):
                print(f"[PATH-C] {symbol} — incomplete armed state, clearing")
                st["path_c_armed"] = False
                save_state(all_state)
            else:
                bars_waiting = max(0, int((last_ts - armed_ts) // (CANDLE_SECONDS * 1000)))

                touch_margin     = zone_low * (1 - PATH_C_TOUCH_TOLERANCE_PCT / 100.0)
                broken_threshold = zone_low * (1 - 2.0 / 100.0)

                if not st.get("path_c_zone_touched"):
                    if last_low <= zone_high * (1 + PATH_C_TOUCH_TOLERANCE_PCT / 100.0) \
                       and last_low >= touch_margin:
                        st["path_c_zone_touched"] = True
                        print(f"[PATH-C] {symbol} — zone TOUCHED at low {last_low} (zone {zone_low}–{zone_high})")

                if last_close < broken_threshold:
                    print(f"[PATH-C] {symbol} — zone BROKEN (close {last_close} < {broken_threshold:.6f}), cancelling")
                    send_telegram(
                        f"❌ <b>PATH C CANCELLED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Close    : <code>{last_close}</code>\n"
                        f"🧱 Zone     : <code>{round(zone_low, precision)} – {round(zone_high, precision)}</code>\n"
                        f"⚠️ Reason   : zone broken (price fell &gt;2% below zone)"
                    )
                    st["path_c_armed"]        = False
                    st["path_c_zone_low"]     = None
                    st["path_c_zone_high"]    = None
                    st["path_c_zone_center"]  = None
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = None
                    st["path_c_tf_count"]     = None
                    st["path_c_gate_reason"]  = None
                    save_state(all_state)
                    return

                if bars_waiting > PATH_C_MAX_WAIT_BARS:
                    print(f"[PATH-C] {symbol} — wait timed out ({bars_waiting} > {PATH_C_MAX_WAIT_BARS})")
                    st["path_c_armed"]        = False
                    st["path_c_zone_low"]     = None
                    st["path_c_zone_high"]    = None
                    st["path_c_zone_center"]  = None
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = None
                    st["path_c_tf_count"]     = None
                    st["path_c_gate_reason"]  = None
                    save_state(all_state)
                    return

                confirm_bar = confirm_30m_touch_and_close_above(symbol, zone_low, zone_high)

                if confirm_bar is not None:
                    entry_price_30m = float(confirm_bar["close"])
                    print(f"[PATH-C] {symbol} — RECLAIM CONFIRMED on 30m close {entry_price_30m} > zone_high {zone_high}")

                    if get_position_by_pair(symbol) is not None:
                        print(f"[ABORT] {symbol} — position appeared just before placement")
                        return
                    if has_open_order(symbol):
                        print(f"[ABORT] {symbol} — order appeared just before placement")
                        return

                    entry_price = entry_price_30m
                    tp_price    = entry_price * (1 + TP_PCT / 100)
                    sl_price    = zone_low * (1 - PATH_C_SL_BELOW_ZONE_PCT / 100)

                    placed = place_long_order(symbol, entry_price, tp_price, sl_price, precision, "support_bounce")
                    if placed:
                        st["path_c_armed"]        = False
                        st["path_c_zone_low"]     = None
                        st["path_c_zone_high"]    = None
                        st["path_c_zone_center"]  = None
                        st["path_c_zone_touched"] = False
                        st["path_c_start_ts"]     = None
                        st["path_c_tf_count"]     = None
                        st["path_c_gate_reason"]  = None
                        st["in_position"]         = True
                        st["entry_path"]          = "support_bounce"
                        st["entry_price"]         = round(entry_price, precision)
                        st["tp_level"]            = round(tp_price,    precision)
                        st["sl_price"]            = round(sl_price,    precision)
                        update_sheet_tp(row, st["tp_level"])
                        update_sheet_sl(row, st["sl_price"])

                    save_state(all_state)
                    return

                touched_str = "touched" if st.get("path_c_zone_touched") else "awaiting touch"
                print(f"[PATH-C] {symbol} — waiting ({bars_waiting}/{PATH_C_MAX_WAIT_BARS}b, {touched_str}), "
                      f"zone {round(zone_low, precision)}–{round(zone_high, precision)}, close {last_close}")
                save_state(all_state)
                return

        # ── STAGE 2: not armed — evaluate whether to arm a new zone ────────
        # New activation gate (replaces drop-based gate):
        #   above_within → price above EMA, distance ≤ PATH_C_ABOVE_EMA_MAX_PCT
        #   below_deep   → price below EMA by ≥ PATH_C_BELOW_EMA_MIN_PCT
        above_within = (0 < ema_distance_pct <= PATH_C_ABOVE_EMA_MAX_PCT)
        below_deep   = (ema_distance_pct <= -PATH_C_BELOW_EMA_MIN_PCT)
        path_c_gate_ok = above_within or below_deep

        if path_c_gate_ok:
            gate_reason = ("above_ema_within_4pct" if above_within
                           else "below_ema_by_8pct_plus")

            zone = find_nearest_support_zone_below(symbol, last_close)

            if zone is not None and zone["high"] < last_close:
                tf_count   = len(zone["tfs"])
                tfs_str    = ",".join(sorted(zone["tfs"]))
                zone_low   = zone["low"]
                zone_high  = zone["high"]
                zone_cent  = zone["center"]
                dist_pct   = (last_close - zone_cent) / last_close * 100.0

                print(f"[PATH-C] {symbol} — ARMING zone {round(zone_low, precision)}–{round(zone_high, precision)} "
                      f"(center {round(zone_cent, precision)}, {tf_count} TFs: {tfs_str}, "
                      f"{round(dist_pct, 2)}% below close, gate={gate_reason}, "
                      f"distEMA={round(ema_distance_pct, 2)}%)")

                st["path_c_armed"]        = True
                st["path_c_zone_low"]     = zone_low
                st["path_c_zone_high"]    = zone_high
                st["path_c_zone_center"]  = zone_cent
                st["path_c_zone_touched"] = False
                st["path_c_start_ts"]     = last_ts
                st["path_c_tf_count"]     = tf_count
                st["path_c_gate_reason"]  = gate_reason

                send_telegram(
                    f"🟠 <b>PATH C ARMED (support bounce) — {symbol}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━\n"
                    f"📍 Close      : <code>{last_close}</code>\n"
                    f"📊 EMA200     : <code>{round(ema_now, precision)}</code>\n"
                    f"📏 Dist EMA   : <code>{round(ema_distance_pct, 2)}%</code>\n"
                    f"🚪 Gate       : <code>{gate_reason}</code>\n"
                    f"🧱 Zone low   : <code>{round(zone_low, precision)}</code>\n"
                    f"🧱 Zone high  : <code>{round(zone_high, precision)}</code>\n"
                    f"📊 Zone cent  : <code>{round(zone_cent, precision)}</code>\n"
                    f"⏬ Dist below : <code>{round(dist_pct, 2)}%</code>\n"
                    f"🪢 Confluence : <code>{tf_count} TFs ({tfs_str})</code>\n"
                    f"⌛ Waiting up to {PATH_C_MAX_WAIT_BARS} × 4h bars for bounce"
                )
                save_state(all_state)
                return

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
    f"📐 Strategy   : <code>200 EMA Triple-Path (Reversal + Breakout + Support Bounce)</code>\n"
    f"⏱ Analysis   : <code>4h primary (closed bars only)</code>\n"
    f"⚡ Entry      : <code>30m close confirmation (all paths)</code>\n"
    f"🔁 Scan       : <code>Every 30 minutes</code>\n"
    f"🅰️ Path A    : <code>{BELOW_PCT_MIN}% below EMA + cross + slope≥{MIN_EMA_SLOPE_PCT}% + vol×{VOL_MULTIPLIER} + drop≥{MIN_DROP_PCT}% → wait retest (≤{MAX_RETEST_BARS} × 4h bars)</code>\n"
    f"🅱️ Path B    : <code>cross + close&gt;close[{MOMENTUM_LOOKBACK}] + vol×{BREAKOUT_VOL_MULT} + drop≥{MIN_DROP_PCT}% when trend NOT qualifying</code>\n"
    f"🆎 Path C    : <code>(price above EMA ≤{PATH_C_ABOVE_EMA_MAX_PCT}% OR below EMA ≥{PATH_C_BELOW_EMA_MIN_PCT}%) + nearest multi-TF pivot zone below price ({MIN_TF_CONFLUENCE}/{len(PATH_C_ENABLED_TIMEFRAMES)} TFs) → 30m reclaim (≤{PATH_C_MAX_WAIT_BARS} bars)</code>\n"
    f"🔀 Cross      : <code>strict OR within last {CROSS_LOOKBACK} bars (if price still above EMA and ≤{MAX_EMA_DISTANCE_PCT}% away)</code>\n"
    f"📉 Drop A/B   : <code>max-EMA across all crossdowns (or highest-high if never crossed) → lowest-low across last {DROP_LOOKBACK} × 4h bars must be ≥{MIN_DROP_PCT}%</code>\n"
    f"🧱 Pivots     : <code>N={PIVOT_STRENGTH} each side, ±{PIVOT_ZONE_PCT}% zone band, TFs: 4h + 12h synth + 1D</code>\n"
    f"🎯 TP         : <code>{TP_PCT}% fixed above entry</code>\n"
    f"🛑 SL         : <code>EMA × (1 - {SL_BELOW_EMA_PCT}%) for Paths A/B  •  zone_low × (1 - {PATH_C_SL_BELOW_ZONE_PCT}%) for Path C</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 30 min")
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