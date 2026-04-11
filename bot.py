import pandas as pd
import requests
import time
import hmac
import hashlib
import json
import gspread

from decimal import Decimal, getcontext
from google.oauth2.service_account import Credentials

from config import COINDCX_KEY, COINDCX_SECRET, CAPITAL_USDT, LEVERAGE, SHEET_ID, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

getcontext().prec = 28
BASE_URL = "https://api.coindcx.com"

# ─── TUNEABLE CONSTANTS ────────────────────────────────────────────────────────
EMA_PERIOD       = 21
MIN_RR           = 0.05

# ─── DYNAMIC TP SETTINGS ──────────────────────────────────────────────────────
SWING_LOOKBACK     = 3        # candles each side to confirm a swing high
RESISTANCE_CANDLES = 200      # how many candles to scan for swing body highs
MIN_TP_PCT         = 0.012    # minimum TP: 1.2% above entry
MAX_TP_PCT         = 0.05     # maximum TP: 5% above entry (cap)
FALLBACK_TP_PCT    = 0.01     # fallback fixed TP if no resistance found: 1%

# ─── STOP LOSS ────────────────────────────────────────────────────────────────
SL_PCT           = 0.055      # 5.5% fixed below entry

# ─── LINEAR REGRESSION SLOPE ──────────────────────────────────────────────────
LINREG_LOOKBACK  = 4          # candles for slope curve (matches Pine _slopeLook = 4)

# ─── 4H TREND FILTER ──────────────────────────────────────────────────────────
# Before placing a long, verify that the 4H timeframe also shows bullish momentum.
# We fetch the last N 4H candles and compute a linear regression slope on their
# closes using the exact same formula as the 30-min slope. The slope must be
# positive (upward-bending curve on 4H) — if not, the trade is skipped.
LINREG_4H_LOOKBACK = 4        # number of 4H candles to use for the slope check

# ─── CONSOLIDATION FILTER ─────────────────────────────────────────────────────
# Before a valid long, price must have spent enough time BELOW the EMA
# (dipped / consolidated under it). This confirms a proper retest, not a
# mid-air entry on an already extended move.
FILTER_LOOKBACK  = 30         # how many candles to check (Pine _filterLook = 25)
MIN_BELOW_PERC   = 55         # min % of those candles that must be below EMA

# ─── EMA PROXIMITY FILTER ─────────────────────────────────────────────────────
# Even after the crossover, don't buy if price has already run too far above EMA.
MAX_EMA_DISTANCE_PCT = 0.02   # 2% max distance above EMA

# ─── SCAN INTERVAL ────────────────────────────────────────────────────────────
SCAN_INTERVAL    = 900        # 15 minutes in seconds

# ─── REQUEST TIMEOUTS (seconds) ───────────────────────────────────────────────
REQUEST_TIMEOUT  = 15
TELEGRAM_TIMEOUT = 10

# ─── GSPREAD RE-AUTH INTERVAL ─────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60
# ──────────────────────────────────────────────────────────────────────────────


# =====================================================
# GOOGLE SHEETS — with periodic re-auth
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


# =====================================================
# READ / WRITE SHEET
# =====================================================

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
# TELEGRAM NOTIFICATION
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
# PRECISION HELPER
# =====================================================

def get_precision(raw_candle_close):
    s = str(raw_candle_close)
    if "." in s:
        return len(s.split(".")[1])
    return 0


# =====================================================
# INDICATOR HELPERS
# =====================================================

def compute_ema(closes, period):
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    return values


def compute_linreg_slope(values):
    """
    Exact Python port of the Pine Script f_true_slope() function:

        for i = 0 to len - 1
            x = len - i        # x counts DOWN: len, len-1, ..., 1
            y = src[i]         # src[0] = most recent bar
        slope = (n·ΣXY - ΣX·ΣY) / (n·ΣX2 - (ΣX)^2)

    'values' must be ordered newest -> oldest (index 0 = most recent).
    Call as: compute_linreg_slope(list(reversed(ema_values[-N:])))

    A positive slope means the EMA curve is genuinely bending upward.
    """
    n      = len(values)
    sum_x  = 0.0
    sum_y  = 0.0
    sum_xy = 0.0
    sum_x2 = 0.0

    for i in range(n):
        x       = n - i          # x: n, n-1, ..., 1  (matches Pine Script)
        y       = values[i]      # values[0] = most recent
        sum_x  += x
        sum_y  += y
        sum_xy += x * y
        sum_x2 += x * x

    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 0.0

    return (n * sum_xy - sum_x * sum_y) / denom


# =====================================================
# 4H TREND FILTER — linear regression slope on closes
# =====================================================

def check_4h_slope_positive(symbol):
    """
    Fetches the last LINREG_4H_LOOKBACK + a small buffer of 4-hour candles
    and computes a linear regression slope on their closing prices using the
    exact same formula as the 30-min EMA slope.

    Returns True  → slope is positive  (4H trend is rising, allow trade)
    Returns False → slope is flat/negative (4H trend not confirmed, skip trade)

    The closes are passed newest-first to match compute_linreg_slope()'s
    expected ordering (index 0 = most recent bar).
    """
    try:
        pair_api = fut_pair(symbol)
        url      = "https://public.coindcx.com/market_data/candlesticks"
        now      = int(time.time())

        # Fetch enough 4H candles: lookback + a small safety buffer
        fetch_seconds = (LINREG_4H_LOOKBACK + 5) * 4 * 3600

        params = {
            "pair":       pair_api,
            "from":       now - fetch_seconds,
            "to":         now,
            "resolution": "240",   # 240 minutes = 4 hours
            "pcode":      "f",
        }

        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = sorted(response.json()["data"], key=lambda x: x["time"])

        if len(candles) < LINREG_4H_LOOKBACK:
            print(f"[4H FILTER] {symbol} — not enough 4H candles ({len(candles)}), skipping filter (allow trade)")
            return True

        # Take the last LINREG_4H_LOOKBACK closes, newest first
        recent_closes = [float(c["close"]) for c in candles[-LINREG_4H_LOOKBACK:]]
        recent_closes_newest_first = list(reversed(recent_closes))

        slope_4h = compute_linreg_slope(recent_closes_newest_first)
        is_positive = slope_4h > 0

        return is_positive, slope_4h

    except Exception as e:
        print(f"[4H FILTER] {symbol} — fetch error: {e} — skipping filter (allow trade)")
        return True, None


# =====================================================
# DYNAMIC TP — NEAREST RESISTANCE (SWING HIGH BODY)
# =====================================================

def find_nearest_resistance(candles, entry_price, precision):
    """
    Scans the last RESISTANCE_CANDLES candles for swing highs based on
    candle BODY TOPS — max(open, close) — ignoring wicks.

    A swing body high = candle whose body top is greater than SWING_LOOKBACK
    candles on both its left and right side.

    Returns the nearest (lowest) body-based swing high above entry, capped
    between MIN_TP_PCT and MAX_TP_PCT. Falls back to FALLBACK_TP_PCT (1%)
    if none found.
    """
    recent_candles = candles[-RESISTANCE_CANDLES:]
    body_tops = [max(float(c["open"]), float(c["close"])) for c in recent_candles]
    n  = len(body_tops)
    lb = SWING_LOOKBACK

    swing_body_highs = []

    for i in range(lb, n - lb):
        current = body_tops[i]
        left    = body_tops[i - lb : i]
        right   = body_tops[i + 1 : i + lb + 1]

        if all(current > b for b in left) and all(current > b for b in right):
            swing_body_highs.append(current)

    min_tp_price = entry_price * (1 + MIN_TP_PCT)
    max_tp_price = entry_price * (1 + MAX_TP_PCT)

    valid = [b for b in swing_body_highs if min_tp_price <= b <= max_tp_price]

    if valid:
        nearest = min(valid)
        tp      = round(nearest, precision)
        print(f"[BODY RESISTANCE TP] Found swing body high at {tp} (from {len(valid)} candidates)")
        return tp, "body_resistance"

    fallback_tp = round(entry_price * (1 + FALLBACK_TP_PCT), precision)
    print(f"[BODY RESISTANCE TP] No valid swing body high found — using fallback {FALLBACK_TP_PCT*100:.1f}% TP = {fallback_tp}")
    return fallback_tp, "fallback"


# =====================================================
# OPEN POSITIONS
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
        return [p for p in positions if float(p.get("active_pos", 0)) != 0]
    except Exception as e:
        print("get_open_positions error:", e)
        return []


def get_position_tp(symbol):
    try:
        positions = get_open_positions()
        pair = fut_pair(symbol)
        for pos in positions:
            if pos.get("pair") == pair:
                tp = pos.get("take_profit_trigger")
                if tp:
                    return float(tp)
        return None
    except Exception:
        return None


def get_position_entry(symbol):
    try:
        positions = get_open_positions()
        pair = fut_pair(symbol)
        for pos in positions:
            if pos.get("pair") == pair:
                ep = pos.get("entry_price") or pos.get("avg_price")
                if ep:
                    return float(ep)
        return None
    except Exception:
        return None


# =====================================================
# OPEN ORDER CHECK
# =====================================================

def has_open_order(symbol):
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "page":                       1,
            "size":                       50,
            "margin_currency_short_name": "USDT",
            "status":                     ["initial", "open", "partially_filled"],
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        orders   = response.json()

        pair = fut_pair(symbol)
        if isinstance(orders, list):
            for o in orders:
                if o.get("pair") == pair:
                    return True
        return False

    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


# =====================================================
# TP CHECK — recent high over last 15 minutes
# =====================================================

def get_recent_high(symbol):
    """
    Fetches 1-min candles over the last 15 minutes (matching SCAN_INTERVAL)
    to check if price touched TP via a wick since the last cycle.
    """
    try:
        pair_api = fut_pair(symbol)
        url  = "https://public.coindcx.com/market_data/candlesticks"
        now  = int(time.time())
        params = {
            "pair":       pair_api,
            "from":       now - SCAN_INTERVAL,   # last 15 minutes
            "to":         now,
            "resolution": "1",
            "pcode":      "f",
        }
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = response.json()["data"]
        highs    = [float(c["high"]) for c in candles]
        return max(highs)
    except Exception:
        return None


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
# PLACE LONG ORDER — with dynamic TP
# =====================================================

def place_long_order(symbol, entry_price, precision, candles):
    entry   = round(entry_price, precision)
    sl_base = round(entry * (1 - SL_PCT), precision)

    tp, tp_type = find_nearest_resistance(candles, entry, precision)

    reward = tp - entry
    risk   = entry - sl_base

    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>LONG SIGNAL SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason  : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>  ({tp_type})\n"
            f"🛑 SL      : <code>{sl_base}</code>  (below entry)"
        )
        return None, None

    qty = compute_qty(entry_price, symbol)

    tp_pct = round(((tp - entry) / entry) * 100, 2)

    print(
        f"[LONG TRADE] {symbol} BUY | Entry {entry} | TP {tp} (+{tp_pct}% — {tp_type}) "
        f"| SL {sl_base} (-{SL_PCT*100:.0f}%) | RR {round(reward / risk, 2)} | Qty {qty}"
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
            "stop_loss_price":   sl_base,
        },
    }

    payload, headers = sign_request(body)
    response = requests.post(
        BASE_URL + "/exchange/v1/derivatives/futures/orders/create",
        data=payload,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    result = response.json()

    print(f"[API] {symbol} response: {result}")

    if "order" not in result and not isinstance(result, list):
        print(f"[ERROR] {symbol} long order not placed: {result}")
        send_telegram(
            f"❌ <b>LONG ORDER REJECTED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Entry   : <code>{entry}</code>\n"
            f"🎯 TP      : <code>{tp}</code>  ({tp_type})\n"
            f"🛑 SL      : <code>{sl_base}</code>\n"
            f"⚠️ Response : <code>{str(result)[:200]}</code>"
        )
        return None, None

    try:
        order        = result[0] if isinstance(result, list) else result["order"]
        tp_confirmed = order.get("take_profit_price", tp)
    except Exception:
        tp_confirmed = tp

    send_telegram(
        f"🟢 <b>NEW LONG (BUY) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (+{tp_pct}% — {tp_type})\n"
        f"🛑 SL      : <code>{sl_base}</code>  (-{int(SL_PCT * 100)}% below entry)\n"
        f"📊 RR      : <code>{round(reward / risk, 2)}</code>\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
    )

    return tp_confirmed, sl_base


# =====================================================
# MAIN LOGIC
# =====================================================

def check_and_trade(symbol, row, df):

    pair     = fut_pair(symbol)
    pair_api = pair
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())

    params = {
        "pair":       pair_api,
        "from":       now - 360000,
        "to":         now,
        "resolution": "30",
        "pcode":      "f",
    }

    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        candles  = sorted(response.json()["data"], key=lambda x: x["time"])
    except Exception as e:
        print(f"[ERROR] {symbol} candle fetch failed: {e}")
        return

    # Need enough candles for EMA + linreg lookback
    if len(candles) < EMA_PERIOD + LINREG_LOOKBACK + 1:
        return

    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    last_close = float(candles[-1]["close"])

    ema_values = compute_ema(closes, EMA_PERIOD)
    del closes

    ema_now  = ema_values[-1]
    ema_prev = ema_values[-2]   # one bar back — used for crossover detection

    # ── Linear regression slope — Pine Script formula (newest first) ──────────
    # Pass last LINREG_LOOKBACK EMA values in newest->oldest order
    ema_window         = list(reversed(ema_values[-LINREG_LOOKBACK:]))
    ema_slope          = compute_linreg_slope(ema_window)
    ema_slope_prev     = compute_linreg_slope(list(reversed(ema_values[-LINREG_LOOKBACK - 1:-1])))
    ema_slope_positive = ema_slope > 0
    slope_dir          = "positive" if ema_slope_positive else "negative"

    # ── Consolidation Filter (Pine _isConsolidating) ──────────────────────────
    # At least MIN_BELOW_PERC % of the last FILTER_LOOKBACK candles must have
    # closed BELOW the EMA — confirms price dipped/retested before breakout.
    bars_below = sum(
        1 for i in range(1, FILTER_LOOKBACK + 1)
        if float(candles[-(i + 1)]["close"]) < float(ema_values[-(i + 1)])
    ) if len(candles) > FILTER_LOOKBACK + 1 else 0
    perc_below       = (bars_below / FILTER_LOOKBACK) * 100
    is_consolidating = perc_below >= MIN_BELOW_PERC

    # =========================================================================
    # GATE 1 — Open position check
    # =========================================================================
    positions = get_open_positions()
    for pos in positions:
        if pos.get("pair") == pair:
            print(f"[ACTIVE TRADE] {symbol} — position open on CoinDCX, skipping")
            tp_live = get_position_tp(symbol)
            if tp_live:
                update_sheet_tp(row, tp_live)
            return

    # =========================================================================
    # GATE 2 — Open order check
    # =========================================================================
    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled order on book, skipping")
        return

    # ── TP monitoring ─────────────────────────────────────────────────────────
    tp_raw = df.iloc[row, 1]

    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} TP COMPLETED")
        return

    try:
        tp_stored = float(tp_raw)

        if last_close >= tp_stored:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} price {last_close} >= TP {tp_stored}")
            return

        recent_high = get_recent_high(symbol)
        if recent_high and recent_high >= tp_stored:
            update_sheet_tp(row, "TP COMPLETED")
            print(f"[TP HIT] {symbol} recent high {recent_high} >= TP {tp_stored}")
            return

    except Exception:
        pass

    # =========================================================================
    # STRATEGY CONDITIONS
    # =========================================================================

    # ── Trigger logic — mirrors Pine Script scenarios ────────────────────────
    #
    # Scenario 1: EMA slope JUST turned positive (crossover above 0)
    #             AND price is already above EMA
    #             → EMA curve just started bending up, price already broke out
    #
    # Scenario 2: Price JUST crossed above EMA (close crossover)
    #             AND slope is already positive
    #             → Fresh EMA breakout while trend is already pointing up
    #
    # Both scenarios also require:
    #   - Consolidation: enough bars recently below EMA (proper retest)
    #   - Proximity: price hasn't run too far above EMA already

    curve_just_turned_positive = (ema_slope > 0) and (ema_slope_prev <= 0)
    price_just_crossed_above   = (last_close > ema_now) and (float(candles[-2]["close"]) <= ema_prev)

    scenario1 = curve_just_turned_positive and (last_close > ema_now)
    scenario2 = price_just_crossed_above and ema_slope_positive

    ema_distance_pct = (last_close - ema_now) / ema_now if ema_now > 0 else 0
    price_near_ema   = ema_distance_pct <= MAX_EMA_DISTANCE_PCT

    # ── Fetch 4H slope here so it appears in every [SCAN] line ───────────────
    slope_4h_ok, slope_4h_val = check_4h_slope_positive(symbol)
    slope_4h_str = (
        f"{round(slope_4h_val, 6)} ({'✅' if slope_4h_ok else '❌'})"
        if slope_4h_val is not None else "N/A"
    )

    print(
        f"[SCAN] {symbol} | Price {last_close} | 21 EMA {round(ema_now, precision)} | "
        f"slope30m {round(ema_slope, precision)} ({slope_dir}) | "
        f"slope4H {slope_4h_str} | "
        f"below_EMA {round(perc_below, 1)}% (need {MIN_BELOW_PERC}%) | "
        f"dist {round(ema_distance_pct * 100, 2)}% | "
        f"s1={scenario1} s2={scenario2} consol={is_consolidating} near={price_near_ema}"
    )

    if not (scenario1 or scenario2):
        return

    if not is_consolidating:
        print(f"[SKIP] {symbol} — consolidation filter failed ({round(perc_below,1)}% below EMA, need {MIN_BELOW_PERC}%)")
        return

    if not price_near_ema:
        print(f"[SKIP] {symbol} — price {round(ema_distance_pct*100,2)}% above EMA, exceeds {MAX_EMA_DISTANCE_PCT*100}% max — waiting for retest")
        return

    trig = "Scenario1 (curve turned +)" if scenario1 else "Scenario2 (price crossover)"
    print(
        f"[SIGNAL] {symbol} | {trig} ✓ "
        f"| slope {round(ema_slope, precision)} ✓ "
        f"| {round(perc_below,1)}% bars below EMA ✓ "
        f"| dist {round(ema_distance_pct*100,2)}% ✓ "
        f"| Price {last_close} | EMA {round(ema_now, precision)} "
        f"| SL {round(last_close * (1 - SL_PCT), precision)}"
    )

    # =========================================================================
    # GATE 3 — 4H LINEAR REGRESSION SLOPE FILTER
    # The last 4 four-hour candle closes must form a positive linear regression
    # slope (same formula as the 30-min slope). This ensures the higher
    # timeframe trend is rising before we enter on the 30-min signal.
    # =========================================================================
    if not slope_4h_ok:
        print(
            f"[SKIP] {symbol} — 4H linreg slope is negative/flat over last "
            f"{LINREG_4H_LOOKBACK} candles (slope={slope_4h_str}) — higher timeframe not bullish, skipping"
        )
        return

    # =========================================================================
    # FINAL GUARD — re-check everything right before placing
    # =========================================================================
    live_positions = get_open_positions()
    for pos in live_positions:
        if pos.get("pair") == pair:
            print(f"[SKIP] {symbol} — open position detected just before placement, aborting")
            return

    if has_open_order(symbol):
        print(f"[SKIP] {symbol} — unfilled open order detected just before placement, aborting")
        return

    if last_close <= ema_now:
        print(
            f"[SKIP] {symbol} — last close {last_close} not above "
            f"21 EMA {round(ema_now, precision)} at placement, aborting"
        )
        return

    tp_confirmed, sl_placed = place_long_order(
        symbol, last_close, precision, candles
    )
    if tp_confirmed:
        update_sheet_tp(row, tp_confirmed)
    if sl_placed:
        update_sheet_sl(row, sl_placed)


# =====================================================
# MAIN LOOP
# =====================================================

cycle              = 0
consecutive_errors = 0
MAX_CONSECUTIVE_ERRORS = 10

send_telegram(
    f"✅ <b>Bot Started</b>\n"
    f"━━━━━━━━━━━━━━━━━━\n"
    f"📐 Strategy  : <code>21 EMA Breakout Long</code>\n"
    f"⏱ Timeframe  : <code>30 Min</code>\n"
    f"📈 Entry     : <code>Scenario1 (slope crossover 0 + price>EMA) OR Scenario2 (price crossover EMA + slope>0) | consol {MIN_BELOW_PERC}% bars below | dist <{int(MAX_EMA_DISTANCE_PCT*100)}%</code>\n"
    f"✅ Filter    : <code>Dipped coins added manually to sheet</code>\n"
    f"📊 4H Filter : <code>LinReg slope on last {LINREG_4H_LOOKBACK} × 4H closes must be positive</code>\n"
    f"🎯 TP        : <code>Dynamic — nearest swing body resistance (1.5%–8%) | fallback 1%</code>\n"
    f"🛑 SL        : <code>{int(SL_PCT * 100)}% fixed below entry</code>\n"
    f"💰 Capital   : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
    f"🕐 Scanning every 15 minutes..."
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 15 min")
            time.sleep(SCAN_INTERVAL)
            continue

        cycle += 1
        consecutive_errors = 0

        print(f"----- TRADE SCAN — CYCLE {cycle} -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_and_trade(symbol, row, df)

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