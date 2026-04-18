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
# =============================================================================

# ─── CORE ─────────────────────────────────────────────────────────────────────
EMA_PERIOD           = 200
LOOKBACK             = 200      # candles to count below EMA
BELOW_PCT_MIN        = 55.0     # min % of last LOOKBACK candles below EMA
TP_PCT               = 2.5      # Take Profit % (fixed above entry)
SL_BELOW_EMA_PCT     = 1.0      # SL = EMA × (1 - this/100)

# ─── PATH A: REVERSAL RETEST ─────────────────────────────────────────────────
MAX_RETEST_BARS      = 20       # max 15m bars to wait for retest after arming
PROXIMITY_PCT        = 0.3      # retest zone = EMA × (1 + this/100)

# ─── SLOPE FILTER ────────────────────────────────────────────────────────────
USE_SLOPE_FILTER     = True
SLOPE_BARS           = 10       # % change of EMA over this many bars
MIN_EMA_SLOPE_PCT    = 0.1      # min EMA slope % to qualify

# ─── VOLUME FILTER ───────────────────────────────────────────────────────────
USE_VOLUME_FILTER    = True
VOL_LOOKBACK         = 20
VOL_MULTIPLIER       = 1.0      # for reversal path
BREAKOUT_VOL_MULT    = 1.3      # for breakout path

# ─── PATH B: MOMENTUM BREAKOUT ───────────────────────────────────────────────
USE_BREAKOUT_PATH    = True
MOMENTUM_LOOKBACK    = 5        # close > close[N] bars ago

# ─── TRAILING STOP ───────────────────────────────────────────────────────────
USE_TRAILING         = True
TRAIL_ACTIVATE_PCT   = 1.0      # activate trail after profit % reached
TRAIL_DISTANCE_PCT   = 0.8      # trail distance %

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION           = "15"     # CoinDCX 15-minute candles
CANDLE_SECONDS       = 15 * 60
SCAN_INTERVAL        = 900      # 15 minutes in seconds

# ─── REQUEST TIMEOUTS (seconds) ──────────────────────────────────────────────
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10

# ─── GSPREAD RE-AUTH INTERVAL ────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60

# ─── LOCAL STATE FILE (trailing stop + wait-for-retest persistence) ──────────
STATE_FILE           = "bot_state.json"
# =============================================================================


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
# LOCAL STATE PERSISTENCE (wait-retest + trailing stop tracking)
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
        "wait_start_candle_ts":  None,    # ms timestamp of the candle when armed
        "in_position":           False,
        "entry_path":            None,    # "retest" or "breakout"
        "entry_price":           None,
        "tp_level":              None,
        "initial_sl":            None,
        "current_sl":            None,
        "highest_since_entry":   None,
        "trail_active":          False,
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
    """Returns EMA series aligned so ema_values[-1] pairs with closes[-1]."""
    if len(closes) < period:
        return []
    multiplier = 2 / (period + 1)
    ema        = sum(closes[:period]) / period
    values     = [ema]
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
        values.append(ema)
    # Pad left so index alignment with closes is clean
    pad = [None] * (len(closes) - len(values))
    return pad + values


# =====================================================
# CANDLE FETCH (15-minute resolution)
# =====================================================

def fetch_candles(symbol, num_candles_needed):
    pair_api = fut_pair(symbol)
    url      = "https://public.coindcx.com/market_data/candlesticks"
    now      = int(time.time())
    # +50 as safety buffer
    fetch_seconds = (num_candles_needed + 50) * CANDLE_SECONDS

    params = {
        "pair":       pair_api,
        "from":       now - fetch_seconds,
        "to":         now,
        "resolution": RESOLUTION,
        "pcode":      "f",
    }
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        data     = response.json().get("data", [])
        candles  = sorted(data, key=lambda x: x["time"])
        return candles
    except Exception as e:
        print(f"[CANDLES] {symbol} fetch error: {e}")
        return []


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


def list_orders_by_pair(symbol, statuses):
    """Returns orders for a pair filtered by status list (e.g. ["open","initial","partially_filled"])"""
    try:
        body = {
            "timestamp":                  int(time.time() * 1000),
            "page":                       1,
            "size":                       50,
            "margin_currency_short_name": "USDT",
            "status":                     statuses,
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        orders   = response.json()

        pair = fut_pair(symbol)
        if not isinstance(orders, list):
            return []
        return [o for o in orders if o.get("pair") == pair]
    except Exception as e:
        print(f"[ORDERS] {symbol} fetch error: {e}")
        return []


def has_open_order(symbol):
    """Entry-type open order still on book (not TP/SL)."""
    try:
        orders = list_orders_by_pair(symbol, ["initial", "open", "partially_filled"])
        return len(orders) > 0
    except Exception as e:
        print(f"has_open_order error ({symbol}):", e)
        return False


def get_stop_loss_orders(symbol):
    """Untriggered stop_market / stop_limit orders attached to the position."""
    orders = list_orders_by_pair(symbol, ["untriggered"])
    return [o for o in orders if o.get("order_type") in ("stop_market", "stop_limit")]


def cancel_order(order_id):
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "id":        order_id,
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/orders/cancel"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        result   = response.json()
        return result.get("status") == 200 or result.get("code") == 200
    except Exception as e:
        print(f"[CANCEL] order {order_id} error: {e}")
        return False


def create_position_sl(position_id, new_sl_price, precision):
    """Create a stop-loss order for the position via create_tpsl endpoint (SL only)."""
    try:
        body = {
            "timestamp": int(time.time() * 1000),
            "id":        position_id,
            "stop_loss": {
                "stop_price": str(round(new_sl_price, precision)),
                "order_type": "stop_market",
            },
        }
        payload, headers = sign_request(body)
        url      = BASE_URL + "/exchange/v1/derivatives/futures/positions/create_tpsl"
        response = requests.post(url, data=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        return response.json()
    except Exception as e:
        print(f"[SL CREATE] position {position_id} error: {e}")
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
# TRAILING STOP — cancel existing SL orders, place new one
# =====================================================

def update_trailing_sl_on_exchange(symbol, position, new_sl, precision):
    try:
        existing_sl_orders = get_stop_loss_orders(symbol)
        for o in existing_sl_orders:
            cancel_order(o["id"])
            time.sleep(0.3)

        position_id = position.get("id")
        result = create_position_sl(position_id, new_sl, precision)
        if not result:
            return False

        sl_info = result.get("stop_loss") if isinstance(result, dict) else None
        if isinstance(sl_info, dict) and sl_info.get("success") is False:
            print(f"[SL UPDATE] {symbol} create failed: {sl_info.get('error')}")
            return False
        return True

    except Exception as e:
        print(f"[SL UPDATE] {symbol} error: {e}")
        return False


# =====================================================
# MAIN PER-SYMBOL LOGIC
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    pair = fut_pair(symbol)

    # ─── Fetch enough 15m candles ────────────────────────────────────────────
    candles_needed = EMA_PERIOD + LOOKBACK + 30
    candles = fetch_candles(symbol, candles_needed)

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    precision = get_precision(candles[-1]["close"])
    closes    = [float(c["close"])  for c in candles]
    highs     = [float(c["high"])   for c in candles]
    lows      = [float(c["low"])    for c in candles]
    volumes   = [float(c.get("volume", 0)) for c in candles]

    last_close = closes[-1]
    last_high  = highs[-1]
    last_low   = lows[-1]
    last_ts    = int(candles[-1]["time"])   # milliseconds

    # ─── Indicators ──────────────────────────────────────────────────────────
    ema_values = compute_ema(closes, EMA_PERIOD)
    # Validate we have EMA available at the bars we need
    if ema_values[-1] is None or ema_values[-1 - SLOPE_BARS] is None \
       or ema_values[-1 - LOOKBACK] is None:
        print(f"[SKIP] {symbol} — EMA not ready deep enough")
        return

    ema_now    = ema_values[-1]
    ema_prev   = ema_values[-2]
    close_prev = closes[-2]

    # % below EMA over last LOOKBACK bars (including current)
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

    # EMA slope %
    ema_slope_ref = ema_values[-1 - SLOPE_BARS]
    ema_slope_pct = ((ema_now - ema_slope_ref) / ema_slope_ref * 100.0) if ema_slope_ref else 0
    slope_ok      = (not USE_SLOPE_FILTER) or (ema_slope_pct >= MIN_EMA_SLOPE_PCT)

    # Volume SMA + checks
    vol_window = volumes[-VOL_LOOKBACK:]
    vol_avg    = (sum(vol_window) / VOL_LOOKBACK) if len(vol_window) == VOL_LOOKBACK else 0
    last_vol   = volumes[-1]
    vol_ok          = (not USE_VOLUME_FILTER) or (vol_avg > 0 and last_vol > vol_avg * VOL_MULTIPLIER)
    breakout_vol_ok = (vol_avg > 0) and (last_vol > vol_avg * BREAKOUT_VOL_MULT)

    # Momentum (close > close N bars ago)
    price_rising = closes[-1] > closes[-1 - MOMENTUM_LOOKBACK]

    # Crossover: prev close <= prev ema AND current close > current ema
    cross_up = (close_prev <= ema_prev) and (last_close > ema_now)

    # Proximity zone
    proximity_level = ema_now * (1 + PROXIMITY_PCT / 100)

    # ─── Get/init per-symbol state ───────────────────────────────────────────
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    # --- Case A: We have an active position on the exchange -----------------
    if position is not None:
        # Rebuild state if missing (e.g. after bot restart)
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close)
            st["in_position"]          = True
            st["entry_path"]           = st.get("entry_path") or "unknown"
            st["entry_price"]          = entry_px
            st["tp_level"]             = round(entry_px * (1 + TP_PCT / 100), precision)
            st["initial_sl"]           = round(ema_now  * (1 - SL_BELOW_EMA_PCT / 100), precision)
            existing_sl_trigger        = position.get("stop_loss_trigger")
            st["current_sl"]           = float(existing_sl_trigger) if existing_sl_trigger else st["initial_sl"]
            st["highest_since_entry"]  = max(last_high, entry_px)
            st["trail_active"]         = ((last_high - entry_px) / entry_px * 100) >= TRAIL_ACTIVATE_PCT
            st["waiting_retest"]       = False
            st["wait_start_candle_ts"] = None
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")

        # ── Trailing-stop management ─────────────────────────────────────────
        if USE_TRAILING and st["entry_price"]:
            st["highest_since_entry"] = max(float(st["highest_since_entry"] or 0), last_high)

            profit_pct = (st["highest_since_entry"] - st["entry_price"]) / st["entry_price"] * 100.0
            if profit_pct >= TRAIL_ACTIVATE_PCT:
                st["trail_active"] = True

            if st["trail_active"]:
                new_sl = round(st["highest_since_entry"] * (1 - TRAIL_DISTANCE_PCT / 100), precision)
                old_sl = float(st.get("current_sl") or 0)

                if new_sl > old_sl:
                    print(
                        f"[TRAIL] {symbol} — raising SL {old_sl} -> {new_sl} "
                        f"(high {st['highest_since_entry']}, profit {round(profit_pct, 2)}%)"
                    )
                    ok = update_trailing_sl_on_exchange(symbol, position, new_sl, precision)
                    if ok:
                        st["current_sl"] = new_sl
                        update_sheet_sl(row, new_sl)
                        send_telegram(
                            f"🔄 <b>TRAIL SL RAISED — {symbol}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📈 Highest  : <code>{round(st['highest_since_entry'], precision)}</code>\n"
                            f"🛑 New SL   : <code>{new_sl}</code>\n"
                            f"📊 Profit % : <code>{round(profit_pct, 2)}%</code>"
                        )
                    else:
                        print(f"[TRAIL] {symbol} — SL update failed, will retry next scan")

        save_state(all_state)
        return

    # --- Case B: Position just closed (state says in_position but exchange doesn't) ---
    if st.get("in_position"):
        print(f"[POSITION CLOSED] {symbol} — cleaning up state")
        send_telegram(
            f"✅ <b>POSITION CLOSED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🛤 Path     : <code>{st.get('entry_path')}</code>\n"
            f"📍 Entry    : <code>{st.get('entry_price')}</code>\n"
            f"🎯 TP was   : <code>{st.get('tp_level')}</code>\n"
            f"🛑 Last SL  : <code>{st.get('current_sl')}</code>"
        )
        all_state[symbol] = init_symbol_state()
        st = all_state[symbol]
        save_state(all_state)

    # --- Skip if an entry limit order is still on the book ------------------
    if has_open_order(symbol):
        print(f"[OPEN ORDER] {symbol} — unfilled entry order on book, skipping")
        return

    # =========================================================================
    # STRATEGY EVALUATION
    # =========================================================================
    ema_distance_pct = ((last_close - ema_now) / ema_now * 100.0) if ema_now else 0

    print(
        f"[SCAN] {symbol} | close={last_close} ema200={round(ema_now, precision)} | "
        f"below%={round(below_pct_actual, 1)} (need >={BELOW_PCT_MIN}) | "
        f"slope%={round(ema_slope_pct, 3)} (need >={MIN_EMA_SLOPE_PCT}) | "
        f"vol={round(last_vol, 2)} avg={round(vol_avg, 2)} | "
        f"crossUp={cross_up} trendQ={trend_qualifies} slopeOK={slope_ok} "
        f"volOK={vol_ok} waitingRetest={st['waiting_retest']}"
    )

    # =========================================================================
    # WAITING RETEST STATE (Path A has already been armed)
    # =========================================================================
    if st["waiting_retest"]:
        wait_start = st.get("wait_start_candle_ts")
        if wait_start is None:
            st["waiting_retest"] = False
            save_state(all_state)
            return

        bars_waiting = max(0, int((last_ts - wait_start) // (CANDLE_SECONDS * 1000)))

        # Invalidated: close back below EMA after >= 1 bar
        if bars_waiting >= 1 and last_close < ema_now:
            print(f"[INVALIDATED] {symbol} — close {last_close} < EMA {round(ema_now, precision)}")
            st["waiting_retest"]       = False
            st["wait_start_candle_ts"] = None
            save_state(all_state)
            return

        # Timed out
        if bars_waiting > MAX_RETEST_BARS:
            print(f"[TIMEOUT] {symbol} — {bars_waiting} bars > max {MAX_RETEST_BARS}, clearing wait")
            st["waiting_retest"]       = False
            st["wait_start_candle_ts"] = None
            save_state(all_state)
            return

        # Retest confirmed: low dipped into proximity zone and close stayed above EMA
        retest_confirmed = (bars_waiting >= 1
                            and last_low <= proximity_level
                            and last_close > ema_now)

        if retest_confirmed:
            print(f"[RETEST CONFIRMED] {symbol} — entering long (Path A)")

            # Final guard
            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            entry_price = last_close
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
                st["initial_sl"]           = round(sl_price,    precision)
                st["current_sl"]           = round(sl_price,    precision)
                st["highest_since_entry"]  = last_high
                st["trail_active"]         = False
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["current_sl"])

            save_state(all_state)
            return

        # Still waiting
        print(f"[WAIT] {symbol} — bars_waiting={bars_waiting}/{MAX_RETEST_BARS}")
        save_state(all_state)
        return

    # =========================================================================
    # PATH A — ARM NEW REVERSAL SETUP
    # =========================================================================
    new_setup = trend_qualifies and cross_up and slope_ok and vol_ok
    if new_setup:
        print(f"[SETUP ARMED] {symbol} — trendQ ✓ crossUp ✓ slope ✓ vol ✓ → waiting retest")
        st["waiting_retest"]       = True
        st["wait_start_candle_ts"] = last_ts
        send_telegram(
            f"🟡 <b>REVERSAL SETUP ARMED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Close     : <code>{last_close}</code>\n"
            f"📊 EMA200    : <code>{round(ema_now, precision)}</code>\n"
            f"📉 Below %   : <code>{round(below_pct_actual, 1)}%</code>\n"
            f"📈 Slope %   : <code>{round(ema_slope_pct, 3)}%</code>\n"
            f"📦 Vol ratio : <code>{round(last_vol / vol_avg, 2) if vol_avg else 0}x</code>\n"
            f"🎯 Proximity : <code>{round(proximity_level, precision)}</code>\n"
            f"⌛ Waiting up to {MAX_RETEST_BARS} × 15m candles for retest"
        )
        save_state(all_state)
        return

    # =========================================================================
    # PATH B — MOMENTUM BREAKOUT (when trend does NOT qualify)
    # =========================================================================
    if USE_BREAKOUT_PATH:
        breakout_entry = (not trend_qualifies) and cross_up and price_rising and breakout_vol_ok
        if breakout_entry:
            print(f"[BREAKOUT] {symbol} — entering long (Path B)")

            # Final guard
            if get_position_by_pair(symbol) is not None:
                print(f"[ABORT] {symbol} — position appeared just before placement")
                return
            if has_open_order(symbol):
                print(f"[ABORT] {symbol} — order appeared just before placement")
                return

            entry_price = last_close
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
                st["initial_sl"]           = round(sl_price,    precision)
                st["current_sl"]           = round(sl_price,    precision)
                st["highest_since_entry"]  = last_high
                st["trail_active"]         = False
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["current_sl"])

            save_state(all_state)
            return

    # No action
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
    f"📐 Strategy   : <code>200 EMA Dual-Path (Reversal + Breakout)</code>\n"
    f"⏱ Timeframe  : <code>15 Min</code>\n"
    f"🅰️ Path A    : <code>{BELOW_PCT_MIN}% below EMA + crossUp + slope≥{MIN_EMA_SLOPE_PCT}% + vol×{VOL_MULTIPLIER} → wait retest (≤{MAX_RETEST_BARS} bars)</code>\n"
    f"🅱️ Path B    : <code>crossUp + close>close[{MOMENTUM_LOOKBACK}] + vol×{BREAKOUT_VOL_MULT} when NOT qualifying</code>\n"
    f"🎯 TP         : <code>{TP_PCT}% fixed above entry</code>\n"
    f"🛑 SL         : <code>EMA × (1 - {SL_BELOW_EMA_PCT}%)</code>\n"
    f"🪜 Trail      : <code>Activate at {TRAIL_ACTIVATE_PCT}% profit, distance {TRAIL_DISTANCE_PCT}%</code>\n"
    f"💰 Capital    : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
    f"🕐 Scanning every 15 minutes..."
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 15 min")
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