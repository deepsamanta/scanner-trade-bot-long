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
# STRATEGY PARAMETERS  (Trendline Break + Anti-Fakeout — LONG ONLY)
#
# TIMEFRAME ARCHITECTURE:
#   - Trendline / S-R levels   : 1h closed candles
#   - Entry confirmation       : 1h closed candles  (breakout & retest)
#   - Scan interval            : 15 minutes  (just to catch a fresh 1h close
#                                 quickly; strategy logic only runs once per
#                                 new closed 1h bar via last_processed_1h_ts)
#
# ENTRY PATHS:
#   - tl_break   : fresh cross above upper_lvl on a 1h close + all
#                  anti-fakeout filters pass on the SAME 1h candle.
#   - tl_retest  : when a fresh cross fires but filters fail, the bot ARMS
#                  a retest watch. If a later 1h candle's wick touches
#                  upper_lvl and the same candle closes back above with a
#                  bullish body, it enters long. Captures the case where
#                  the broken trendline acts as new support.
# =============================================================================

# ─── TRENDLINE TOUCH CONFIRMATION ───────────────────────────────────────────
# Before either path can fire, the trendline must have been TOUCHED at least
# this many times since its anchor pivot. A "touch" = a 1h bar whose high
# reached within TOUCH_TOLERANCE_PCT of the line but whose close stayed below
# (i.e. price tried to break and failed). Consecutive bars in the same
# approach are collapsed: price must drop ≥ TOUCH_GAP_PCT below the line
# before the next attempt counts as a separate touch.
MIN_TL_TOUCHES       = 5       # require ≥ N failed-break attempts on the line
TOUCH_TOLERANCE_PCT  = 0.5     # high within ±X% of line counts as approaching
TOUCH_GAP_PCT        = 1.0     # must drop X% below line to "reset" for next touch

# ─── TRENDLINE CALC (1h) ─────────────────────────────────────────────────────
SWING_LOOKBACK   = 14          # pivot lookback (each side)
SLOPE_MULT       = 1.0         # slope multiplier
ATR_PERIOD_TL    = 14          # ATR period for trendline slope (1h)

# ─── RISK ────────────────────────────────────────────────────────────────────
TP_PCT           = 3.0         # fixed take-profit: entry × (1 + TP_PCT/100)
SL_BELOW_TL_PCT  = 1.5         # SL placed X% below the upper trendline (broken resistance turned support)

# ─── ANTI-FAKEOUT FILTERS (applied on 1h entry candle, tl_break path) ───────
USE_BODY_BREAK   = True
USE_STRONG_BAR   = True
MIN_BODY_PCT     = 50.0        # body ≥ X% of range
USE_VOLUME       = True
VOL_MULT         = 1.2         # vol > SMA(20) × mult
VOL_SMA_PERIOD   = 20
USE_ATR_DIST     = True
ATR_MULT         = 0.25        # break ≥ X × ATR(14) beyond TL
USE_COOLDOWN     = True
COOLDOWN_BARS    = 5           # 1h bars between entries (= 5 hours)

# ─── RETEST PATH (tl_retest) ─────────────────────────────────────────────────
USE_RETEST_PATH      = True    # master switch for the retest path
RETEST_MAX_BARS      = 5       # max 1h bars to wait for retest (= 5 hours)
RETEST_TOUCH_PCT     = 0.3     # 1h wick must come within ±X% of upper_lvl
RETEST_MIN_BODY_PCT  = 30      # bounce-candle body ≥ X% of range
RETEST_CANCEL_PCT    = 1.0     # cancel retest if 1h close falls X% below TL

# ─── CANDLE COUNTS ───────────────────────────────────────────────────────────
TL_CANDLES_1H    = 500         # 1h candles fetched for trendline calc

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION_PRIMARY    = "60"            # 1h
CANDLE_SECONDS        = 60 * 60         # 1h in seconds
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
        "in_position":          False,
        "entry_path":           None,
        "entry_price":          None,
        "tp_level":             None,
        "sl_price":             None,
        "last_entry_ts":        0,        # ms timestamp of last entry (cooldown anchor)
        "last_processed_1h_ts": 0,        # ms ts of last 1h bar strategy ran on (dedup)

        # ── Retest path state ──────────────────────────────
        "retest_armed":       False,
        "retest_armed_ts":    0,        # ms ts of the 1h bar that armed
        "retest_upper_lvl":   None,     # upper_lvl snapshot at arming
        "retest_last_ph":     None,     # for telegram/log only
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


def compute_trendlines(opens, highs, lows, closes, length, mult):
    """
    LuxAlgo-style Trendlines with Breaks (long-side fields).

    Pivot detection uses the CANDLE BODY (max(open,close) for highs,
    min(open,close) for lows), so wicks/spikes never anchor a trendline.
    ATR (used for slope decay) still uses true high/low.

    Walks every bar, replicating Pine semantics:
      slope_ph := ph ? slope : slope_ph
      upper    := ph ? ph    : upper - slope_ph
      (and the symmetric pair for the lower line)

    Returns final-bar values:
        upper_lvl = upper - slope_ph * length     # the projected break level
        last_ph   = most recent pivot high body-top  (resistance)
        last_pl   = most recent pivot low  body-bottom (support — context)
    """
    n = len(closes)
    atr_arr = compute_atr(highs, lows, closes, ATR_PERIOD_TL)

    # Body extremes — used for pivot detection (ignores wicks)
    body_tops    = [max(opens[i], closes[i]) for i in range(n)]
    body_bottoms = [min(opens[i], closes[i]) for i in range(n)]

    cur_upper = 0.0
    cur_lower = 0.0
    cur_slope_ph = 0.0
    cur_slope_pl = 0.0
    cur_last_ph = None
    cur_last_pl = None
    have_upper = False
    have_lower = False

    # Per-bar projected upper level (used for touch counting)
    upper_lvl_history = [None] * n
    last_ph_bar_idx   = None     # bar index where most recent pivot high was confirmed

    for i in range(n):
        ph = None
        pl = None

        # Pivot at index (i-length) is confirmed at bar i — using BODIES
        if i >= 2 * length:
            c    = i - length
            bt_c = body_tops[c]
            bb_c = body_bottoms[c]
            is_ph = True
            is_pl = True
            for k in range(1, length + 1):
                if body_tops[c - k]    >= bt_c or body_tops[c + k]    >= bt_c:
                    is_ph = False
                if body_bottoms[c - k] <= bb_c or body_bottoms[c + k] <= bb_c:
                    is_pl = False
                if not is_ph and not is_pl:
                    break
            if is_ph:
                ph = bt_c   # anchor trendline at the body TOP
            if is_pl:
                pl = bb_c   # anchor trendline at the body BOTTOM

        slope = (atr_arr[i] / length * mult) if (atr_arr[i] is not None) else 0.0

        # Upper trendline (descending resistance from pivot body-tops)
        if ph is not None:
            cur_slope_ph    = slope
            cur_upper       = ph
            cur_last_ph     = ph
            have_upper      = True
            last_ph_bar_idx = i             # remember when this anchor confirmed
        elif have_upper:
            cur_upper -= cur_slope_ph

        # Lower trendline (ascending support from pivot body-bottoms)
        if pl is not None:
            cur_slope_pl = slope
            cur_lower    = pl
            cur_last_pl  = pl
            have_lower   = True
        elif have_lower:
            cur_lower += cur_slope_pl

        # Snapshot the projected upper level at THIS bar, for touch counting
        if have_upper:
            upper_lvl_history[i] = cur_upper - cur_slope_ph * length

    upper_lvl = (cur_upper - cur_slope_ph * length) if have_upper else None
    lower_lvl = (cur_lower + cur_slope_pl * length) if have_lower else None

    # ── Count touches on the CURRENT upper trendline ───────────────────────
    # Walk bars after the last pivot-high confirmation. A touch attempt is a
    # bar whose high reached the line (within TOUCH_TOLERANCE_PCT) but whose
    # close stayed below. Consecutive touch bars are collapsed via a
    # state machine: we only count a NEW touch after price has dropped at
    # least TOUCH_GAP_PCT below the line in between.
    touches_above = 0
    if last_ph_bar_idx is not None and last_ph_bar_idx + 1 < n:
        in_approach = False
        for j in range(last_ph_bar_idx + 1, n):
            lvl = upper_lvl_history[j]
            if lvl is None or lvl <= 0:
                continue
            approach_thresh = lvl * (1 - TOUCH_TOLERANCE_PCT / 100)
            gap_thresh      = lvl * (1 - TOUCH_GAP_PCT       / 100)

            approached = highs[j]  >= approach_thresh
            close_low  = closes[j] <  lvl

            if approached and close_low:
                if not in_approach:
                    touches_above += 1
                    in_approach = True
                # else: same approach attempt continues — don't double-count
            else:
                # Reset only when price has clearly dropped away from the line
                if highs[j] < gap_thresh:
                    in_approach = False

    return {
        "upper_lvl":     upper_lvl,
        "lower_lvl":     lower_lvl,
        "last_ph":       cur_last_ph,
        "last_pl":       cur_last_pl,
        "touches_above": touches_above,
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


def extract_tp_sl(obj):
    """
    Pull (tp, sl) out of a CoinDCX position/order dict.
    Tries multiple field names CoinDCX has used. Returns (tp, sl) as floats;
    either may be None if the field is missing or zero.
    """
    if not isinstance(obj, dict):
        return None, None
    tp_keys = ["take_profit_price", "take_profit_trigger", "tp_price"]
    sl_keys = ["stop_loss_price", "stop_loss_trigger", "sl_price"]

    def _pick(keys):
        for k in keys:
            v = obj.get(k)
            if v is None or v == "" or v == "0" or v == 0:
                continue
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                continue
        return None

    return _pick(tp_keys), _pick(sl_keys)


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

def place_long_order(symbol, entry_price, tp_price, sl_price, precision, entry_path, signal_info=None):
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

    sig_block = ""
    if signal_info:
        if entry_path == "tl_retest":
            sig_block = (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"<b>📊 Retest signal:</b>\n"
                f"📍 c1h        : <code>{signal_info.get('c1h')}</code>\n"
                f"⏬ low1h      : <code>{signal_info.get('l1h')}</code>\n"
                f"📈 upperLvl   : <code>{signal_info.get('upper_lvl')}</code>\n"
                f"🟢 lastPL     : <code>{signal_info.get('last_pl')}</code>\n"
                f"🔴 lastPH     : <code>{signal_info.get('last_ph')}</code>\n"
                f"🪢 Touches    : <code>{signal_info.get('touches_above')}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"🧱 armed @TL  : <code>{signal_info.get('armed_upper_lvl')}</code>\n"
                f"⏳ bars armed : <code>{signal_info.get('bars_armed')}</code> / {RETEST_MAX_BARS}\n"
                f"<b>✅ Retest conditions:</b>\n"
                f"• wick touched line : <code>True</code> "
                f"(low {signal_info.get('l1h')} within ±{RETEST_TOUCH_PCT}% of upperLvl)\n"
                f"• close above line  : <code>True</code> "
                f"(c1h {signal_info.get('c1h')} &gt; upperLvl)\n"
                f"• bullish bar       : <code>True</code> "
                f"(c1h &gt; o1h)\n"
                f"• body strong       : <code>True</code> "
                f"(body={signal_info.get('body_pct')}% ≥ {RETEST_MIN_BODY_PCT}%)\n"
                f"• cooldown          : <code>True</code> "
                f"({signal_info.get('bars_since_last')} bars, need ≥{COOLDOWN_BARS})"
            )
        else:
            # tl_break
            sig_block = (
                f"\n━━━━━━━━━━━━━━━━━━\n"
                f"<b>📊 Signal that fired:</b>\n"
                f"📍 c1h        : <code>{signal_info.get('c1h')}</code>\n"
                f"⏪ prev_c1h   : <code>{signal_info.get('prev_c1h')}</code>\n"
                f"📈 upperLvl   : <code>{signal_info.get('upper_lvl')}</code>\n"
                f"🟢 lastPL     : <code>{signal_info.get('last_pl')}</code>\n"
                f"🔴 lastPH     : <code>{signal_info.get('last_ph')}</code>\n"
                f"🪢 Touches    : <code>{signal_info.get('touches_above')}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"<b>✅ Filters passed:</b>\n"
                f"• fresh cross : <code>True</code> "
                f"(prev≤{signal_info.get('upper_lvl')} &amp; c1h&gt;{signal_info.get('upper_lvl')})\n"
                f"• body break  : <code>True</code> "
                f"(min(o,c)={signal_info.get('min_oc')} &gt; upperLvl)\n"
                f"• strong bar  : <code>True</code> "
                f"(body={signal_info.get('body_pct')}% ≥ {signal_info.get('min_body_pct')}%)\n"
                f"• volume      : <code>True</code> "
                f"(vol={signal_info.get('vol')} &gt; SMA20×{signal_info.get('vol_mult')}={signal_info.get('vol_threshold')})\n"
                f"• ATR distance: <code>True</code> "
                f"(c1h &gt; upperLvl + {signal_info.get('atr_mult')}×ATR = {signal_info.get('atr_threshold')})\n"
                f"• cooldown    : <code>True</code> "
                f"({signal_info.get('bars_since_last')} bars since last entry, need ≥{signal_info.get('cooldown_bars')})"
            )

    send_telegram(
        f"🟢 <b>NEW LONG ({entry_path.upper()}) — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry   : <code>{entry}</code>\n"
        f"🎯 TP      : <code>{tp}</code>  (+{tp_pct_display}%)\n"
        f"🛑 SL      : <code>{sl}</code>  (-{sl_pct_display}%)\n"
        f"📦 Qty     : <code>{qty}</code>\n"
        f"💰 Margin  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>"
        f"{sig_block}"
    )
    return True


# =====================================================
# MAIN PER-SYMBOL LOGIC  (Trendline Break — long only, 1h confirmation)
# =====================================================

def check_and_trade(symbol, row, df, all_state):
    # ─── Fetch 1h for trendline / S-R + signal evaluation ────
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
    opens_1h  = [float(c["open"])  for c in candles_1h]

    tl = compute_trendlines(opens_1h, highs_1h, lows_1h, closes_1h, SWING_LOOKBACK, SLOPE_MULT)
    upper_lvl     = tl["upper_lvl"]
    last_pl       = tl["last_pl"]
    last_ph       = tl["last_ph"]
    touches_above = tl["touches_above"]

    if upper_lvl is None or last_pl is None or last_ph is None:
        print(f"[SKIP] {symbol} — trendline / pivots not ready yet")
        return

    precision     = get_precision(candles_1h[-1]["close"])
    last_close_1h = closes_1h[-1]

    # Per-symbol state
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # Backfill any missing fields for older state files
    for k, v in init_symbol_state().items():
        if k not in st:
            st[k] = v

    # =========================================================================
    # TP COMPLETED MONITORING  (runs every 15-min scan for fast wick detection)
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
    # RECONCILE WITH EXCHANGE  (every scan)
    # =========================================================================
    position = get_position_by_pair(symbol)

    if position is not None:
        if not st.get("in_position"):
            entry_px = float(position.get("avg_price") or position.get("entry_price") or last_close_1h)
            st["in_position"] = True
            st["entry_path"]  = st.get("entry_path") or "tl_break"
            st["entry_price"] = entry_px
            print(f"[RECONCILE] {symbol} — reconstructed state from exchange position")

        tp_pos, sl_pos = extract_tp_sl(position)
        if st.get("tp_level") is None and tp_pos is not None:
            st["tp_level"] = round(tp_pos, precision)
        if st.get("sl_price") is None and sl_pos is not None:
            st["sl_price"] = round(sl_pos, precision)

        b_val = str(df.iloc[row, 1]).strip() if df.shape[1] > 1 else ""
        c_val = str(df.iloc[row, 2]).strip() if df.shape[1] > 2 else ""
        if st.get("tp_level") is not None and b_val == "":
            update_sheet_tp(row, st["tp_level"])
        if st.get("sl_price") is not None and c_val == "":
            update_sheet_sl(row, st["sl_price"])

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
        return

    # =========================================================================
    # 1h BAR — extract latest closed-bar values for signal evaluation
    # =========================================================================
    last1h = candles_1h[-1]
    prev1h = candles_1h[-2]

    o1h = float(last1h["open"])
    h1h = float(last1h["high"])
    l1h = float(last1h["low"])
    c1h = float(last1h["close"])
    v1h = float(last1h.get("volume", 0))
    ts1h = int(last1h["time"])
    prev_c1h = float(prev1h["close"])

    # =========================================================================
    # DEDUP — strategy logic runs at most once per closed 1h bar.
    # Subsequent 15-min scans on the same 1h bar will TP-monitor / reconcile
    # but skip strategy evaluation (no duplicate ARM telegrams etc.)
    # =========================================================================
    if st.get("last_processed_1h_ts", 0) == ts1h:
        save_state(all_state)
        return

    # =========================================================================
    # 1h FILTER CALCS (tl_break path)
    # =========================================================================
    bar_range = h1h - l1h
    bar_body  = abs(c1h - o1h)
    body_pct  = (bar_body / bar_range * 100) if bar_range > 0 else 0

    vols_1h = [float(c.get("volume", 0)) for c in candles_1h[-VOL_SMA_PERIOD:]]
    vol_sma = sum(vols_1h) / len(vols_1h) if vols_1h else 0

    atr_arr_1h = compute_atr(highs_1h, lows_1h, closes_1h, ATR_PERIOD_TL)
    atr_1h = atr_arr_1h[-1] if atr_arr_1h[-1] is not None else 0

    fresh_cross = (c1h > upper_lvl) and (prev_c1h <= upper_lvl)
    body_break  = (not USE_BODY_BREAK) or (min(o1h, c1h) > upper_lvl)
    strong_bar  = (not USE_STRONG_BAR) or (body_pct >= MIN_BODY_PCT)
    vol_ok      = (not USE_VOLUME)     or (v1h > vol_sma * VOL_MULT)
    atr_ok      = (not USE_ATR_DIST)   or (c1h > upper_lvl + atr_1h * ATR_MULT)

    # Cooldown — in 1h bars
    last_entry_ts = st.get("last_entry_ts", 0) or 0
    if USE_COOLDOWN and last_entry_ts > 0:
        bars_since  = (ts1h - last_entry_ts) // (CANDLE_SECONDS * 1000)
        cooldown_ok = bars_since >= COOLDOWN_BARS
    else:
        cooldown_ok = True

    long_sig = (
        fresh_cross and body_break and strong_bar and vol_ok and atr_ok
        and cooldown_ok and (touches_above >= MIN_TL_TOUCHES)
    )

    print(
        f"[SCAN] {symbol} | c1h={c1h} prev_c1h={prev_c1h} | "
        f"upperLvl={round(upper_lvl, precision)} lastPL={round(last_pl, precision)} "
        f"lastPH={round(last_ph, precision)} touches={touches_above}/{MIN_TL_TOUCHES} | "
        f"fresh={fresh_cross} body={body_break} "
        f"strong={strong_bar}({round(body_pct, 1)}%) "
        f"vol={vol_ok}({round(v1h, 2)} vs {round(vol_sma * VOL_MULT, 2)}) "
        f"atr={atr_ok} cd={cooldown_ok}"
    )

    # =========================================================================
    # RETEST PATH — arm / evaluate / cancel  (all on 1h bars)
    # =========================================================================
    retest_sig         = False
    retest_signal_info = None

    if USE_RETEST_PATH:
        # ARM: a fresh cross fired but main long_sig didn't go through
        if fresh_cross and not long_sig and touches_above >= MIN_TL_TOUCHES:
            st["retest_armed"]      = True
            st["retest_armed_ts"]   = ts1h
            st["retest_upper_lvl"]  = upper_lvl
            st["retest_last_ph"]    = last_ph
            print(f"[RETEST-ARM] {symbol} — armed at upperLvl={round(upper_lvl, precision)} "
                  f"touches={touches_above} "
                  f"(filters that blocked main: body={body_break} strong={strong_bar} "
                  f"vol={vol_ok} atr={atr_ok} cd={cooldown_ok})")
            send_telegram(
                f"🟠 <b>RETEST ARMED — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📍 c1h        : <code>{round(c1h, precision)}</code>\n"
                f"📈 upperLvl   : <code>{round(upper_lvl, precision)}</code>\n"
                f"🟢 lastPL     : <code>{round(last_pl, precision)}</code>\n"
                f"🔴 lastPH     : <code>{round(last_ph, precision)}</code>\n"
                f"🪢 Touches    : <code>{touches_above}</code> (≥ {MIN_TL_TOUCHES})\n"
                f"⏳ Waiting up to {RETEST_MAX_BARS} × 1h bars for pullback-bounce"
            )
        elif fresh_cross and not long_sig and touches_above < MIN_TL_TOUCHES:
            print(f"[RETEST-NO-ARM] {symbol} — fresh cross but only {touches_above}/{MIN_TL_TOUCHES} touches, "
                  f"trendline not tested enough")

        # EVALUATE
        if st.get("retest_armed"):
            armed_ts  = st.get("retest_armed_ts", 0)
            armed_lvl = st.get("retest_upper_lvl")

            if armed_ts and armed_lvl:
                bars_armed = max(0, (ts1h - armed_ts) // (CANDLE_SECONDS * 1000))

                # Cancel: timed out
                if bars_armed > RETEST_MAX_BARS:
                    print(f"[RETEST-CANCEL] {symbol} — timed out after {bars_armed} 1h bars")
                    send_telegram(
                        f"⌛ <b>RETEST EXPIRED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⏳ {bars_armed} &gt; {RETEST_MAX_BARS} × 1h bars, no bounce occurred"
                    )
                    st["retest_armed"]     = False
                    st["retest_armed_ts"]  = 0
                    st["retest_upper_lvl"] = None
                    st["retest_last_ph"]   = None

                elif c1h < upper_lvl * (1 - RETEST_CANCEL_PCT / 100):
                    print(f"[RETEST-CANCEL] {symbol} — close {c1h} fell &gt;{RETEST_CANCEL_PCT}% "
                          f"below upperLvl {round(upper_lvl, precision)} → trendline broken")
                    send_telegram(
                        f"❌ <b>RETEST CANCELLED — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 c1h      : <code>{round(c1h, precision)}</code>\n"
                        f"📈 upperLvl : <code>{round(upper_lvl, precision)}</code>\n"
                        f"⚠️ Reason   : trendline broken (close fell &gt;{RETEST_CANCEL_PCT}% below)"
                    )
                    st["retest_armed"]     = False
                    st["retest_armed_ts"]  = 0
                    st["retest_upper_lvl"] = None
                    st["retest_last_ph"]   = None

                elif bars_armed < 1:
                    # Just armed on THIS 1h bar — wait for the next one before
                    # checking bounce conditions.
                    print(f"[RETEST-WAIT] {symbol} — just armed this 1h bar, "
                          f"waiting for next 1h bar before checking bounce")

                else:
                    # TRIGGER (1h candle that bounces off the line):
                    #   1. wick within ±RETEST_TOUCH_PCT of upper_lvl
                    #   2. close back above upper_lvl
                    #   3. bullish candle (c1h > o1h)
                    #   4. body ≥ RETEST_MIN_BODY_PCT of range
                    #   5. cooldown clear
                    #   6. touches still ≥ MIN_TL_TOUCHES
                    touch_floor = upper_lvl * (1 - RETEST_TOUCH_PCT / 100)
                    touch_ceil  = upper_lvl * (1 + RETEST_TOUCH_PCT / 100)

                    cond_touch     = touch_floor <= l1h <= touch_ceil
                    cond_close_up  = c1h > upper_lvl
                    cond_bullish   = c1h > o1h
                    cond_body_ok   = body_pct >= RETEST_MIN_BODY_PCT
                    cond_cd_ok     = cooldown_ok
                    cond_touches   = touches_above >= MIN_TL_TOUCHES

                    retest_sig = (cond_touch and cond_close_up and cond_bullish
                                  and cond_body_ok and cond_cd_ok and cond_touches
                                  and not long_sig)

                    print(
                        f"[RETEST-EVAL] {symbol} | armed_lvl={round(armed_lvl, precision)} "
                        f"upperLvl={round(upper_lvl, precision)} bars={bars_armed}/{RETEST_MAX_BARS} "
                        f"touches={touches_above}/{MIN_TL_TOUCHES} | "
                        f"touch={cond_touch}(l1h={l1h}) closeUp={cond_close_up} "
                        f"bull={cond_bullish} body={cond_body_ok}({round(body_pct, 1)}%) "
                        f"cd={cond_cd_ok} → retest={retest_sig}"
                    )

                    if retest_sig:
                        retest_signal_info = {
                            "c1h":             round(c1h, precision),
                            "l1h":             round(l1h, precision),
                            "upper_lvl":       round(upper_lvl, precision),
                            "armed_upper_lvl": round(armed_lvl, precision),
                            "last_pl":         round(last_pl, precision),
                            "last_ph":         round(last_ph, precision),
                            "body_pct":        round(body_pct, 1),
                            "bars_armed":      int(bars_armed),
                            "touches_above":   touches_above,
                            "bars_since_last": (
                                (ts1h - last_entry_ts) // (CANDLE_SECONDS * 1000)
                                if last_entry_ts > 0 else "n/a"
                            ),
                        }

    # =========================================================================
    # ENTRY DISPATCH — main path takes priority
    # =========================================================================
    entry_path = None
    sig_info   = None

    if long_sig:
        entry_path = "tl_break"
        bars_since_last = (
            (ts1h - last_entry_ts) // (CANDLE_SECONDS * 1000)
            if last_entry_ts > 0 else "n/a"
        )
        sig_info = {
            "c1h":             round(c1h, precision),
            "prev_c1h":        round(prev_c1h, precision),
            "upper_lvl":       round(upper_lvl, precision),
            "last_pl":         round(last_pl, precision),
            "last_ph":         round(last_ph, precision),
            "min_oc":          round(min(o1h, c1h), precision),
            "body_pct":        round(body_pct, 1),
            "min_body_pct":    MIN_BODY_PCT,
            "vol":             round(v1h, 2),
            "vol_mult":        VOL_MULT,
            "vol_threshold":   round(vol_sma * VOL_MULT, 2),
            "atr_mult":        ATR_MULT,
            "atr_threshold":   round(upper_lvl + atr_1h * ATR_MULT, precision),
            "bars_since_last": bars_since_last,
            "cooldown_bars":   COOLDOWN_BARS,
            "touches_above":   touches_above,
        }
    elif retest_sig:
        entry_path = "tl_retest"
        sig_info   = retest_signal_info

    # Mark this 1h bar as processed BEFORE returning, so subsequent 15-min
    # scans on the same 1h bar skip strategy eval (only TP/reconcile run).
    st["last_processed_1h_ts"] = ts1h

    if entry_path is None:
        save_state(all_state)
        return

    # ─── Compute SL/TP (same geometry for both paths) ───────
    entry_price = c1h
    sl_price    = upper_lvl * (1 - SL_BELOW_TL_PCT / 100)
    tp_price    = entry_price * (1 + TP_PCT / 100)
    risk        = entry_price - sl_price

    if risk <= 0 or sl_price >= entry_price:
        print(f"[SKIP] {symbol} — invalid risk (entry {entry_price} ≤ SL {sl_price})")
        save_state(all_state)
        return

    # Last-second guards
    if get_position_by_pair(symbol) is not None:
        print(f"[ABORT] {symbol} — position appeared just before placement")
        return
    if has_open_order(symbol):
        print(f"[ABORT] {symbol} — order appeared just before placement")
        return

    placed = place_long_order(symbol, entry_price, tp_price, sl_price, precision, entry_path, sig_info)
    if placed:
        st["in_position"]   = True
        st["entry_path"]    = entry_path
        st["entry_price"]   = round(entry_price, precision)
        st["tp_level"]      = round(tp_price,    precision)
        st["sl_price"]      = round(sl_price,    precision)
        st["last_entry_ts"] = ts1h
        # Clear retest arming on any successful entry
        st["retest_armed"]     = False
        st["retest_armed_ts"]  = 0
        st["retest_upper_lvl"] = None
        st["retest_last_ph"]   = None
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
    f"⚡ Entry      : <code>1h close confirmation (breakout & retest)</code>\n"
    f"🔁 Scan       : <code>Every 15 minutes (strategy runs once per new 1h bar)</code>\n"
    f"🧪 Filters    : <code>body-break, body≥{MIN_BODY_PCT:.0f}%, vol&gt;SMA{VOL_SMA_PERIOD}×{VOL_MULT}, "
    f"break≥{ATR_MULT}×ATR14, cooldown={COOLDOWN_BARS}×1h</code>\n"
    f"🛤 Paths      : <code>tl_break + tl_retest "
    f"(retest: {'ON' if USE_RETEST_PATH else 'OFF'}, "
    f"max {RETEST_MAX_BARS}×1h, ±{RETEST_TOUCH_PCT}% touch, body≥{RETEST_MIN_BODY_PCT}%)</code>\n"
    f"🪢 Touches    : <code>require ≥ {MIN_TL_TOUCHES} prior failed-break attempts on the trendline</code>\n"
    f"🎯 TP         : <code>entry × (1 + {TP_PCT}%)</code>\n"
    f"🛑 SL         : <code>upperLvl × (1 - {SL_BELOW_TL_PCT}%)</code>\n"
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