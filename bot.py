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
EMA_FAST_PERIOD       = 50
EMA_SLOW_PERIOD       = 100
TP_PCT                = 0.02
SL_PCT                = 0.075
MIN_RR                = 0.05
EMA50_SLOPE_BARS      = 5
EMA100_SLOPE_BARS     = 5
EMA100_FLAT_THRESHOLD = 0.001

# ─── REQUEST TIMEOUTS (seconds) ───────────────────────────────────────────────
REQUEST_TIMEOUT       = 15
TELEGRAM_TIMEOUT      = 10

# ─── GSPREAD RE-AUTH INTERVAL ─────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60   # 45 minutes in seconds
# ──────────────────────────────────────────────────────────────────────────────


# =====================================================
# GOOGLE SHEETS — with periodic re-auth
# =====================================================

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_sheet            = None
_last_auth_time   = 0


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
    """Column C stores live SL for trailing stop tracking."""
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
    """
    Returns True if there is any unfilled order for this pair still on the book.
    """
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
# WICK TP DETECTION  (for longs — recent HIGH vs TP)
# =====================================================

def get_recent_high(symbol):
    """
    Returns the highest high across the last 3 minutes of 1-min candles.
    Used to detect if a wick already tagged the TP level.
    """
    try:
        pair_api = fut_pair(symbol)
        url  = "https://public.coindcx.com/market_data/candlesticks"
        now  = int(time.time())
        params = {
            "pair":       pair_api,
            "from":       now - 180,
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
# PLACE LONG ORDER  (BUY side only — this bot is long-only)
# =====================================================

def place_long_order(symbol, entry_price, precision):
    entry   = round(entry_price, precision)

    # LONG: TP is ABOVE entry (price needs to rise to hit profit)
    #       SL is BELOW entry (price falling beyond SL closes trade)
    tp      = round(entry * (1 + TP_PCT), precision)
    sl_base = round(entry * (1 - SL_PCT), precision)

    reward = tp - entry       # how much price needs to rise to hit TP
    risk   = entry - sl_base  # how much price can fall before SL triggers

    if risk <= 0 or (reward / risk) < MIN_RR:
        rr = round(reward / risk, 2) if risk > 0 else "inf"
        print(f"[SKIP] {symbol} RR {rr} < {MIN_RR}")
        send_telegram(
            f"⚠️ <b>LONG SIGNAL SKIPPED — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❌ Reason : <code>RR {rr} below minimum {MIN_RR}</code>\n"
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>  (above entry)\n"
            f"🛑 SL     : <code>{sl_base}</code>  (below entry)"
        )
        return None, None

    qty = compute_qty(entry_price, symbol)

    print(
        f"[LONG TRADE] {symbol} BUY | Entry {entry} | TP {tp} (+{TP_PCT*100:.1f}%) "
        f"| SL {sl_base} (-{SL_PCT*100:.0f}%) | RR {round(reward / risk, 2)} | Qty {qty}"
    )

    body = {
        "timestamp": int(time.time() * 1000),
        "order": {
            "side":              "buy",          # ← always BUY — this is a long-only bot
            "pair":              fut_pair(symbol),
            "order_type":        "limit_order",
            "price":             entry,
            "total_quantity":    qty,
            "leverage":          LEVERAGE,
            "take_profit_price": tp,             # TP above entry
            "stop_loss_price":   sl_base,        # SL below entry
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
            f"📍 Entry  : <code>{entry}</code>\n"
            f"🎯 TP     : <code>{tp}</code>  (above entry)\n"
            f"🛑 SL     : <code>{sl_base}</code>  (below entry)\n"
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
        f"🎯 TP      : <code>{tp}</code>  (+{TP_PCT * 100:.1f}% above entry)\n"
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

    if len(candles) < EMA_SLOW_PERIOD + 5:
        return

    precision  = get_precision(candles[-1]["close"])
    closes     = [float(c["close"]) for c in candles]
    last_close = float(candles[-1]["close"])

    del candles

    ema50_values  = compute_ema(closes, EMA_FAST_PERIOD)
    ema100_values = compute_ema(closes, EMA_SLOW_PERIOD)
    del closes

    ema50  = round(ema50_values[-1],  precision)
    ema100 = round(ema100_values[-1], precision)

    # =========================================================================
    # GATE 1 — Open position check (order was filled → live position exists)
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
    # GATE 2 — Open order check (order NOT yet filled, status="open")
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

        # For a LONG: TP is hit when price rises TO or ABOVE the TP level
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

    # ── SLOPE FILTERS ─────────────────────────────────────────────────────────

    # 50 EMA must have a POSITIVE slope (turning up = bullish momentum building)
    ema50_slope = ema50_values[-1] - ema50_values[-EMA50_SLOPE_BARS]
    if ema50_slope <= 0:
        print(f"[SKIP] {symbol} 50 EMA slope not up ({round(ema50_slope, precision)}) | 50 EMA {ema50} | 100 EMA {ema100} | Price {last_close}")
        return

    # 100 EMA must be FLAT or POSITIVE (not still strongly falling)
    ema100_slope     = ema100_values[-1] - ema100_values[-EMA100_SLOPE_BARS]
    ema100_slope_pct = ema100_slope / last_close
    if ema100_slope_pct < -EMA100_FLAT_THRESHOLD:
        print(f"[SKIP] {symbol} 100 EMA still falling (slope {round(ema100_slope_pct * 100, 4)}%) | 50 EMA {ema50} | 100 EMA {ema100} | Price {last_close}")
        return

    # ── STRATEGY CONDITIONS ───────────────────────────────────────────────────

    # Macro context: 50 EMA was below 100 EMA (bearish-to-bullish flip in play)
    macro_bearish_context = ema50 < ema100

    # Price has crossed ABOVE the 50 EMA from below (bullish EMA crossover signal)
    above_ema50 = last_close > ema50

    # Price has also pushed ABOVE the 100 EMA (full breakout confirmed)
    above_ema100 = last_close > ema100

    if not macro_bearish_context:
        print(f"[SKIP] {symbol} 50 EMA {ema50} is already above 100 EMA {ema100} — macro context not bearish-to-bullish | Price {last_close}")
        return

    if not above_ema50:
        print(f"[SKIP] {symbol} price {last_close} has not crossed above 50 EMA {ema50} | 100 EMA {ema100}")
        return

    if not above_ema100:
        print(f"[SKIP] {symbol} price {last_close} has not cleared above 100 EMA {ema100} | 50 EMA {ema50}")
        return

    print(
        f"[SIGNAL] {symbol} | Price {last_close} | 50 EMA {ema50} | 100 EMA {ema100} "
        f"| 50 slope +{round(ema50_slope, precision)} | 100 slope {round(ema100_slope_pct * 100, 4)}% "
        f"| TP {round(last_close * (1 + TP_PCT), precision)} "
        f"| SL {round(last_close * (1 - SL_PCT), precision)}"
    )

    # =========================================================================
    # FINAL GUARD — re-check both states right before placing
    # Protects against race conditions where state changed mid-scan
    # =========================================================================

    # Guard 1: active position already open
    live_positions = get_open_positions()
    for pos in live_positions:
        if pos.get("pair") == pair:
            print(f"[SKIP] {symbol} — open position detected just before placement, aborting")
            return

    # Guard 2: unfilled order still on book
    if has_open_order(symbol):
        print(f"[SKIP] {symbol} — unfilled open order detected just before placement, aborting")
        return

    # ── Place LONG (BUY) trade ────────────────────────────────────────────────
    tp_confirmed, sl_placed = place_long_order(
        symbol, last_close, precision
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
    f"📐 Strategy : <code>50/100 Breakout Long</code>\n"
    f"⏱ Timeframe : <code>30 Min</code>\n"
    f"📈 Entry    : <code>Price above 50 &amp; 100 EMA (50 EMA still below 100)</code>\n"
    f"✅ Context  : <code>50 EMA below 100 EMA (bearish-to-bullish flip)</code>\n"
    f"📉 50 EMA   : <code>Slope must be positive</code>\n"
    f"📊 100 EMA  : <code>Slope must be flat or positive</code>\n"
    f"🎯 TP       : <code>{TP_PCT * 100:.1f}% fixed above entry</code>\n"
    f"🛑 SL       : <code>{int(SL_PCT * 100)}% fixed below entry</code>\n"
    f"💰 Capital  : <code>{CAPITAL_USDT} USDT × {LEVERAGE}x</code>\n"
    f"🕐 Scanning every 30 seconds..."
)

while True:
    try:
        df = get_sheet_data()

        if df.empty:
            print("[WARN] Sheet returned empty — possible auth issue, retrying in 30s")
            time.sleep(30)
            continue

        cycle += 1
        consecutive_errors = 0

        if cycle % 10 == 0:
            print("----- TRADE SCAN (5 MIN) -----")
        else:
            print("----- TP / SL MONITOR (30s) -----")

        for row in range(len(df)):
            pair = df.iloc[row, 0]
            if not pair:
                continue
            symbol = normalize_symbol(pair)
            check_and_trade(symbol, row, df)

        time.sleep(30)

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