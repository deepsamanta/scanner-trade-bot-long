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
# Source: Long_Stratergy  (no trailing SL — fixed TP + fixed SL)
# =============================================================================

# ─── CORE ─────────────────────────────────────────────────────────────────────
EMA_PERIOD           = 200
LOOKBACK             = 250      # candles to count below EMA
BELOW_PCT_MIN        = 70.0     # min % of last LOOKBACK candles below EMA
TP_PCT               = 2.5      # Take Profit % (fixed above entry)
SL_BELOW_EMA_PCT     = 0.8      # SL = EMA × (1 - this/100)

# ─── PATH A: REVERSAL RETEST ─────────────────────────────────────────────────
MAX_RETEST_BARS      = 20       # max 15m bars to wait for retest after arming
PROXIMITY_PCT        = 0.3      # retest zone = EMA × (1 + this/100)

# ─── SLOPE FILTER ────────────────────────────────────────────────────────────
# Pine: "Require EMA Flattening (Reversal)"  →  minEmaSlope = -0.2
# Allows slight negative slope (flat/flattening EMA) — bullish early-reversal
USE_SLOPE_FILTER     = True
SLOPE_BARS           = 10       # % change of EMA over this many bars
MIN_EMA_SLOPE_PCT    = 0.02     # min EMA slope % to qualify  (Pine default: -0.2)

# ─── VOLUME FILTER ───────────────────────────────────────────────────────────
USE_VOLUME_FILTER    = True
VOL_LOOKBACK         = 20
VOL_MULTIPLIER       = 1.0      # reversal path (Path A)
BREAKOUT_VOL_MULT    = 1.3      # breakout path (Path B)

# ─── PATH B: MOMENTUM BREAKOUT ───────────────────────────────────────────────
USE_BREAKOUT_PATH    = True
MOMENTUM_LOOKBACK    = 5        # close > close[N] bars ago

# ─── CROSSOVER LOOKBACK (rescue filter) ──────────────────────────────────────
# Pine strict: crossover must fire on the CURRENT bar.
# Problem: if any other filter (slope/volume) happens to fail on the exact bar
# the cross fires, the whole setup is discarded — even if it would have passed
# on the very next bar. This causes many viable signals to be missed.
# Fix: accept a cross that happened within the last N bars, PROVIDED price is
# still above EMA and not overextended. All other filters (trend, slope, vol)
# must still pass on the CURRENT bar, exactly as before.
CROSS_LOOKBACK        = 5        # accept crossover from last N bars (incl. current)
MAX_EMA_DISTANCE_PCT  = 1.5      # don't arm if price is already >X% above EMA (anti-chase)

# ─── CROSSDOWN DROP FILTER ───────────────────────────────────────────────────
# Rejects coins that drifted below EMA without a real dump. The intent: only
# trade reversals where the coin genuinely dropped, because those have room to
# bounce. A coin that crossed down and sat there 2% below EMA is probably
# ranging, not reversing.
#
# Algorithm (measures peak-to-trough dump magnitude):
#   1. Scan the last DROP_LOOKBACK candles for ALL crossdowns
#      (close went from ≥EMA → <EMA). Multiple are common in choppy periods.
#   2a. One or more crossdowns FOUND:
#         upper_hinge = MAX(EMA value at each crossdown bar)
#                       (the "top of the damage" reference)
#         lower       = LOWEST LOW across the entire DROP_LOOKBACK window
#   2b. No crossdown (price was already below EMA throughout the window):
#         upper_hinge = HIGHEST HIGH across the window
#         lower       = LOWEST LOW across the window
#   3. drop% = (upper_hinge - lower) / upper_hinge × 100
#   4. Require drop% ≥ MIN_DROP_PCT
USE_DROP_FILTER      = True
DROP_LOOKBACK        = 250       # candles to scan
MIN_DROP_PCT         = 10.0      # min drop % from upper hinge to lowest low

# ─── PATH C: SUPPORT BOUNCE (multi-timeframe pivot confluence) ───────────────
# Alternative long-entry path for coins that have dumped ≥5% from the top of
# the EMA damage and are now heading toward a historically defended zone.
#
# "Support zone" = a horizontal price band where price previously reversed
# direction — this includes both pivot HIGHS (prior resistance that may flip
# to support) and pivot LOWS (prior bounce levels). Any bar that was a local
# extreme counts as a reactive level.
#
# Confluence across 4 timeframes (15m / 30m / 1h / 4h) identifies zones that
# matter on multiple scales — a single 15m pivot is noise, but a zone where
# 3 of 4 timeframes have pivots is a meaningful structural level.
#
# Flow:
#   1. Coin dumped ≥PATH_C_MIN_DROP_PCT from the MOST RECENT crossdown
#      (uses a different drop measurement than Paths A/B, which use max-EMA).
#      Fallback: if no crossdown in window, reuse the general drop_pct
#      (max-EMA / highest-high based).
#   2. Find all pivots (highs + lows) in 600-bar windows on each of 4 TFs
#   3. Cluster pivots that fall within PIVOT_ZONE_PCT of each other into zones
#   4. Keep only zones defended by ≥MIN_TF_CONFLUENCE timeframes
#   5. Filter to zones BELOW current price → price is expected to find support here
#   6. Pick the NEAREST zone below current price (the first floor the coin will test)
#   7. Arm the setup and wait for a bounce: a 15m bar closes above the zone
#      AFTER price has traded at or below the zone center
#   8. Enter long on the reclaim bar
USE_PATH_C                = True
PATH_C_ENABLED_TIMEFRAMES = ["15", "30", "60", "240"]  # CoinDCX resolution strings
PATH_C_CANDLES            = 600           # bars per TF
PATH_C_MIN_DROP_PCT       = 5.0           # min drop from MOST RECENT crossdown
PIVOT_STRENGTH            = 3             # N bars on each side for pivot detection
PIVOT_ZONE_PCT            = 1.0           # ±% band for clustering pivots
MIN_TF_CONFLUENCE         = 3             # minimum TFs defending a zone (3 of 4)
PATH_C_MAX_WAIT_BARS      = 30            # max 15m bars to wait for bounce after arming
PATH_C_TOUCH_TOLERANCE_PCT = 0.5          # price must come within this % of zone to count as "tested"
PATH_C_SL_BELOW_ZONE_PCT  = 0.8           # SL placed this % below zone_low (Path C only, decoupled from Paths A/B)

# ─── TIMEFRAME / SCAN ────────────────────────────────────────────────────────
RESOLUTION           = "15"     # CoinDCX 15-minute candles
CANDLE_SECONDS       = 15 * 60
SCAN_INTERVAL        = 900      # 15 minutes in seconds

# ─── REQUEST TIMEOUTS (seconds) ──────────────────────────────────────────────
REQUEST_TIMEOUT      = 15
TELEGRAM_TIMEOUT     = 10

# ─── GSPREAD RE-AUTH INTERVAL ────────────────────────────────────────────────
GSHEET_REAUTH_INTERVAL = 45 * 60

# ─── LOCAL STATE FILE (wait-for-retest persistence) ──────────────────────────
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
# LOCAL STATE PERSISTENCE (waiting_retest across scans)
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
        # ── Path A state ─────────────────────────────────────────────────
        "waiting_retest":        False,
        "wait_start_candle_ts":  None,    # ms timestamp of the candle when armed
        # ── Path C state (support bounce) ────────────────────────────────
        "path_c_armed":          False,
        "path_c_start_ts":       None,    # ms timestamp when zone was armed
        "path_c_zone_low":       None,    # lower edge of support zone
        "path_c_zone_high":      None,    # upper edge of support zone
        "path_c_zone_center":    None,    # midpoint (for entry comparisons)
        "path_c_zone_touched":   False,   # has price dipped into the zone yet?
        "path_c_tf_count":       None,    # how many TFs defended this zone (log/telegram only)
        # ── Common position state ────────────────────────────────────────
        "in_position":           False,
        "entry_path":            None,    # "retest", "breakout", or "support_bounce"
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
    """
    Returns EMA series aligned so ema_values[-1] pairs with closes[-1].
    Left-padded with None for indices where EMA is not yet defined.
    """
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
    """
    Returns (found, bars_ago) where found=True if a close crossed above EMA
    at any point within the last `lookback` bars (inclusive of current bar).
    bars_ago = 0 means cross on current bar, 1 means previous bar, etc.

    A cross is defined as: closes[i-1] <= emas[i-1] AND closes[i] > emas[i]

    Used by the rescue filter so a crossover that happened a few bars ago
    isn't forgotten just because slope/volume failed on that exact bar.
    """
    # Need at least one prior bar to compare against
    n = len(closes)
    if n < 2 or lookback < 1:
        return False, None

    # Walk backwards from the current bar (i=-1) up to lookback bars back
    for k in range(lookback):
        i_now  = n - 1 - k        # current index within lookback
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


def fetch_candles_tf(symbol, resolution_str, num_candles_needed):
    """
    Generic multi-timeframe candle fetch. `resolution_str` must be a CoinDCX
    resolution: "15", "30", "60", "240", "1D". Returns a list of dicts with
    keys: time, open, high, low, close, volume.
    """
    # Map resolution string to seconds per candle
    res_to_seconds = {
        "1":    60,
        "5":    5 * 60,
        "15":   15 * 60,
        "30":   30 * 60,
        "60":   60 * 60,
        "240":  4  * 60 * 60,
        "1D":   24 * 60 * 60,
    }
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
# PIVOT DETECTION (Path C)
# =====================================================

def find_pivots(highs, lows, strength):
    """
    Returns a list of pivot prices found in the given candle series.
    A bar is a PIVOT HIGH if its high is strictly greater than the highs of
    the `strength` bars before AND the `strength` bars after.
    A bar is a PIVOT LOW if its low is strictly less than the lows of the
    `strength` bars before AND the `strength` bars after.

    Both pivot types are treated the same way — they're "reactive levels"
    where price changed direction. Returns a flat list of price values.

    The most recent `strength` bars are excluded (we can't know yet if they
    will become pivots until `strength` more bars form on the right side).
    """
    pivot_prices = []
    n = len(highs)
    if n < 2 * strength + 1:
        return pivot_prices

    for i in range(strength, n - strength):
        h_center = highs[i]
        l_center = lows[i]

        # Pivot high check
        is_pivot_high = True
        for k in range(1, strength + 1):
            if highs[i - k] >= h_center or highs[i + k] >= h_center:
                is_pivot_high = False
                break
        if is_pivot_high:
            pivot_prices.append(h_center)
            continue  # same bar can't be both high and low pivot

        # Pivot low check
        is_pivot_low = True
        for k in range(1, strength + 1):
            if lows[i - k] <= l_center or lows[i + k] <= l_center:
                is_pivot_low = False
                break
        if is_pivot_low:
            pivot_prices.append(l_center)

    return pivot_prices


def cluster_pivots_to_zones(pivots_by_tf, proximity_pct):
    """
    Takes a dict { tf_label: [pivot_prices...] } and clusters pivots across
    all timeframes into zones. Two pivots belong to the same zone if they are
    within proximity_pct of each other (measured from the lower pivot).

    Returns a list of zone dicts:
      { "center": float,
        "low":    float,
        "high":   float,
        "tfs":    set(tf_labels),  # which timeframes defend this zone
        "pivots": [(price, tf), ...] }

    Algorithm:
      1. Flatten all pivots with their TF label
      2. Sort ascending by price
      3. Walk through in order, starting a new zone whenever the gap between
         a pivot and the running zone center exceeds proximity_pct
      4. Compute final zone bounds and TF membership
    """
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
        # Distance from current zone center as %
        gap_pct = abs(price - current_center) / current_center * 100.0
        if gap_pct <= proximity_pct:
            # Same zone
            current_pivots.append((price, tf))
            # Recenter as the running mean
            current_center = sum(p for p, _ in current_pivots) / len(current_pivots)
        else:
            # Flush the current zone
            zones.append(_finalize_zone(current_pivots))
            current_pivots = [(price, tf)]
            current_center = price

    # Don't forget the last one
    zones.append(_finalize_zone(current_pivots))
    return zones


def _finalize_zone(pivots):
    """Helper: build a zone dict from a list of (price, tf) tuples."""
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
    """
    Full Path C support-zone detection pipeline.

    1. Fetch PATH_C_CANDLES bars on each timeframe in PATH_C_ENABLED_TIMEFRAMES
    2. Compute pivots on each TF (both pivot highs and pivot lows)
    3. Cluster all pivots across TFs into confluence zones (±PIVOT_ZONE_PCT)
    4. Keep only zones defended by ≥MIN_TF_CONFLUENCE timeframes
    5. Filter to zones strictly BELOW current_price
    6. Return the NEAREST one (highest zone among those below current price)

    Returns the zone dict, or None if no qualifying zone found.
    """
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

    # Keep only confluence zones with enough TF support
    strong_zones = [z for z in zones if len(z["tfs"]) >= MIN_TF_CONFLUENCE]
    if not strong_zones:
        return None

    # Filter to zones strictly below current price
    # A zone's upper edge must be below current price so price has room to
    # dip down INTO the zone. Otherwise we'd be "reclaiming" something we
    # haven't yet dropped through.
    below_zones = [z for z in strong_zones if z["high"] < current_price]
    if not below_zones:
        return None

    # Pick the one with the highest center (nearest to current price from below)
    nearest = max(below_zones, key=lambda z: z["center"])
    return nearest


# =====================================================
# RECENT HIGH — wick-based TP detection
# =====================================================

def get_recent_high(symbol):
    """
    Fetches 1-min candles over the last SCAN_INTERVAL seconds to check if
    price wicked up to touch a stored TP between scans. Without this, a TP
    that was hit by a wick (but not by a 15m close) would be missed, and the
    bot would keep seeing the numeric TP in col B and treat the coin as still
    in a live trade.
    """
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
    """Entry-type unfilled order still on book."""
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
# PLACE LONG ORDER — fixed TP + fixed SL attached to entry
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

    # ─── Fetch enough 15m candles ────────────────────────────────────────────
    candles_needed = EMA_PERIOD + LOOKBACK + 30
    candles = fetch_candles(symbol, candles_needed)

    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient candles ({len(candles)})")
        return

    # ─── Use CLOSED candles only ─────────────────────────────────────────────
    # CoinDCX returns candles including the currently-forming bar at the end.
    # Acting on the in-progress bar causes intrabar false signals: e.g. price
    # briefly spikes above EMA mid-bar, bot enters, bar closes back below.
    # Pine Script's default behavior is to run strategies on bar close only,
    # so we drop the last (live) candle and treat candles[-1] (now the prior
    # bar) as the most recent CONFIRMED close.
    if len(candles) >= 2:
        now_ms           = int(time.time() * 1000)
        last_candle_time = int(candles[-1]["time"])
        bar_elapsed_ms   = now_ms - last_candle_time
        # A bar is "closed" if more than CANDLE_SECONDS has passed since it opened.
        # If the last bar is still forming (elapsed < CANDLE_SECONDS), drop it.
        if bar_elapsed_ms < CANDLE_SECONDS * 1000:
            candles = candles[:-1]
            print(f"[{symbol}] Dropping in-progress bar ({bar_elapsed_ms/1000:.0f}s elapsed of {CANDLE_SECONDS}s)")

    # Revalidate after dropping
    if len(candles) < EMA_PERIOD + LOOKBACK + 5:
        print(f"[SKIP] {symbol} — insufficient closed candles ({len(candles)})")
        return

    precision = get_precision(candles[-1]["close"])
    closes    = [float(c["close"])  for c in candles]
    highs     = [float(c["high"])   for c in candles]
    lows      = [float(c["low"])    for c in candles]
    volumes   = [float(c.get("volume", 0)) for c in candles]

    last_close = closes[-1]   # most recent CONFIRMED close (bar is closed)
    last_low   = lows[-1]
    last_ts    = int(candles[-1]["time"])   # milliseconds

    # ─── Indicators ──────────────────────────────────────────────────────────
    ema_values = compute_ema(closes, EMA_PERIOD)
    if (ema_values[-1] is None
            or ema_values[-1 - SLOPE_BARS] is None
            or ema_values[-1 - LOOKBACK] is None):
        print(f"[SKIP] {symbol} — EMA not ready deep enough")
        return

    ema_now    = ema_values[-1]
    ema_prev   = ema_values[-2]
    close_prev = closes[-2]

    # % below EMA over last LOOKBACK bars (matches Pine: math.sum(below_bar, lookback))
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

    # EMA slope %  (matches Pine: (ema - ema[slopeBars]) / ema[slopeBars] * 100)
    ema_slope_ref = ema_values[-1 - SLOPE_BARS]
    ema_slope_pct = ((ema_now - ema_slope_ref) / ema_slope_ref * 100.0) if ema_slope_ref else 0
    slope_ok      = (not USE_SLOPE_FILTER) or (ema_slope_pct >= MIN_EMA_SLOPE_PCT)

    # Volume SMA + checks
    vol_window = volumes[-VOL_LOOKBACK:]
    vol_avg    = (sum(vol_window) / VOL_LOOKBACK) if len(vol_window) == VOL_LOOKBACK else 0
    last_vol   = volumes[-1]
    vol_ok          = (not USE_VOLUME_FILTER) or (vol_avg > 0 and last_vol > vol_avg * VOL_MULTIPLIER)
    breakout_vol_ok = (vol_avg > 0) and (last_vol > vol_avg * BREAKOUT_VOL_MULT)

    # ─── Crossdown drop filter ──────────────────────────────────────────────
    # Measure how much the coin dumped — only trade reversals with real damage.
    #
    # Step 1: Scan the last DROP_LOOKBACK candles for ALL crossdowns
    #         (close went from ≥EMA → <EMA). There may be multiple.
    #
    # Step 2a — One or more crossdowns FOUND:
    #   Of all those crossdown bars, pick the one where EMA value is HIGHEST.
    #   This picks the "top of the damage" — in a real downtrend, the EMA at
    #   the first crossdown is usually highest, but if EMA is still rising or
    #   flat we want the best peak reference available.
    #       upper_hinge = max(EMA_at_each_crossdown)
    #       lower       = LOWEST LOW across the entire DROP_LOOKBACK window
    #
    # Step 2b — No crossdowns (price was already below EMA throughout):
    #       upper_hinge = HIGHEST HIGH across the window
    #       lower       = LOWEST LOW across the window
    #
    # Step 3: drop_pct = (upper_hinge - lower) / upper_hinge × 100
    #         drop_ok  = drop_pct ≥ MIN_DROP_PCT
    n_candles      = len(closes)
    drop_start_idx = max(0, n_candles - DROP_LOOKBACK)
    window_lows    = lows[drop_start_idx:]
    window_highs   = highs[drop_start_idx:]
    lowest_in_window  = min(window_lows)  if window_lows  else 0
    highest_in_window = max(window_highs) if window_highs else 0

    # Collect ALL crossdown events and the EMA values at each
    crossdown_emas = []   # list of (bar_index, ema_at_that_bar)
    for i in range(drop_start_idx + 1, n_candles):
        e_prev_i = ema_values[i - 1]
        e_now_i  = ema_values[i]
        if e_prev_i is None or e_now_i is None:
            continue
        if closes[i - 1] >= e_prev_i and closes[i] < e_now_i:
            crossdown_emas.append((i, e_now_i))

    if crossdown_emas:
        # Pick the crossdown with the HIGHEST EMA value as upper hinge
        best_xd_idx, upper_hinge = max(crossdown_emas, key=lambda t: t[1])
        drop_anchor_type   = "crossdown"
        crossdown_count    = len(crossdown_emas)
        chosen_crossdown_i = best_xd_idx
    else:
        # No crossdown → price was already below EMA throughout the window
        upper_hinge        = highest_in_window
        drop_anchor_type   = "highest_high"
        crossdown_count    = 0
        chosen_crossdown_i = None

    if upper_hinge is None or upper_hinge <= 0 or lowest_in_window <= 0:
        # Data not usable — fail-safe to NOT pass filter (unless filter disabled)
        drop_pct = 0.0
        drop_ok  = (not USE_DROP_FILTER)
    else:
        drop_pct = (upper_hinge - lowest_in_window) / upper_hinge * 100.0
        drop_ok  = (not USE_DROP_FILTER) or (drop_pct >= MIN_DROP_PCT)

    # ─── Path C drop variant ────────────────────────────────────────────────
    # Paths A/B use `drop_pct` above (max-EMA across all crossdowns) because
    # they want to measure the total magnitude of damage.
    #
    # Path C uses a DIFFERENT measurement: only the CURRENT leg down, from
    # the most recent crossdown to now.
    #
    # Rationale: Path C is a support-bounce play looking for a recent dump
    # that's now about to test a support zone. If the lowest low in the 150-
    # bar window happened during an unrelated earlier dump (say 80 bars ago)
    # and the recent crossdown is 20 bars ago, mixing those inflates the drop
    # artificially. We only care about how much the coin has fallen in this
    # current leg.
    #
    # Formula:
    #   - recent_xd_idx    = LAST entry in crossdown_emas (most recent cut)
    #   - upper            = EMA at recent_xd_idx
    #   - lower_since_xd   = min(lows[recent_xd_idx:])
    #                        ← only bars from the recent crossdown onwards
    #   - drop_pct_path_c  = (upper - lower_since_xd) / upper × 100
    #
    # Fallback: if no crossdown exists in the window, reuse the general
    # drop_pct (which already handles the "already below EMA" case via
    # the highest_high anchor).
    if crossdown_emas:
        recent_xd_idx, recent_xd_ema = crossdown_emas[-1]
        # Only consider lows from the recent crossdown bar to the current bar.
        # This restricts the measurement to the CURRENT leg down and ignores
        # any older deeper lows from earlier dumps that have since recovered.
        lows_since_recent_xd = lows[recent_xd_idx:]
        lowest_since_recent_xd = min(lows_since_recent_xd) if lows_since_recent_xd else 0

        if recent_xd_ema and recent_xd_ema > 0 and lowest_since_recent_xd > 0:
            drop_pct_path_c = (recent_xd_ema - lowest_since_recent_xd) / recent_xd_ema * 100.0
            path_c_drop_anchor = f"recent_crossdown@{recent_xd_idx}"
        else:
            drop_pct_path_c = 0.0
            path_c_drop_anchor = "invalid"
    else:
        # No crossdown → reuse the general drop_pct (highest_high based)
        drop_pct_path_c = drop_pct
        path_c_drop_anchor = f"fallback_{drop_anchor_type}"

    # Momentum (close > close N bars ago) — Path B only
    price_rising = closes[-1] > closes[-1 - MOMENTUM_LOOKBACK]

    # ─── Crossover detection ─────────────────────────────────────────────────
    # STRICT (Pine-exact): cross on the current bar only.
    cross_up_strict = (close_prev <= ema_prev) and (last_close > ema_now)

    # RESCUE: a cross within the last CROSS_LOOKBACK bars (inclusive of current).
    # This exists so a viable signal isn't thrown away when slope/volume happen
    # to fail on the exact bar the cross fires. All other filters still need to
    # pass on the current bar — we only relax *when* the cross occurred.
    cross_up_recent, cross_bars_ago = had_recent_crossup(closes, ema_values, CROSS_LOOKBACK)

    # Anti-chase guard: don't arm if price has already run far above EMA.
    # Without this, a recent-cross rescue could fire on a bar where price is
    # already overextended and there's no meaningful room left to the TP.
    ema_distance_pct  = ((last_close - ema_now) / ema_now * 100.0) if ema_now else 0
    not_overextended  = ema_distance_pct <= MAX_EMA_DISTANCE_PCT
    price_above_ema   = last_close > ema_now

    # Final cross gate used by both paths:
    #   - strict fires alone (Pine behavior), OR
    #   - recent cross + price still above EMA + not overextended
    cross_valid = cross_up_strict or (
        cross_up_recent and price_above_ema and not_overextended
    )

    # Proximity zone for retest
    proximity_level = ema_now * (1 + PROXIMITY_PCT / 100)

    # ─── Get/init per-symbol state ───────────────────────────────────────────
    st = all_state.get(symbol)
    if st is None:
        st = init_symbol_state()
        all_state[symbol] = st

    # =========================================================================
    # TP COMPLETED MONITORING  (matches sample bot behavior)
    # =========================================================================
    # Runs BEFORE the position/order checks so it fires even while a trade is
    # live on the exchange. Critical: if we only ran this after position checks,
    # we'd miss the TP event because:
    #   - While position exists → early return skips this block
    #   - After position closes via TP → price may already be back below TP,
    #     so Rules 2 & 3 would fail and col B would stay numeric forever
    #
    # Col B semantics:
    #   - "" or empty     : no live trade on this coin; scanning allowed
    #   - numeric (float) : live trade — stored TP price for this coin
    #   - "TP COMPLETED"  : most recent trade closed at TP; do NOT re-enter
    # =========================================================================
    tp_raw = df.iloc[row, 1] if df.shape[1] > 1 else ""

    # Rule 1: explicit "TP COMPLETED" marker — skip this symbol entirely
    if str(tp_raw).strip().upper() == "TP COMPLETED":
        print(f"[SKIP] {symbol} — TP COMPLETED marker in sheet, not re-entering")
        save_state(all_state)
        return

    # Rules 2 & 3: if col B has a numeric TP, watch for it being hit.
    try:
        tp_stored = float(str(tp_raw).strip())
    except (ValueError, TypeError):
        tp_stored = None

    if tp_stored is not None and tp_stored > 0:
        tp_hit = False
        hit_kind = None
        hit_price = None

        # Rule 2 — TP hit by current 15m CLOSE
        if last_close >= tp_stored:
            tp_hit    = True
            hit_kind  = "close"
            hit_price = last_close

        # Rule 3 — TP hit by a WICK between scans (1-min high over last 15 min)
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
            # Clear local position state — the exchange's TP leg will (or already did) close the position
            if st.get("in_position"):
                all_state[symbol] = init_symbol_state()
            save_state(all_state)
            return

    # =========================================================================
    # RECONCILE WITH EXCHANGE
    # =========================================================================
    position = get_position_by_pair(symbol)

    # --- Case A: We have an active position on the exchange -----------------
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

        # Position exists — no trailing updates. TP/SL already set on exchange at entry.
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
            f"🛑 SL was   : <code>{st.get('sl_price')}</code>"
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
    vol_ratio = round(last_vol / vol_avg, 2) if vol_avg else 0

    # Describe the cross state for logs
    if cross_up_strict:
        cross_log = "strict(now)"
    elif cross_up_recent:
        cross_log = f"recent({cross_bars_ago}b ago)"
    else:
        cross_log = "none"

    print(
        f"[SCAN] {symbol} | close={last_close} ema200={round(ema_now, precision)} | "
        f"below%={round(below_pct_actual, 1)} (need >={BELOW_PCT_MIN}) | "
        f"slope%={round(ema_slope_pct, 3)} (need >={MIN_EMA_SLOPE_PCT}) | "
        f"vol={round(last_vol, 2)} avg={round(vol_avg, 2)} ratio={vol_ratio}x | "
        f"drop%={round(drop_pct, 2)} from {drop_anchor_type} (xd_count={crossdown_count}, need >={MIN_DROP_PCT}) | "
        f"dropC%={round(drop_pct_path_c, 2)} from {path_c_drop_anchor} (need >={PATH_C_MIN_DROP_PCT}) | "
        f"cross={cross_log} crossValid={cross_valid} | "
        f"distEMA={round(ema_distance_pct, 2)}% (max {MAX_EMA_DISTANCE_PCT}%) notOver={not_overextended} | "
        f"trendQ={trend_qualifies} slopeOK={slope_ok} volOK={vol_ok} dropOK={drop_ok} "
        f"priceRising={price_rising} waitingRetest={st['waiting_retest']} "
        f"pathC_armed={st.get('path_c_armed', False)}"
    )

    # =========================================================================
    # PATH A — WAITING RETEST STATE  (already armed in a prior scan)
    # =========================================================================
    if st["waiting_retest"]:
        wait_start = st.get("wait_start_candle_ts")
        if wait_start is None:
            st["waiting_retest"] = False
            save_state(all_state)
        else:
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

            # Retest confirmed: low dipped into proximity zone AND close stayed above EMA
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
                    st["sl_price"]             = round(sl_price,    precision)
                    update_sheet_tp(row, st["tp_level"])
                    update_sheet_sl(row, st["sl_price"])

                save_state(all_state)
                return

            # Still waiting — fall through so breakout path can still evaluate
            # (Pine does NOT gate breakoutEntry on waitingRetest, and when trend
            #  qualifies the breakout `not trendQualifies` guard blocks it anyway.)
            print(f"[WAIT] {symbol} — bars_waiting={bars_waiting}/{MAX_RETEST_BARS}")

    # =========================================================================
    # PATH A — ARM NEW REVERSAL SETUP
    # Pine reference:
    #   newSetup = trendQualifies and crossUp and slopeOK and volOK
    #            and strategy.position_size == 0 and not waitingRetest
    #
    # Adapted: `cross_up` is replaced with `cross_valid` so a cross that
    # happened up to CROSS_LOOKBACK bars ago can still arm the setup, provided
    # price is still above EMA and not overextended. All other filters still
    # evaluate on the CURRENT bar, unchanged from Pine.
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
            f"⌛ Waiting up to {MAX_RETEST_BARS} × 15m candles for retest"
        )
        save_state(all_state)
        return

    # =========================================================================
    # PATH B — MOMENTUM BREAKOUT  (fires when trend does NOT qualify)
    # Pine reference:
    #   breakoutEntry = useBreakoutPath and not trendQualifies and crossUp
    #                 and priceRising and breakoutVolOK and strategy.position_size == 0
    #
    # Adapted: same rescue-window treatment for the cross as Path A.
    # `cross_valid` already enforces price_above_ema and not_overextended
    # when the cross is rescued, so we don't re-check those here.
    # =========================================================================
    if USE_BREAKOUT_PATH:
        breakout_entry = ((not trend_qualifies)
                          and cross_valid
                          and price_rising
                          and breakout_vol_ok
                          and drop_ok)
        if breakout_entry:
            cross_detail = "strict" if cross_up_strict else f"rescued ({cross_bars_ago}b ago)"
            print(f"[BREAKOUT] {symbol} — entering long (Path B, cross:{cross_detail}, drop:{round(drop_pct, 2)}%)")

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
                st["sl_price"]             = round(sl_price,    precision)
                update_sheet_tp(row, st["tp_level"])
                update_sheet_sl(row, st["sl_price"])

            save_state(all_state)
            return

    # =========================================================================
    # PATH C — SUPPORT BOUNCE  (multi-TF pivot confluence)
    # =========================================================================
    # Two-stage logic:
    #   Stage 1 (WAITING): If a zone is already armed, watch for the bounce.
    #     - Track whether price has dipped INTO the zone (touched)
    #     - Once touched, wait for a 15m close strictly above the zone upper edge
    #     - On such a close → enter long
    #     - Cancel the armed zone if:
    #         • wait exceeds PATH_C_MAX_WAIT_BARS, OR
    #         • price has fallen more than 2% below zone_low (zone broken)
    #
    #   Stage 2 (ARM): Otherwise, if conditions hold, arm a new zone:
    #     - Drop from highest-EMA crossdown ≥ PATH_C_MIN_DROP_PCT
    #     - Find nearest confluence support zone BELOW current price
    #       (zone defended by ≥ MIN_TF_CONFLUENCE of 4 timeframes)
    #     - Store it and wait for the bounce in a future scan
    #
    # This path is independent of trend_qualifies / cross_valid / slope — it
    # operates purely on structural price levels across multiple timeframes.
    # =========================================================================
    if USE_PATH_C:
        # ── STAGE 1: already armed — watch for bounce ──────────────────────
        if st.get("path_c_armed"):
            zone_low    = st.get("path_c_zone_low")
            zone_high   = st.get("path_c_zone_high")
            zone_center = st.get("path_c_zone_center")
            armed_ts    = st.get("path_c_start_ts")

            # Sanity: if any state missing, clear and continue
            if None in (zone_low, zone_high, zone_center, armed_ts):
                print(f"[PATH-C] {symbol} — incomplete armed state, clearing")
                st["path_c_armed"] = False
                save_state(all_state)
            else:
                bars_waiting = max(0, int((last_ts - armed_ts) // (CANDLE_SECONDS * 1000)))

                # Zone-broken check: price traded well below zone (abandoned)
                touch_margin     = zone_low * (1 - PATH_C_TOUCH_TOLERANCE_PCT / 100.0)
                broken_threshold = zone_low * (1 - 2.0 / 100.0)  # 2% below zone_low = broken

                # Mark "touched" if low dipped into or near the zone
                if not st.get("path_c_zone_touched"):
                    if last_low <= zone_high * (1 + PATH_C_TOUCH_TOLERANCE_PCT / 100.0) \
                       and last_low >= touch_margin:
                        st["path_c_zone_touched"] = True
                        print(f"[PATH-C] {symbol} — zone TOUCHED at low {last_low} (zone {zone_low}–{zone_high})")

                # Zone broken?
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
                    save_state(all_state)
                    return

                # Timeout?
                if bars_waiting > PATH_C_MAX_WAIT_BARS:
                    print(f"[PATH-C] {symbol} — wait timed out ({bars_waiting} > {PATH_C_MAX_WAIT_BARS})")
                    st["path_c_armed"]        = False
                    st["path_c_zone_low"]     = None
                    st["path_c_zone_high"]    = None
                    st["path_c_zone_center"]  = None
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = None
                    st["path_c_tf_count"]     = None
                    save_state(all_state)
                    return

                # Bounce confirmed?
                reclaim_confirmed = (st.get("path_c_zone_touched")
                                     and last_close > zone_high)

                if reclaim_confirmed:
                    print(f"[PATH-C] {symbol} — RECLAIM CONFIRMED close {last_close} > zone_high {zone_high}")

                    # Final placement guards
                    if get_position_by_pair(symbol) is not None:
                        print(f"[ABORT] {symbol} — position appeared just before placement")
                        return
                    if has_open_order(symbol):
                        print(f"[ABORT] {symbol} — order appeared just before placement")
                        return

                    entry_price = last_close
                    tp_price    = entry_price * (1 + TP_PCT / 100)
                    # SL for Path C: fixed % below zone_low (structural level).
                    # Decoupled from SL_BELOW_EMA_PCT (which governs Paths A/B)
                    # so Path C's SL stays anchored to the support band regardless
                    # of any tuning applied to the EMA-based paths.
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
                        st["in_position"]         = True
                        st["entry_path"]          = "support_bounce"
                        st["entry_price"]         = round(entry_price, precision)
                        st["tp_level"]            = round(tp_price,    precision)
                        st["sl_price"]            = round(sl_price,    precision)
                        update_sheet_tp(row, st["tp_level"])
                        update_sheet_sl(row, st["sl_price"])

                    save_state(all_state)
                    return

                # Still waiting
                touched_str = "touched" if st.get("path_c_zone_touched") else "awaiting touch"
                print(f"[PATH-C] {symbol} — waiting ({bars_waiting}/{PATH_C_MAX_WAIT_BARS}b, {touched_str}), "
                      f"zone {round(zone_low, precision)}–{round(zone_high, precision)}, close {last_close}")
                save_state(all_state)
                return

        # ── STAGE 2: not armed — evaluate whether to arm a new zone ────────
        # Prereq: Path C-specific drop measurement ≥ PATH_C_MIN_DROP_PCT.
        # This uses the MOST RECENT crossdown (not the max-EMA one used by
        # Paths A/B) to measure the current leg down. Falls back to the
        # general drop_pct if no crossdown exists in the window.
        path_c_drop_ok = drop_pct_path_c >= PATH_C_MIN_DROP_PCT

        if path_c_drop_ok:
            # Find the nearest confluence support zone below current price
            zone = find_nearest_support_zone_below(symbol, last_close)

            if zone is not None:
                # Sanity: zone must actually be below current price by some margin
                # (the check inside find_nearest_support_zone_below already enforces high < current_price,
                # but we double-check here for safety)
                if zone["high"] < last_close:
                    tf_count   = len(zone["tfs"])
                    tfs_str    = ",".join(sorted(zone["tfs"]))
                    zone_low   = zone["low"]
                    zone_high  = zone["high"]
                    zone_cent  = zone["center"]
                    dist_pct   = (last_close - zone_cent) / last_close * 100.0

                    print(f"[PATH-C] {symbol} — ARMING zone {round(zone_low, precision)}–{round(zone_high, precision)} "
                          f"(center {round(zone_cent, precision)}, {tf_count} TFs: {tfs_str}, "
                          f"{round(dist_pct, 2)}% below close, drop_c {round(drop_pct_path_c, 2)}% from {path_c_drop_anchor})")

                    st["path_c_armed"]        = True
                    st["path_c_zone_low"]     = zone_low
                    st["path_c_zone_high"]    = zone_high
                    st["path_c_zone_center"]  = zone_cent
                    st["path_c_zone_touched"] = False
                    st["path_c_start_ts"]     = last_ts
                    st["path_c_tf_count"]     = tf_count

                    send_telegram(
                        f"🟠 <b>PATH C ARMED (support bounce) — {symbol}</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📍 Close      : <code>{last_close}</code>\n"
                        f"🧱 Zone low   : <code>{round(zone_low, precision)}</code>\n"
                        f"🧱 Zone high  : <code>{round(zone_high, precision)}</code>\n"
                        f"📊 Zone cent  : <code>{round(zone_cent, precision)}</code>\n"
                        f"⏬ Dist below : <code>{round(dist_pct, 2)}%</code>\n"
                        f"📉 Drop (C)   : <code>{round(drop_pct_path_c, 2)}% ({path_c_drop_anchor})</code>\n"
                        f"🪢 Confluence : <code>{tf_count} TFs ({tfs_str})</code>\n"
                        f"⌛ Waiting up to {PATH_C_MAX_WAIT_BARS} × 15m bars for bounce"
                    )
                    save_state(all_state)
                    return

    # No action this cycle
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
    f"⏱ Timeframe  : <code>15 Min (closed bars only)</code>\n"
    f"🅰️ Path A    : <code>{BELOW_PCT_MIN}% below EMA + cross + slope≥{MIN_EMA_SLOPE_PCT}% + vol×{VOL_MULTIPLIER} + drop≥{MIN_DROP_PCT}% → wait retest (≤{MAX_RETEST_BARS} bars)</code>\n"
    f"🅱️ Path B    : <code>cross + close&gt;close[{MOMENTUM_LOOKBACK}] + vol×{BREAKOUT_VOL_MULT} + drop≥{MIN_DROP_PCT}% when trend NOT qualifying</code>\n"
    f"🆎 Path C    : <code>drop≥{PATH_C_MIN_DROP_PCT}% (from most-recent crossdown) + nearest multi-TF pivot zone below price ({MIN_TF_CONFLUENCE}/{len(PATH_C_ENABLED_TIMEFRAMES)} TFs) → wait bounce + 15m reclaim (≤{PATH_C_MAX_WAIT_BARS} bars)</code>\n"
    f"🔀 Cross      : <code>strict OR within last {CROSS_LOOKBACK} bars (if price still above EMA and ≤{MAX_EMA_DISTANCE_PCT}% away)</code>\n"
    f"📉 Drop       : <code>max-EMA across all crossdowns (or highest-high if never crossed) → lowest-low across last {DROP_LOOKBACK} bars must be ≥{MIN_DROP_PCT}%</code>\n"
    f"🧱 Pivots     : <code>N={PIVOT_STRENGTH} each side, ±{PIVOT_ZONE_PCT}% zone band, TFs: {', '.join(PATH_C_ENABLED_TIMEFRAMES)}m/h</code>\n"
    f"🎯 TP         : <code>{TP_PCT}% fixed above entry</code>\n"
    f"🛑 SL         : <code>EMA × (1 - {SL_BELOW_EMA_PCT}%) for Paths A/B, zone_low × (1 - {PATH_C_SL_BELOW_ZONE_PCT}%) for Path C</code>\n"
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