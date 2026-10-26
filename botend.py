import requests
import pandas as pd
import numpy as np
import time
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import traceback
import warnings

warnings.filterwarnings("ignore")

# ================== КОНФИГУРАЦИЯ ==================
SYMBOL_BINANCE = "BTCUSDT"
TIMEFRAME = "5m"
HISTORY_DAYS = 7
TOUCH_TOLERANCE = 0.007
ATR_MULTIPLIER = 0.8
MIN_RR = 10
USE_TIME_FILTER = True
TIME_START = 8           # 8 UTC = 11 MSK
TIME_END = 20            # 20 UTC = 23 MSK

LEFT_PIVOT = 5
RIGHT_PIVOT = 5
ACTIVE_ZONE_BARS = 60
USE_VOLUME_FILTER = False
USE_ZONE_SELECTION = True

NTFY_TOPIC = "bosbot"

current_position = None
all_trades = []
daily_trades = defaultdict(list)
weekly_trades = defaultdict(list)
last_3r_notification_id = None
last_daily_report_date = None
last_weekly_report_week = None

# ------------------------------------------------------------
# ЦЕНА С BINANCE
# ------------------------------------------------------------
def get_binance_price():
    try:
        url = "https://api.binance.com/api/v3/ticker/price"
        resp = requests.get(url, params={'symbol': SYMBOL_BINANCE}, timeout=5)
        if resp.status_code == 200:
            return float(resp.json()['price'])
    except Exception as e:
        print(f"Binance price error: {e}")
    return None

def get_binance_candles(symbol='BTCUSDT', interval='5m', days=7):
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines"
    limit = 1000
    all_candles = []
    current_start = start_time
    while current_start < end_time:
        params = {'symbol': symbol, 'interval': interval, 'limit': limit,
                  'startTime': current_start, 'endTime': end_time}
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            all_candles.extend(data)
            current_start = data[-1][0] + 1
            time.sleep(0.2)
        except:
            break
    if not all_candles:
        return None
    cleaned = [c[:6] for c in all_candles]
    df = pd.DataFrame(cleaned, columns=['timestamp','open','high','low','close','volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    for col in ['open','high','low','close','volume']:
        df[col] = df[col].astype(float)
    df = df.sort_index()
    start_cut = datetime.now() - timedelta(days=days)
    df = df[df.index >= start_cut]
    df.rename(columns={'open':'open','high':'high','low':'low','close':'close','volume':'volume'}, inplace=True)
    df['ATR'] = calc_atr(df)
    return df

def calc_atr(df, period=14):
    high, low, close = df['high'], df['low'], df['close']
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def find_pivots(df, left, right):
    highs = df['high'].values
    lows = df['low'].values
    n = len(df)
    piv_highs = []
    piv_lows = []
    for i in range(left, n - right):
        if highs[i] == max(highs[i-left:i+right+1]):
            piv_highs.append((i, highs[i]))
        if lows[i] == min(lows[i-left:i+right+1]):
            piv_lows.append((i, lows[i]))
    return piv_highs, piv_lows

def detect_bos(df, i, piv_highs, piv_lows):
    if piv_highs:
        last_piv_high = piv_highs[-1]
        if df['high'].iloc[i] > last_piv_high[1]:
            prev_piv_low = None
            for pl in reversed(piv_lows):
                if pl[0] < last_piv_high[0]:
                    prev_piv_low = pl
                    break
            if prev_piv_low is not None:
                return 'bull', prev_piv_low[1], last_piv_high[1]
    if piv_lows:
        last_piv_low = piv_lows[-1]
        if df['low'].iloc[i] < last_piv_low[1]:
            prev_piv_high = None
            for ph in reversed(piv_highs):
                if ph[0] < last_piv_low[0]:
                    prev_piv_high = ph
                    break
            if prev_piv_high is not None:
                return 'bear', last_piv_low[1], prev_piv_high[1]
    return None, None, None

# ------------------------------------------------------------
# ОТПРАВКА УВЕДОМЛЕНИЙ С ПОВТОРАМИ
# ------------------------------------------------------------
def send_ntfy(title, message, priority="high", max_retries=3):
    url = f"https://ntfy.sh/{NTFY_TOPIC}"
    try:
        title_ascii = title.encode('ascii', 'ignore').decode('ascii')
        message_utf8 = message.encode('utf-8')
        headers = {
            "Title": title_ascii,
            "Priority": priority,
            "Content-Type": "text/plain; charset=utf-8"
        }
        for attempt in range(max_retries):
            try:
                response = requests.post(url, data=message_utf8, headers=headers, timeout=30)
                if response.status_code == 200:
                    print(f"[NTFY] Sent: {title_ascii}")
                    return True
                else:
                    print(f"[NTFY] Status {response.status_code} (attempt {attempt+1})")
            except requests.exceptions.Timeout:
                print(f"[NTFY] Timeout (attempt {attempt+1}/{max_retries})")
            except Exception as e:
                print(f"[NTFY] Error: {e} (attempt {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2)
        print(f"[NTFY] FAILED after {max_retries} attempts: {title_ascii}")
        return False
    except Exception as e:
        print(f"ntfy error: {e}")
        return False

# ------------------------------------------------------------
def update_statistics(trade):
    all_trades.append(trade)
    date_key = trade['date'].strftime('%Y-%m-%d')
    daily_trades[date_key].append(trade)
    week_key = f"{trade['date'].year}-W{trade['date'].isocalendar()[1]:02d}"
    weekly_trades[week_key].append(trade)
    print(f"[DEBUG] Trade added: side={trade['side']}, date={date_key}, daily total={len(daily_trades[date_key])}")

def get_stats(trades):
    if not trades:
        return None
    total = len(trades)
    wins = sum(1 for t in trades if t['pnl'] > 0)
    losses = total - wins
    win_rate = wins / total * 100
    total_pnl = sum(t['pnl'] for t in trades)
    total_risk = sum(t['risk'] for t in trades)
    total_rr = total_pnl / total_risk if total_risk != 0 else 0
    sum_profit_r = sum(t['rr'] for t in trades if t['rr'] > 0)
    sum_loss_r = sum(t['rr'] for t in trades if t['rr'] < 0)
    net_r = sum_profit_r + sum_loss_r
    return {
        'total': total, 'wins': wins, 'losses': losses, 'win_rate': win_rate,
        'total_pnl': total_pnl, 'total_rr': total_rr,
        'sum_profit_r': sum_profit_r, 'sum_loss_r': sum_loss_r, 'net_r': net_r
    }

def send_daily_report(force=False):
    global last_daily_report_date
    today = datetime.now().strftime('%Y-%m-%d')
    if not force and last_daily_report_date == today:
        return
    trades = daily_trades.get(today, [])
    print(f"[DEBUG] Daily report for {today}: {len(trades)} trades")
    if not trades:
        send_ntfy(f"Daily report ({today})", "No trades today", priority="default")
    else:
        stats = get_stats(trades)
        if stats:
            msg = (f"Total: {stats['total']} | Wins: {stats['wins']} | Losses: {stats['losses']}\n"
                   f"Win rate: {stats['win_rate']:.1f}%\n"
                   f"PnL: {stats['total_pnl']:.2f}$ | RR: {stats['total_rr']:.2f}\n"
                   f"Profit R: +{stats['sum_profit_r']:.2f}\n"
                   f"Loss R: {stats['sum_loss_r']:.2f}\n"
                   f"Net R: {stats['net_r']:.2f}")
            send_ntfy(f"Daily report ({today})", msg, priority="default")
    last_daily_report_date = today

def send_weekly_report(force=False):
    global last_weekly_report_week
    today = datetime.now()
    week_key = f"{today.year}-W{today.isocalendar()[1]:02d}"
    if not force and last_weekly_report_week == week_key:
        return
    trades = weekly_trades.get(week_key, [])
    print(f"[DEBUG] Weekly report for {week_key}: {len(trades)} trades")
    if not trades:
        send_ntfy(f"Weekly report ({week_key})", "No trades this week", priority="default")
    else:
        stats = get_stats(trades)
        if stats:
            msg = (f"Total: {stats['total']} | Wins: {stats['wins']} | Losses: {stats['losses']}\n"
                   f"Win rate: {stats['win_rate']:.1f}%\n"
                   f"PnL: {stats['total_pnl']:.2f}$ | RR: {stats['total_rr']:.2f}\n"
                   f"Profit R: +{stats['sum_profit_r']:.2f}\n"
                   f"Loss R: {stats['sum_loss_r']:.2f}\n"
                   f"Net R: {stats['net_r']:.2f}")
            send_ntfy(f"Weekly report ({week_key})", msg, priority="default")
    last_weekly_report_week = week_key

# ------------------------------------------------------------
async def main():
    global current_position, last_3r_notification_id, last_daily_report_date, last_weekly_report_week

    print("Bot BOS Fibonacci Zones started. Active hours: 11:00-23:00 MSK (8-20 UTC).")
    print(f"RR = {MIN_RR}, ATR = {ATR_MULTIPLIER}")
    print("Price source: Binance (BTCUSDT).")
    print("Binance price printed every minute. TP/SL check every second.")

    send_ntfy("Bot started", f"BOS Fibonacci Zones | ATR={ATR_MULTIPLIER}, RR={MIN_RR}, touch={TOUCH_TOLERANCE*100}%, time 11-23 MSK | Price: Binance")

    hist = get_binance_candles(SYMBOL_BINANCE, TIMEFRAME, HISTORY_DAYS)
    if hist is None:
        print("Failed to load history.")
        return

    last_price_print = 0
    last_hist_update = time.time()

    active_zones = []
    left, right = LEFT_PIVOT, RIGHT_PIVOT
    for i in range(100, len(hist)):
        piv_highs, piv_lows = find_pivots(hist.iloc[:i+1], left, right)
        bos_dir, level_a, level_b = detect_bos(hist, i, piv_highs, piv_lows)
        if bos_dir is not None:
            lo = min(level_a, level_b)
            hi = max(level_a, level_b)
            rng = hi - lo
            L1, L2 = 1.414, 1.618
            zone_low_long = lo + rng * L1
            zone_high_long = lo + rng * L2
            zone_low_short = hi - rng * L2
            zone_high_short = hi - rng * L1
            active_zones.append({'side': 'BUY', 'zone_low': zone_low_long, 'zone_high': zone_high_long,
                                 'expiry': i + ACTIVE_ZONE_BARS, 'atr': hist['ATR'].iloc[i]})
            active_zones.append({'side': 'SELL', 'zone_low': zone_low_short, 'zone_high': zone_high_short,
                                 'expiry': i + ACTIVE_ZONE_BARS, 'atr': hist['ATR'].iloc[i]})
    last_idx = len(hist) - 1
    active_zones = [z for z in active_zones if z['expiry'] > last_idx]

    send_daily_report(force=True)
    send_weekly_report(force=True)

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_local = now_utc.astimezone()
            hour_utc = now_utc.hour

            price = get_binance_price()
            if price is None:
                await asyncio.sleep(1)
                continue

            current_ts = time.time()
            if current_ts - last_price_print >= 60:
                print(f"[{now_local.strftime('%H:%M:%S')} MSK] Binance price: {price:.2f}")
                last_price_print = current_ts

            is_active = True
            if USE_TIME_FILTER:
                if hour_utc < TIME_START or hour_utc >= TIME_END:
                    is_active = False

            if is_active and (current_ts - last_hist_update >= 300):
                hist = get_binance_candles(SYMBOL_BINANCE, TIMEFRAME, HISTORY_DAYS)
                if hist is not None:
                    new_zones = []
                    for i in range(100, len(hist)):
                        piv_highs, piv_lows = find_pivots(hist.iloc[:i+1], left, right)
                        bos_dir, level_a, level_b = detect_bos(hist, i, piv_highs, piv_lows)
                        if bos_dir is not None:
                            lo = min(level_a, level_b)
                            hi = max(level_a, level_b)
                            rng = hi - lo
                            L1, L2 = 1.414, 1.618
                            zone_low_long = lo + rng * L1
                            zone_high_long = lo + rng * L2
                            zone_low_short = hi - rng * L2
                            zone_high_short = hi - rng * L1
                            new_zones.append({'side': 'BUY', 'zone_low': zone_low_long, 'zone_high': zone_high_long,
                                              'expiry': i + ACTIVE_ZONE_BARS, 'atr': hist['ATR'].iloc[i]})
                            new_zones.append({'side': 'SELL', 'zone_low': zone_low_short, 'zone_high': zone_high_short,
                                              'expiry': i + ACTIVE_ZONE_BARS, 'atr': hist['ATR'].iloc[i]})
                    last_idx = len(hist) - 1
                    active_zones = [z for z in new_zones if z['expiry'] > last_idx]
                last_hist_update = current_ts

            # Управление позицией (всегда)
            if current_position is not None:
                side = current_position['side']
                entry = current_position['entry']
                sl = current_position['sl']
                tp = current_position['tp']
                risk = current_position['risk']
                pos_id = current_position['id']
                closed = False
                pnl = 0.0
                reason = ''

                if side == 'BUY':
                    if price >= tp:
                        pnl = tp - entry
                        reason = 'TP'
                        closed = True
                    elif price <= sl:
                        pnl = sl - entry
                        reason = 'SL'
                        closed = True
                    else:
                        current_r = (price - entry) / risk
                        if current_r >= 3.0 and last_3r_notification_id != pos_id:
                            send_ntfy("Position +3R", f"Current R: {current_r:.1f}\nEntry: {entry:.2f}\nPrice: {price:.2f}")
                            last_3r_notification_id = pos_id
                else:
                    if price <= tp:
                        pnl = entry - tp
                        reason = 'TP'
                        closed = True
                    elif price >= sl:
                        pnl = entry - sl
                        reason = 'SL'
                        closed = True
                    else:
                        current_r = (entry - price) / risk
                        if current_r >= 3.0 and last_3r_notification_id != pos_id:
                            send_ntfy("Position +3R", f"Current R: {current_r:.1f}\nEntry: {entry:.2f}\nPrice: {price:.2f}")
                            last_3r_notification_id = pos_id

                if closed:
                    rr = pnl / risk
                    side_label = "LONG" if side == 'BUY' else "SHORT"
                    print(f"\nTRADE CLOSED: {side_label} {reason} | PnL: {pnl:.2f} | RR: {rr:.2f}")
                    msg = f"Closed by {reason}\nEntry: {entry:.2f}\nExit: {price:.2f}\nPnL: {pnl:.2f}\nRR: {rr:.2f}"
                    send_ntfy(f"{side_label} {reason}", msg)
                    trade = {'side': side, 'entry_price': entry, 'exit_price': price,
                             'tp_sl': reason, 'pnl': pnl, 'risk': risk, 'rr': rr,
                             'date': datetime.now()}
                    update_statistics(trade)
                    current_position = None
                    last_3r_notification_id = None

            # Поиск сигнала (только в окне)
            if current_position is None and is_active:
                for zone in active_zones:
                    if USE_ZONE_SELECTION:
                        mid = (zone['zone_low'] + zone['zone_high']) / 2
                        if zone['side'] == 'BUY' and price > mid:
                            continue
                        if zone['side'] == 'SELL' and price < mid:
                            continue
                    if zone['zone_low'] <= price <= zone['zone_high']:
                        entry = price
                        atr = zone['atr']
                        if zone['side'] == 'BUY':
                            sl = entry - atr * ATR_MULTIPLIER
                            risk = entry - sl
                            if risk > 0:
                                tp = entry + risk * MIN_RR
                                side_label = "LONG"
                                print(f"\n🔔 {side_label} | Entry: {entry:.2f} | SL: {sl:.2f} | TP: {tp:.2f} | RR: {MIN_RR}")
                                send_ntfy(f"Signal {side_label}", f"Entry: {entry:.2f}\nSL: {sl:.2f}\nTP: {tp:.2f}\nRR: {MIN_RR}")
                                current_position = {'side': 'BUY', 'entry': entry, 'sl': sl, 'tp': tp,
                                                    'risk': risk, 'open_time': datetime.now(),
                                                    'id': int(time.time() * 1000)}
                                break
                        else:
                            sl = entry + atr * ATR_MULTIPLIER
                            risk = sl - entry
                            if risk > 0:
                                tp = entry - risk * MIN_RR
                                side_label = "SHORT"
                                print(f"\n🔔 {side_label} | Entry: {entry:.2f} | SL: {sl:.2f} | TP: {tp:.2f} | RR: {MIN_RR}")
                                send_ntfy(f"Signal {side_label}", f"Entry: {entry:.2f}\nSL: {sl:.2f}\nTP: {tp:.2f}\nRR: {MIN_RR}")
                                current_position = {'side': 'SELL', 'entry': entry, 'sl': sl, 'tp': tp,
                                                    'risk': risk, 'open_time': datetime.now(),
                                                    'id': int(time.time() * 1000)}
                                break

            # Отчёты
            if now_local.hour == 0 and now_local.minute == 0:
                send_daily_report()
            if now_local.weekday() == 0 and now_local.hour == 0 and now_local.minute == 0:
                send_weekly_report()

            await asyncio.sleep(1)

        except Exception as e:
            print(f"Critical error: {e}")
            traceback.print_exc()
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем.")