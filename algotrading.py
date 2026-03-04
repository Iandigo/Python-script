import time
import requests
import os
import threading
import logging
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from vnstock import stock_historical_data
except ImportError:
    raise ImportError("Vui lòng cài: pip install vnstock")

load_dotenv()

# ==========================================
# PARSE ARGUMENT: --test
# ==========================================
parser = argparse.ArgumentParser(description="VN30 Trend Following Bot")
parser.add_argument("--test", action="store_true",
    help="Chạy TEST mode: bypass giờ thị trường, dùng dữ liệu lịch sử, không gửi Telegram thật")
parser.add_argument("--test-date", type=str, default=None, metavar="YYYY-MM-DD",
    help="[Test] Giả lập ngày giao dịch cụ thể (mặc định: ngày GD gần nhất)")
parser.add_argument("--tickers", type=str, default=None, metavar="ACB,FPT",
    help="[Test] Chỉ quét các mã này, cách nhau bằng dấu phẩy")
args = parser.parse_args()
TEST_MODE = args.test

# ==========================================
# CẤU HÌNH
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv('TOKEN_TREND_BOT')
TELEGRAM_CHAT_ID   = os.getenv('TELEGRAM_CHAT_ID')

VN30_FULL = [
    'ACB','BCM','BID','BVH','CTG','FPT','GAS','GVR','HDB','HPG',
    'MBB','MSN','MWG','PLX','POW','SAB','SHB','SSB','SSI','STB',
    'TCB','TPB','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VRE'
]
VN30         = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else VN30_FULL
SHORT_WINDOW = 20
LONG_WINDOW  = 50
RSI_PERIOD   = 14
RSI_OB       = 70
RSI_OS       = 30
COOLDOWN_MIN = 60
MAX_WORKERS  = 10

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename=f"logs/trend_{datetime.now().strftime('%Y%m')}.log",
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    encoding='utf-8'
)

# ==========================================
# TRẠNG THÁI TOÀN CỤC
# ==========================================
alert_cache: dict = {}
daily_stats: dict = {"buy": [], "sell": [], "date": None}
cache_lock        = threading.Lock()
test_results      = []

# ==========================================
# TIỆN ÍCH
# ==========================================
def log(msg: str):
    print(msg)
    logging.info(msg)

def send_telegram(message: str, reply_markup: dict = None):
    if TEST_MODE:
        import re
        clean = re.sub(r'<[^>]+>', '', message)
        print("\n" + "─" * 45)
        print("📨 [TEST - KHÔNG GỬI TELEGRAM THẬT]")
        print(clean)
        print("─" * 45)
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log(f"[ERROR] Gửi Telegram thất bại: {e}")

def chart_buttons(ticker: str) -> dict:
    return {"inline_keyboard": [[
        {"text": "📈 VietStock",   "url": f"https://finance.vietstock.vn/{ticker}/do-thi-ky-thuat.htm"},
        {"text": "📊 TradingView", "url": f"https://www.tradingview.com/chart/?symbol=HOSE:{ticker}"}
    ]]}

def is_market_open() -> bool:
    if TEST_MODE:
        return True                   # ← Bypass khi test
    now = datetime.now()
    if now.weekday() > 4:
        return False
    t = now.time()
    return (
        datetime.strptime("09:00","%H:%M").time() <= t <= datetime.strptime("11:30","%H:%M").time()
        or
        datetime.strptime("13:00","%H:%M").time() <= t <= datetime.strptime("15:00","%H:%M").time()
    )

def is_end_of_session() -> bool:
    now = datetime.now()
    return now.weekday() <= 4 and now.hour == 15 and now.minute == 5

def get_date_range():
    if TEST_MODE:
        if args.test_date:
            anchor = datetime.strptime(args.test_date, "%Y-%m-%d")
        else:
            anchor = datetime.now() - timedelta(days=1)
            while anchor.weekday() > 4:          # Lùi qua cuối tuần
                anchor -= timedelta(days=1)
        end_date   = anchor.strftime('%Y-%m-%d')
        start_date = (anchor - timedelta(days=365)).strftime('%Y-%m-%d')
        log(f"[TEST] Giả lập ngày giao dịch: {end_date}")
    else:
        end_date   = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    return start_date, end_date

def reset_daily_state():
    global alert_cache, daily_stats
    today = datetime.now().date()
    with cache_lock:
        if daily_stats.get("date") != today:
            alert_cache = {}
            daily_stats = {"buy": [], "sell": [], "date": today}
            log("🔄 Reset cache & thống kê ngày mới.")

def in_cooldown(ticker: str, signal: str) -> bool:
    if TEST_MODE:
        return False                  # ← Không cooldown khi test
    with cache_lock:
        sent_at = alert_cache.get(ticker, {}).get(signal)
    return bool(sent_at and (datetime.now() - sent_at) < timedelta(minutes=COOLDOWN_MIN))

def mark_sent(ticker: str, signal: str):
    with cache_lock:
        if ticker not in alert_cache:
            alert_cache[ticker] = {}
        alert_cache[ticker][signal] = datetime.now()

def update_stats(ticker: str, signal: str):
    with cache_lock:
        daily_stats[signal].append(ticker)

# ==========================================
# CHỈ BÁO KỸ THUẬT
# ==========================================
def calc_sma(series, window):
    return series.rolling(window=window).mean()

def calc_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def signal_strength(ratio: float) -> str:
    if ratio >= 1.03:   return "⭐⭐⭐ Rất mạnh"
    if ratio >= 1.015:  return "⭐⭐ Trung bình"
    return "⭐ Yếu"

# ==========================================
# XỬ LÝ TỪNG MÃ
# ==========================================
def fetch_with_retry(ticker, start, end, retries=3):
    for attempt in range(retries):
        try:
            df = stock_historical_data(
                symbol=ticker, start_date=start, end_date=end,
                resolution='1D', type='stock', beautify=True)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            if attempt == retries - 1:
                log(f"[WARN] {ticker} thất bại sau {retries} lần: {e}")
            time.sleep(1 * (attempt + 1))
    return None

def process_ticker(ticker: str, start: str, end: str):
    df = fetch_with_retry(ticker, start, end)

    if df is None or len(df) < LONG_WINDOW + 2:
        if TEST_MODE:
            test_results.append({"ticker": ticker, "signal": "⚠️ Lỗi/Thiếu data",
                                  "close": 0, "rsi": 0, "sma20": 0, "sma50": 0, "position": "", "strength": ""})
        return None

    close     = df['close']
    sma_s     = calc_sma(close, SHORT_WINDOW)
    sma_l     = calc_sma(close, LONG_WINDOW)
    rsi       = calc_rsi(close, RSI_PERIOD)

    ts, tl    = sma_s.iloc[-1],  sma_l.iloc[-1]
    ys, yl    = sma_s.iloc[-2],  sma_l.iloc[-2]
    p2s, p2l  = sma_s.iloc[-3],  sma_l.iloc[-3]
    t_close   = close.iloc[-1]
    t_rsi     = rsi.iloc[-1]
    t_date    = df.index[-1] if hasattr(df.index[-1], 'strftime') else datetime.now().strftime('%d/%m/%Y')
    ratio     = ts / tl if tl != 0 else 1.0
    strength  = signal_strength(ratio)
    position  = "📈 Trên SMA50" if ts > tl else "📉 Dưới SMA50"

    if TEST_MODE:
        test_results.append({"ticker": ticker, "signal": "⚪ Không có",
                              "close": t_close, "rsi": t_rsi,
                              "sma20": ts, "sma50": tl,
                              "position": position, "strength": strength})

    # ── TÍN HIỆU MUA ──
    if (ts > tl) and (ys <= yl) and (p2s <= p2l) and (t_rsi < RSI_OB):
        if in_cooldown(ticker, "buy"): return None
        mark_sent(ticker, "buy");  update_stats(ticker, "buy")
        if TEST_MODE: test_results[-1]["signal"] = "🟢 MUA"
        msg = (f"🟢 <b>TÍN HIỆU MUA: {ticker}</b>\n"
               f"├ SMA{SHORT_WINDOW} <b>cắt LÊN</b> SMA{LONG_WINDOW}\n"
               f"├ Giá đóng cửa: <b>{t_close:,.0f} VNĐ</b>\n"
               f"├ RSI({RSI_PERIOD}): <b>{t_rsi:.1f}</b>\n"
               f"├ Độ mạnh: <b>{strength}</b>\n"
               f"└ Ngày: {t_date}")
        return (ticker, "buy", msg)

    # ── TÍN HIỆU BÁN ──
    if (ts < tl) and (ys >= yl) and (p2s >= p2l) and (t_rsi > RSI_OS):
        if in_cooldown(ticker, "sell"): return None
        mark_sent(ticker, "sell"); update_stats(ticker, "sell")
        if TEST_MODE: test_results[-1]["signal"] = "🔴 BÁN"
        msg = (f"🔴 <b>TÍN HIỆU BÁN: {ticker}</b>\n"
               f"├ SMA{SHORT_WINDOW} <b>cắt XUỐNG</b> SMA{LONG_WINDOW}\n"
               f"├ Giá đóng cửa: <b>{t_close:,.0f} VNĐ</b>\n"
               f"├ RSI({RSI_PERIOD}): <b>{t_rsi:.1f}</b>\n"
               f"├ Độ mạnh: <b>{strength}</b>\n"
               f"└ Ngày: {t_date}")
        return (ticker, "sell", msg)

    return None

# ==========================================
# IN BẢNG TEST
# ==========================================
def print_test_table():
    print("\n" + "═" * 78)
    print(f"  {'MÃ':<6} {'TÍN HIỆU':<16} {'GIÁ ĐÓNG':>12} {'RSI':>7} {'SMA20':>10} {'SMA50':>10}  VỊ TRÍ")
    print("─" * 78)
    for r in sorted(test_results, key=lambda x: x.get("signal",""), reverse=True):
        if r["close"] == 0:
            print(f"  {r['ticker']:<6} {r['signal']}")
            continue
        print(f"  {r['ticker']:<6} {r['signal']:<16} {r['close']:>12,.0f} "
              f"{r['rsi']:>7.1f} {r['sma20']:>10,.0f} {r['sma50']:>10,.0f}  {r['position']}")
    print("═" * 78)
    buys  = [r['ticker'] for r in test_results if "MUA" in r.get("signal","")]
    sells = [r['ticker'] for r in test_results if "BÁN" in r.get("signal","")]
    print(f"  🟢 MUA ({len(buys)}): {', '.join(buys) or 'Không có'}")
    print(f"  🔴 BÁN ({len(sells)}): {', '.join(sells) or 'Không có'}")
    print("═" * 78 + "\n")

# ==========================================
# TỔNG KẾT PHIÊN
# ==========================================
def send_daily_summary():
    with cache_lock:
        buys  = list(daily_stats["buy"])
        sells = list(daily_stats["sell"])
    if not buys and not sells: return
    today = datetime.now().strftime('%d/%m/%Y')
    msg = (f"📊 <b>TỔNG KẾT PHIÊN {today}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"🟢 Tín hiệu MUA ({len(buys)}): <b>{', '.join(buys) or 'Không có'}</b>\n"
           f"🔴 Tín hiệu BÁN ({len(sells)}): <b>{', '.join(sells) or 'Không có'}</b>\n"
           f"━━━━━━━━━━━━━━━━━━━\n"
           f"📌 Chiến lược: SMA{SHORT_WINDOW}/{LONG_WINDOW} + RSI{RSI_PERIOD}")
    send_telegram(msg)
    log("📊 Đã gửi tổng kết phiên.")

# ==========================================
# QUÉT CHÍNH
# ==========================================
def scan_all():
    reset_daily_state()
    test_results.clear()
    t0 = time.time()
    start_date, end_date = get_date_range()
    label = "[TEST]" if TEST_MODE else "[LIVE]"
    log(f"{label} [{datetime.now().strftime('%H:%M:%S')}] Đang quét {len(VN30)} mã...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t, start_date, end_date): t for t in VN30}
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    for ticker, signal, msg in results:
        send_telegram(msg, reply_markup=chart_buttons(ticker))

    if TEST_MODE:
        print_test_table()

    log(f"⏳ {len(VN30)} mã / {time.time()-t0:.2f}s / Tín hiệu: {len(results)}")

# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    if TEST_MODE:
        print("╔══════════════════════════════════════════╗")
        print("║       🧪  CHẠY Ở CHẾ ĐỘ TEST MODE       ║")
        print("║  • Bypass giờ thị trường                 ║")
        print("║  • Dùng dữ liệu ngày GD gần nhất        ║")
        print("║  • Không gửi Telegram thật               ║")
        print("║  • Không cooldown                        ║")
        print("╚══════════════════════════════════════════╝\n")
        scan_all()   # Chạy 1 lần rồi thoát
    else:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            print("LỖI: Thiếu TOKEN_TREND_BOT hoặc TELEGRAM_CHAT_ID trong .env")
        else:
            log("🚀 VN30 Trend Following Bot đã khởi động!")
            summary_sent_today = None
            while True:
                now = datetime.now()
                if is_end_of_session() and summary_sent_today != now.date():
                    send_daily_summary()
                    summary_sent_today = now.date()
                if is_market_open():
                    scan_all()
                    time.sleep(300)
                else:
                    log(f"[{now.strftime('%H:%M:%S')}] Thị trường nghỉ. Bot đang chờ...")
                    time.sleep(300)