import time
import requests
import os
from vnstock import stock_historical_data
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# --- CẤU HÌNH ---
TOKEN_VOLUME_BOT = os.getenv('TOKEN_VOLUME_BOT')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
WATCHLIST = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB', 'SSI', 'STB', 
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
]
SENSITIVITY = 2.5       # Độ nhạy (Gấp 2.5 lần trung bình 20 nến 5p)
PRICE_THRESHOLD = 1.0   # Chỉ báo khi giá tăng/giảm trên 1%
MAX_WORKERS = 10        # Số lượng "công nhân" chạy song song

alert_cache = {}  # { ticker: {"buy": candle_time, "sell": candle_time} }

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TOKEN_VOLUME_BOT}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

def is_market_open():
    """Kiểm tra giờ giao dịch chứng khoán Việt Nam"""
    now = datetime.now()
    if now.weekday() > 4: return False 
    
    current_time = now.time()
    m_start = datetime.strptime("09:00", "%H:%M").time()
    m_end = datetime.strptime("11:30", "%H:%M").time()
    a_start = datetime.strptime("13:00", "%H:%M").time()
    a_end = datetime.strptime("15:00", "%H:%M").time()

    return (m_start <= current_time <= m_end) or (a_start <= current_time <= a_end)

def process_single_ticker(ticker, start_date, end_date):
    """Hàm xử lý riêng lẻ cho từng mã cổ phiếu (Sẽ được chạy song song)"""
    try:
        df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='5', type='stock', beautify=True)
        if df is None or len(df) < 21: 
            return None

        current_candle = df.iloc[-1]
        last_20_candles = df.iloc[-21:-1]
        
        avg_vol = last_20_candles['volume'].mean()
        cur_vol = current_candle['volume']
        
        if current_candle['open'] == 0:
            return None
            
        price_change = ((current_candle['close'] - current_candle['open']) / current_candle['open']) * 100
        candle_time = str(current_candle.get('time', 'unknown_time'))
        ratio = cur_vol / avg_vol

        # Khởi tạo cache cho mã nếu chưa có
        if ticker not in alert_cache:
            alert_cache[ticker] = {"buy": None, "sell": None}

        # --- PHÁT HIỆN CÁ MẬP GOM HÀNG (Volume cao + Giá tăng) ---
        if cur_vol > (avg_vol * SENSITIVITY) and price_change >= PRICE_THRESHOLD:
            if alert_cache[ticker]["buy"] == candle_time:
                pass  # Đã báo rồi, bỏ qua
            else:
                alert_cache[ticker]["buy"] = candle_time
                msg = (
                    f"⚡ <b>PHÁT HIỆN DÒNG TIỀN: {ticker}</b>\n"
                    f"▪️ Biến động Vol: <b>Gấp {ratio:.1f} lần</b> (5p)\n"
                    f"▪️ Giá hiện tại: {current_candle['close']:,} (Tăng {price_change:+.2f}%)\n"
                    f"➡️ <b>Tín hiệu: CÁ MẬP ĐANG GOM HÀNG! 🟢</b>"
                )
                return (ticker, msg)

        # --- PHÁT HIỆN CÁ MẬP XẢ HÀNG (Volume cao + Giá giảm) ---
        elif cur_vol > (avg_vol * SENSITIVITY) and price_change <= -PRICE_THRESHOLD:
            if alert_cache[ticker]["sell"] == candle_time:
                pass  # Đã báo rồi, bỏ qua
            else:
                alert_cache[ticker]["sell"] = candle_time
                msg = (
                    f"🔴 <b>CẢNH BÁO XẢ HÀNG: {ticker}</b>\n"
                    f"▪️ Biến động Vol: <b>Gấp {ratio:.1f} lần</b> (5p)\n"
                    f"▪️ Giá hiện tại: {current_candle['close']:,} (Giảm {price_change:+.2f}%)\n"
                    f"➡️ <b>Tín hiệu: CÁ MẬP ĐANG XẢ HÀNG! 🔴</b>"
                )
                return (ticker, msg)
            
    except Exception as e:
        pass
        
    return None

def check_intraday_flow():
    start_time_scan = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Đang quét VN30 (Multi-threading)...")
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    
    alerts_to_send = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_ticker = {executor.submit(process_single_ticker, ticker, start_date, end_date): ticker for ticker in WATCHLIST}
        
        for future in as_completed(future_to_ticker):
            result = future.result()
            if result:
                alerts_to_send.append(result)

    for ticker, msg in alerts_to_send:
        send_telegram(msg)
        # In log khác nhau cho 2 loại tín hiệu
        if "GOM HÀNG" in msg:
            print(f"🟢 Đã gửi báo động GOM: {ticker}")
        else:
            print(f"🔴 Đã gửi báo động XẢ:  {ticker}")

    scan_duration = time.time() - start_time_scan
    print(f"⏳ Hoàn thành quét {len(WATCHLIST)} mã trong {scan_duration:.2f} giây.")

if __name__ == "__main__":
    if not TOKEN_VOLUME_BOT or not CHAT_ID:
        print("LỖI: Chưa cấu hình TOKEN_VOLUME_BOT hoặc TELEGRAM_CHAT_ID trong file .env!")
    else:
        print("🚀 Bot Đa luồng VN30 đã sẵn sàng!")
        while True:
            if is_market_open():
                check_intraday_flow()
                time.sleep(120)
            else:
                now_str = datetime.now().strftime('%H:%M:%S')
                print(f"[{now_str}] Thị trường đang nghỉ. Bot đang chờ...")
                time.sleep(300)