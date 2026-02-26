import pandas as pd
import requests
import time
from vnstock import stock_historical_data, price_board
from datetime import datetime, timedelta
import os
import schedule
from dotenv import load_dotenv

load_dotenv()

# --- CẤU HÌNH ---
TOKEN_VOLUME_BOT = os.getenv('TOKEN_VOLUME_BOT')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
WATCHLIST = ['VHM', 'MBB', 'HPG', 'ACB', 'VCB', 'STB', 'TCB', 'FPT', 'VIC']
SLEEP_TIME = 300  # Thời gian nghỉ giữa các lần quét (300 giây = 5 phút)

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TOKEN_VOLUME_BOT}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Lỗi gửi Telegram: {e}")

def is_market_open():
    """Kiểm tra xem hiện tại có phải giờ giao dịch không (Thứ 2-6, 9:00 - 15:00)"""
    now = datetime.now()
    if now.weekday() > 4:  # Thứ 7 và Chủ Nhật
        return False
    
    # Định nghĩa khung giờ (9:00 sáng đến 15:00 chiều)
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
    
    return start_time <= now <= end_time

def run_moltbot():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Moltbot đang quét dòng tiền...")
    alerts = []
    
    try:
        live_data = price_board(WATCHLIST)
        # Fix lỗi nếu API trả về dataframe có tên cột khác hoặc rỗng
        if live_data.empty:
            return
        live_data.set_index('Mã CP', inplace=True) 
    except Exception as e:
        print(f"Moltbot lỗi API giá: {e}")
        return

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')

    for ticker in WATCHLIST:
        try:
            # Lấy dữ liệu lịch sử để tính TB 20 phiên
            hist_df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
            
            if len(hist_df) >= 20:
                past_20_sessions = hist_df.iloc[-21:-1] 
                avg_vol_20 = past_20_sessions['volume'].mean()
                
                # Lấy Volume hiện tại từ bảng giá (Lưu ý: đơn vị của vnstock có thể khác nhau tùy thời điểm API)
                current_vol = live_data.loc[ticker, 'Tổng KL'] # Thay 'KL Khớp Lệnh' bằng 'Tổng KL' để lấy vol tích lũy trong ngày
                
                if current_vol > (avg_vol_20 * 1.5):
                    ratio = current_vol / avg_vol_20
                    price = live_data.loc[ticker, 'Giá Khớp Lệnh']
                    change = live_data.loc[ticker, '% Thay đổi'] # Kiểm tra lại tên cột chính xác của vnstock
                    
                    alerts.append(
                        f"🔥 <b>{ticker} - ĐỘT BIẾN VOLUME</b>\n"
                        f"▪️ Giá: {price} ({change}%)\n"
                        f"▪️ Vol hiện tại: {current_vol:,.0f}\n"
                        f"▪️ TB 20 phiên: {avg_vol_20:,.0f}\n"
                        f"➡️ <b>Gấp {ratio:.1f} lần!</b>"
                    )
        except Exception as e:
            print(f"Lỗi khi phân tích {ticker}: {e}")
            continue

    if alerts:
        msg = "🚨 <b>MOLTBOT: PHÁT HIỆN DÒNG TIỀN</b> 🚨\n\n" + "\n\n".join(alerts)
        send_telegram(msg)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Đã gửi báo cáo!")

if __name__ == "__main__":
    print("Moltbot đã được kích hoạt và đang chạy ngầm...")
    
    while True:
        if is_market_open():
            try:
                run_moltbot()
            except Exception as e:
                print(f"Lỗi hệ thống: {e}")
            
            # Đợi 5 phút trước khi quét lại
            time.sleep(SLEEP_TIME)
        else:
            # Nếu ngoài giờ giao dịch, kiểm tra lại sau mỗi 30 phút
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ngoài giờ giao dịch. Đang nghỉ...")
            time.sleep(1800)