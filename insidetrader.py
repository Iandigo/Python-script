import sqlite3
import feedparser
import requests
from datetime import datetime, timedelta
import time
import os
import schedule
from dotenv import load_dotenv

# ==========================================
# 1. CẤU HÌNH THÔNG SỐ 
# ==========================================
# Nhớ có dấu trừ (-) ở đầu nếu là Group Chat ID
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("⚠️ LỖI CHÍ MẠNG: Không tìm thấy Token hoặc Chat ID trong file .env!")

DB_FILENAME = 'openclaw_master_data.db'

NEWS_CATEGORIES = {
    "VĨ MÔ & TIỀN TỆ": [
        'lãi suất', 'tỷ giá', 'nhnn', 'fed', 'hút tiền', 'bơm tiền', 
        'tín phiếu', 'lạm phát', 'gdp', 'tín dụng'
    ],
    "PHÁP LÝ & RỦI RO": [
        'khởi tố', 'bắt giam', 'thanh tra', 'kiểm tra', 'kỷ luật',
        'thao túng', 'hủy niêm yết', 'đình chỉ giao dịch', 'phạt vi phạm'
    ],
    "DOANH NGHIỆP & CỔ TỨC": [
        'lợi nhuận kỷ lục', 'báo cáo tài chính', 'chia cổ tức', 'chốt quyền',
        'phát hành thêm', 'tăng vốn', 'trúng thầu', 'đại hội cổ đông'
    ],
    "GIAO DỊCH CÁ MẬP": [
        'đăng ký mua', 'đăng ký bán', 'mua thỏa thuận', 'bán thỏa thuận',
        'chào mua công khai', 'hoàn tất mua', 'hoàn tất bán', 'đã mua', 'đã bán', 
        'mua bất thành', 'bán bất thành', 'không mua hết', 'nâng sở hữu', 
        'giảm sở hữu', 'nâng tỷ lệ', 'giảm tỷ lệ', 'thoái vốn', 'thoái toàn bộ', 
        'thoái sạch', 'trở thành cổ đông lớn', 'không còn là cổ đông lớn',
        'gom thêm', 'gom mạnh', 'gom cổ phiếu', 'xả hàng', 'bán sạch', 
        'chốt lời', 'bán chui', 'quỹ ngoại mua', 'quỹ ngoại bán', 'sang tay'
    ]
}

# ==========================================
# 2. KHỞI TẠO DATABASE (SQLITE)
# ==========================================
def init_db():
    """Tạo file cơ sở dữ liệu và bảng nếu chưa tồn tại"""
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_log (
            link TEXT PRIMARY KEY,
            ngay_quet TEXT,
            chu_de TEXT,
            nguon TEXT,
            tieu_de TEXT
        )
    ''')
    conn.commit()
    conn.close()

# ==========================================
# 3. CÁC HÀM XỬ LÝ CHÍNH
# ==========================================
def send_telegram_message(message):
    """Gửi tin nhắn Telegram và chia nhỏ nếu tin quá dài (giới hạn 4096 ký tự)"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_length = 4000
    messages_to_send = [message[i:i+max_length] for i in range(0, len(message), max_length)]
    
    for msg_chunk in messages_to_send:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg_chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True 
        }
        try:
            response = requests.post(url, json=payload)
            if response.status_code != 200:
                print(f"⚠️ Lỗi gửi Telegram: {response.text}")
        except Exception as e:
            print(f"⚠️ Lỗi mạng khi gửi Telegram: {e}")

def job_scan_news():
    now = datetime.now()
    print(f"\n[{now.strftime('%d/%m/%Y %H:%M:%S')}] 🦅 OPENCLAW đang quét thị trường...")
    
    db_insert_data = []
    messages_by_category = {cat: [] for cat in NEWS_CATEGORIES.keys()}
    
    rss_sources = {
        "CafeF": "https://cafef.vn/doanh-nghiep.rss",
        "Vietstock": "https://vietstock.vn/rss/doanh-nghiep.vi",
        "VNExpress_KinhDoanh": "https://vnexpress.net/rss/kinh-doanh.rss"
    }

    # Mở kết nối Database một lần cho toàn bộ quá trình quét
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    
    # Tập hợp tạm để chống trùng lặp các bài báo xuất hiện nhiều lần trong cùng 1 lần quét
    processed_links_this_run = set()

    # Quét dữ liệu RSS
    for source, url in rss_sources.items():
        feed = feedparser.parse(url)
        if feed.status not in [200, 301, 302]: continue
            
        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            title_lower = title.lower()
            
            if link and link not in processed_links_this_run:
                # TRUY VẤN DATABASE: Kiểm tra xem link này đã từng được lưu chưa?
                cursor.execute("SELECT 1 FROM news_log WHERE link = ?", (link,))
                is_exists = cursor.fetchone()
                
                # Nếu CHƯA TỒN TẠI trong Database, tiến hành phân tích
                if not is_exists:
                    matched_category = None
                    for category, keywords in NEWS_CATEGORIES.items():
                        if any(kw in title_lower for kw in keywords):
                            matched_category = category
                            break
                    
                    if matched_category:
                        ngay_quet = now.strftime('%Y-%m-%d %H:%M:%S')
                        # Chuẩn bị dữ liệu để lưu vào Database
                        db_insert_data.append((link, ngay_quet, matched_category, source, title))
                        
                        # Chuẩn bị tin nhắn Telegram
                        formatted_news = f"🔹 <a href='{link}'>{title}</a> <i>({source})</i>"
                        messages_by_category[matched_category].append(formatted_news)
                        
                        processed_links_this_run.add(link)

    # Nếu có tin mới, lưu vào DB và gửi Telegram
    if db_insert_data:
        # Ghi hàng loạt vào SQLite (cực nhanh)
        cursor.executemany('''
            INSERT INTO news_log (link, ngay_quet, chu_de, nguon, tieu_de) 
            VALUES (?, ?, ?, ?, ?)
        ''', db_insert_data)
        
        # DỌN RÁC: Xóa các bản ghi cũ hơn 30 ngày để file DB không bị phình to
        thirty_days_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("DELETE FROM news_log WHERE ngay_quet < ?", (thirty_days_ago,))
        
        conn.commit()
        
        # Đóng gói và gửi Telegram
        final_message = f"🦅 <b>BẢN TIN OPENCLAW ({now.strftime('%H:%M')})</b>\n\n"
        for category, items in messages_by_category.items():
            if items:
                unique_items = list(set(items))
                final_message += f"<b>📌 {category}</b>\n"
                final_message += "\n".join(unique_items) + "\n\n"
        
        print(f"✅ Tìm thấy {len(db_insert_data)} tin quan trọng. Đã lưu DB và gửi Telegram.")
        send_telegram_message(final_message)
    else:
        print("Trạng thái: Yên tĩnh. Không có tin tức nóng nào xuất hiện.")

    # Luôn đóng kết nối Database khi hoàn thành job
    conn.close()

# ==========================================
# 4. HỆ THỐNG LẬP LỊCH CHẠY MỖI 30 PHÚT
# ==========================================
if __name__ == "__main__":
    print("=== HỆ THỐNG OPENCLAW ĐÃ KHỞI ĐỘNG ===")
    print("Công nghệ lưu trữ: SQLite3 siêu tốc.")
    print("Lịch trình: Quét liên tục mỗi 30 phút.")
    
    # Khởi tạo Database nếu chưa có
    init_db()
    
    # Lên lịch chạy lặp lại mỗi 30 phút
    schedule.every(30).minutes.do(job_scan_news)

    # Chạy lần đầu tiên để gom toàn bộ dữ liệu đang có trên RSS
    print("\n[Hệ thống] Đang chạy lần đầu tiên...")
    job_scan_news() 

    # Vòng lặp giữ cho script luôn thức
    while True:
        schedule.run_pending()
        time.sleep(60)