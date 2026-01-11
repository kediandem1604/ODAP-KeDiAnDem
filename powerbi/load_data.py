#!/usr/bin/env python3
"""
Script đọc dữ liệu từ HDFS và push lên Power BI
Đọc file CSV compacted, lọc dữ liệu mới dựa trên timestamp và push lên Power BI
"""

import pandas as pd
from hdfs import InsecureClient
import json
import requests
import os
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

# Hàm hỗ trợ xử lý datetime
def parse_iso_z(s: str) -> datetime:
    """Parse ISO string với Z thành datetime"""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def to_iso_z(dt: datetime) -> str:
    """Convert datetime thành ISO string với Z"""
    return dt.isoformat().replace("+00:00", "Z")

def bucket_minutes(dt: datetime, m: int) -> datetime:
    """Làm tròn datetime xuống mỗi m phút"""
    mm = (dt.minute // m) * m
    return dt.replace(minute=mm, second=0, microsecond=0)

def bucket_2h(dt: datetime) -> datetime:
    """Làm tròn datetime xuống mỗi 2 giờ"""
    h2 = (dt.hour // 2) * 2
    return dt.replace(hour=h2, minute=0, second=0, microsecond=0)

# Load config
load_dotenv(find_dotenv())

# === Thông tin HDFS & Power BI ===
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE_URL", "hdfs://localhost:9000")
HDFS_HOST = os.getenv("HDFS_WEB_HOST", "localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "panda")
FOLDER_HDFS_PATH = os.getenv("HDFS_COMPACTED_PATH", "/credit_card_data/compacted/")

# Power BI Push URL - Cần cập nhật sau khi tạo dataset trên Power BI
PUSH_URL = os.getenv("POWERBI_PUSH_URL", "")

# File lưu timestamp lần push cuối
LAST_PUSH_FILE = os.getenv("LAST_PUSH_FILE", "/tmp/last_push_time.txt")

# Khởi tạo HDFS client
HDFS_CLIENT = InsecureClient(f'http://{HDFS_HOST}', user=HDFS_USER)

# 1. Khởi tạo timestamp lần đầu nếu chưa có
if not os.path.exists(LAST_PUSH_FILE):
    with open(LAST_PUSH_FILE, "w") as f:
        f.write("1970-01-01T00:00:00Z")

# Kiểm tra Power BI URL
if not PUSH_URL:
    raise Exception("Vui lòng cấu hình POWERBI_PUSH_URL trong file .env hoặc biến môi trường!")

# 2. Đọc dữ liệu từ HDFS
print("="*60)
print("ĐANG ĐỌC DỮ LIỆU TỪ HDFS")
print("="*60)

try:
    # Lấy danh sách file trong thư mục compacted
    files = HDFS_CLIENT.list(FOLDER_HDFS_PATH)
    
    # Lọc ra file CSV chứa 'part'
    csv_files = [f for f in files if f.endswith('.csv') and 'part' in f]
    
    if not csv_files:
        print("Không tìm thấy file CSV compacted nào.")
        print("Có thể cần chạy compact_csv.py trước.")
        exit(0)
    
    # Sắp xếp để lấy file mới nhất
    csv_files.sort(reverse=True)
    latest_file = csv_files[0]
    
    print(f"Đang đọc file: {latest_file}")
    
    # Đọc file CSV từ HDFS
    with HDFS_CLIENT.read(FOLDER_HDFS_PATH + latest_file, encoding='utf-8') as reader:
        df = pd.read_csv(reader, header=0)
    
    print(f"Tổng số dòng trong file: {len(df)}")
    
except Exception as e:
    print(f"Lỗi khi đọc từ HDFS: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# 3. Đọc mốc thời gian push cuối
with open(LAST_PUSH_FILE, "r") as f:
    last_push_time_str = f.read().strip()
    try:
        last_push_time = pd.to_datetime(last_push_time_str, utc=True)
    except:
        last_push_time = pd.to_datetime("1970-01-01T00:00:00Z", utc=True)

print(f"\nTimestamp push cuối: {last_push_time}")

# 4. Chuẩn hóa và lọc dữ liệu mới
print("\nĐang xử lý và lọc dữ liệu...")

# Tạo cột event_time (Date&Time) từ các trường có sẵn
def create_event_time(row):
    try:
        # Nếu có cột event_time sẵn
        if "event_time" in row and pd.notna(row["event_time"]):
            return pd.to_datetime(row["event_time"], utc=True)
        
        # Nếu có Transaction_Date và Transaction_Time
        if "Transaction_Date" in row and "Transaction_Time" in row:
            date_str = str(row["Transaction_Date"])
            time_str = str(row["Transaction_Time"])
            
            # Parse date: dd/mm/yyyy -> yyyy-mm-dd
            date_parts = date_str.split("/")
            if len(date_parts) == 3:
                day, month, year = date_parts
                datetime_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}T{time_str}"
                return pd.to_datetime(datetime_str, utc=True)
        
        # Nếu có Date&Time
        if "Date&Time" in row and pd.notna(row["Date&Time"]):
            return pd.to_datetime(row["Date&Time"], utc=True)
    except:
        pass
    return None

df["event_time"] = df.apply(create_event_time, axis=1)

# Lọc dữ liệu mới hơn timestamp cuối (bỏ qua các dòng không có event_time hợp lệ)
df_filtered = df[(df["event_time"].notna()) & (df["event_time"] > last_push_time)].copy()

print(f"Số dòng mới cần push: {len(df_filtered)}")

if len(df_filtered) == 0:
    print("Không có dữ liệu mới để push.")
    exit(0)

# 5. Chuyển đổi dữ liệu sang format Power BI API
# Format API: User, Date&Time, Amount, Merchant Name, Merchant City, Errors?, Is Fraud?
# Thêm các cột tính toán: TxnDate, Hour, TimeBucket60Min, TimeBucket2H
rows = []
for _, row in df_filtered.iterrows():
    try:
        # User - lấy từ User hoặc Card
        user = float(row.get("User", row.get("Card", 0))) if pd.notna(row.get("User", row.get("Card", 0))) else 0.0
        
        # Date&Time - format ISO 8601
        date_time_str = row["event_time"].isoformat() if pd.notna(row["event_time"]) else ""
        
        # Parse datetime để tính các cột phụ
        dt = None
        if date_time_str:
            try:
                dt = parse_iso_z(date_time_str)
            except:
                # Nếu không parse được, thử parse từ event_time trực tiếp
                if pd.notna(row["event_time"]):
                    dt = row["event_time"]
                    if isinstance(dt, pd.Timestamp):
                        dt = dt.to_pydatetime()
        
        # Amount - lấy từ Amount hoặc Amount_VND
        amount = float(row.get("Amount", row.get("Amount_VND", 0))) if pd.notna(row.get("Amount", row.get("Amount_VND", 0))) else 0.0
        
        # Merchant Name
        merchant_name = str(row.get("Merchant Name", row.get("Merchant_Name", ""))) if pd.notna(row.get("Merchant Name", row.get("Merchant_Name", ""))) else ""
        
        # Merchant City
        merchant_city = str(row.get("Merchant City", row.get("Merchant_City", ""))) if pd.notna(row.get("Merchant City", row.get("Merchant_City", ""))) else ""
        
        # Errors? - lấy từ Errors? hoặc Errors
        errors = str(row.get("Errors?", row.get("Errors", ""))) if pd.notna(row.get("Errors?", row.get("Errors", ""))) else ""
        
        # Is Fraud? - lấy từ Is Fraud? hoặc Is Fraud
        is_fraud = str(row.get("Is Fraud?", row.get("Is Fraud", ""))) if pd.notna(row.get("Is Fraud?", row.get("Is Fraud", ""))) else ""
        
        # Tạo row với các trường cơ bản
        row_data = {
            "User": user,
            "Date&Time": date_time_str,
            "Amount": amount,
            "Merchant Name": merchant_name,
            "Merchant City": merchant_city,
            "Errors?": errors,
            "Is Fraud?": is_fraud
        }
        
        # Thêm các cột tính toán nếu có datetime hợp lệ
        if dt:
            # TxnDate (YYYY-MM-DD)
            row_data["TxnDate"] = dt.date().isoformat()
            
            # Hour (0-23)
            row_data["Hour"] = dt.hour
            
            # TimeBucket60Min (YYYY-MM-DDTHH:00:00Z)
            row_data["TimeBucket60Min"] = to_iso_z(bucket_minutes(dt, 60))
            
            # TimeBucket2H (YYYY-MM-DDTHH:00:00Z, làm tròn xuống mỗi 2 giờ)
            row_data["TimeBucket2H"] = to_iso_z(bucket_2h(dt))
            
            # DayOfWeekNum (0=Monday, 6=Sunday) - Python weekday: 0=Monday, 6=Sunday
            row_data["DayOfWeekNum"] = dt.weekday()
            
            # DayOfWeekName (tên thứ trong tuần)
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            row_data["DayOfWeekName"] = day_names[dt.weekday()]
            
            # IsWeekend (1 nếu là cuối tuần, 0 nếu không)
            row_data["IsWeekend"] = 1 if dt.weekday() >= 5 else 0
        else:
            # Nếu không có datetime hợp lệ, để giá trị mặc định
            row_data["TxnDate"] = ""
            row_data["Hour"] = 0
            row_data["TimeBucket60Min"] = ""
            row_data["TimeBucket2H"] = ""
            row_data["DayOfWeekNum"] = 0
            row_data["DayOfWeekName"] = ""
            row_data["IsWeekend"] = 0
        
        # HasErrorFlag (1 nếu có lỗi, 0 nếu không)
        row_data["HasErrorFlag"] = 1 if errors and str(errors).strip().lower() not in ["", "no", "none", "false"] else 0
        
        # IsFraudFlag (1 nếu là fraud, 0 nếu không)
        row_data["IsFraudFlag"] = 1 if is_fraud and str(is_fraud).strip().lower() in ["yes", "true", "1"] else 0
        
        rows.append(row_data)
    except Exception as e:
        print(f"Lỗi khi xử lý dòng: {e}")
        import traceback
        traceback.print_exc()
        continue

print(f"\nTổng số dòng chuẩn bị push: {len(rows)}")

# 6. Push dữ liệu lên Power BI theo batch
print("\n" + "="*60)
print("ĐANG PUSH DỮ LIỆU LÊN POWER BI")
print("="*60)

headers = {'Content-Type': 'application/json'}
batch_size = 100

for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    try:
        res = requests.post(PUSH_URL, headers=headers, data=json.dumps(batch), timeout=30)
        if res.status_code == 200:
            print(f"✅ Batch {i//batch_size + 1} ({len(batch)} dòng): Thành công")
        else:
            print(f"❌ Batch {i//batch_size + 1} ({len(batch)} dòng): Lỗi {res.status_code}")
            print(f"   Response: {res.text[:200]}")
    except Exception as e:
        print(f"❌ Batch {i//batch_size + 1}: Lỗi kết nối - {e}")

# 7. Lưu lại timestamp mới nhất
if rows:
    try:
        # Lấy Date&Time từ rows và convert sang datetime
        latest_time = max([pd.to_datetime(r["Date&Time"]) for r in rows if r["Date&Time"]])
        with open(LAST_PUSH_FILE, "w") as f:
            f.write(latest_time.isoformat())
        print(f"\n✅ Đã lưu timestamp mới: {latest_time.isoformat()}")
    except Exception as e:
        print(f"\n⚠️  Lỗi khi lưu timestamp: {e}")

print("\n" + "="*60)
print("HOÀN THÀNH")
print("="*60)

