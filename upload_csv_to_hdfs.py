#!/usr/bin/env python3
"""
Script upload CSV từ dataset/ vào HDFS
Upload file CSV vào /credit_card_data/final để sau đó có thể chạy compact_csv.py
"""

import os
from hdfs import InsecureClient
from dotenv import load_dotenv, find_dotenv
from datetime import datetime

load_dotenv(find_dotenv())

HDFS_HOST = os.getenv("HDFS_WEB_HOST", "localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "khtn_22120300")
HDFS_FINAL_PATH = os.getenv("HDFS_FINAL_OUTPUT_PATH", "/credit_card_data/final")

# Khởi tạo HDFS client
print("Đang kết nối với HDFS...")
client = InsecureClient(f'http://{HDFS_HOST}', user=HDFS_USER)

# Đường dẫn file CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
csv_file = os.path.join(BASE_DIR, "dataset", "User0_credit_card_transactions.csv")

# Kiểm tra file có tồn tại không
if not os.path.exists(csv_file):
    print(f"❌ Không tìm thấy file: {csv_file}")
    print(f"   Kiểm tra file có trong thư mục dataset/ không")
    exit(1)

# Tạo tên file với timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
hdfs_filename = f"batch_manual_{timestamp}.csv"
hdfs_path = f"{HDFS_FINAL_PATH}/{hdfs_filename}"

# Upload file
print(f"\nĐang upload {csv_file} vào HDFS...")
print(f"Đích: {hdfs_path}")

try:
    with open(csv_file, 'rb') as f:
        client.write(hdfs_path, f, overwrite=True)
    
    print(f"✅ Đã upload thành công!")
    print(f"   File: {hdfs_path}")
    print(f"\n📋 Bước tiếp theo:")
    print(f"   1. Chạy: spark-submit hadoop/compact_csv.py")
    print(f"   2. Sau đó chạy: python3 powerbi/load_data.py")
    
except Exception as e:
    print(f"❌ Lỗi khi upload: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

