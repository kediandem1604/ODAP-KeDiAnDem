#!/usr/bin/env python3
"""
Script gộp các batch CSV thành 1 file CSV duy nhất trên Hadoop
Đọc tất cả batch CSV từ /credit_card_data/final, gộp thành 1 file compacted
"""

from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime
from dotenv import load_dotenv, find_dotenv

# Load config
load_dotenv(find_dotenv())

# Cấu hình từ .env
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE_URL", "hdfs://localhost:9000")
CSV_INPUT_DIR = f"{HDFS_NAMENODE}{os.getenv('HDFS_FINAL_OUTPUT_PATH', '/credit_card_data/final')}"
CSV_COMPACTED_DIR = f"{HDFS_NAMENODE}{os.getenv('HDFS_COMPACTED_PATH', '/credit_card_data/compacted')}"

def main():
    print("="*60)
    print("BẮT ĐẦU GỘP CSV BATCH THÀNH FILE COMPACTED")
    current_time = datetime.now()
    print(f"Thời gian: {current_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    
    # Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("CompactCSVFiles") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Hadoop FileSystem Config
        sc = spark.sparkContext
        from py4j.java_gateway import java_import
        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        
        fs = sc._jvm.FileSystem.get(
            sc._jvm.java.net.URI(HDFS_NAMENODE),
            sc._jsc.hadoopConfiguration()
        )
        
        input_path_obj = sc._jvm.Path(CSV_INPUT_DIR)
        
        # Kiểm tra thư mục input có tồn tại không
        if not fs.exists(input_path_obj):
            print(f"Không tìm thấy thư mục {CSV_INPUT_DIR}. Dừng.")
            return 0
        
        # Lấy danh sách file CSV batch
        file_status_list = fs.listStatus(input_path_obj)
        csv_files = []
        
        for status in file_status_list:
            path = status.getPath()
            name = path.getName()
            if name.startswith("batch_") and name.endswith(".csv"):
                csv_files.append(str(path))
        
        if not csv_files:
            print("Không tìm thấy file CSV batch nào. Dừng.")
            return 0
        
        print(f"Tìm thấy {len(csv_files)} file CSV batch")
        
        # Đọc tất cả CSV và gộp lại
        print("\nĐang đọc và gộp các file CSV...")
        dfs = []
        for csv_file in csv_files:
            try:
                df = spark.read.option("header", "true").csv(csv_file)
                dfs.append(df)
                print(f"  - Đã đọc: {os.path.basename(csv_file)} ({df.count()} dòng)")
            except Exception as e:
                print(f"  - Lỗi khi đọc {csv_file}: {e}")
                continue
        
        if not dfs:
            print("Không có file CSV nào đọc được. Dừng.")
            return 0
        
        # Gộp tất cả DataFrame
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        total_rows = combined_df.count()
        print(f"\nTổng số dòng sau khi gộp: {total_rows}")
        
        if total_rows == 0:
            print("Không có dữ liệu để gộp. Dừng.")
            return 0
        
        # Tạo thư mục output nếu chưa có
        compacted_path_obj = sc._jvm.Path(CSV_COMPACTED_DIR)
        if not fs.exists(compacted_path_obj):
            fs.mkdirs(compacted_path_obj)
        
        # Xóa các file cũ trong thư mục compacted (chỉ giữ file mới nhất)
        if fs.exists(compacted_path_obj):
            old_files = fs.listStatus(compacted_path_obj)
            for old_file in old_files:
                if old_file.getPath().getName().startswith("part-"):
                    fs.delete(old_file.getPath(), True)
        
        # Ghi ra file CSV compacted (1 file duy nhất)
        print(f"\nĐang ghi file compacted vào: {CSV_COMPACTED_DIR}")
        combined_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(CSV_COMPACTED_DIR)
        
        print("-> Đã gộp thành công!")
        
        print("\n" + "="*60)
        print("HOÀN THÀNH")
        print("="*60)
        return 0
            
    except Exception as e:
        print(f"\nLỖI: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


