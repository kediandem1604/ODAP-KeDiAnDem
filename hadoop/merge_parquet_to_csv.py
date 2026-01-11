#!/usr/bin/env python3
"""
Script gộp Parquet thành CSV trên Hadoop
Đọc tất cả Parquet, gộp thành 1 file CSV duy nhất với tên cố định
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
PARQUET_INPUT_PATH = f"{HDFS_NAMENODE}{os.getenv('HDFS_PARQUET_PATH', '/credit_card_data/parquet')}"
STAGING_PATH = f"{HDFS_NAMENODE}{os.getenv('HDFS_STAGING_PATH', '/credit_card_data/staging_area')}"
CSV_OUTPUT_DIR = f"{HDFS_NAMENODE}{os.getenv('HDFS_FINAL_OUTPUT_PATH', '/credit_card_data/final')}"
TEMP_OUTPUT_PATH = f"{HDFS_NAMENODE}{os.getenv('HDFS_TEMP_PROCESSING_PATH', '/credit_card_data/temp_processing')}"

def main():
    print("="*60)
    print("BẮT ĐẦU CHUYỂN ĐỔI PARQUET -> CSV (STAGING MODE)")
    current_time = datetime.now()
    timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
    print(f"Thời gian: {current_time.strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60)
    
    # Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("MergeParquetToCsv") \
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
        
        input_path_obj = sc._jvm.Path(PARQUET_INPUT_PATH)
        staging_path_obj = sc._jvm.Path(STAGING_PATH)
        
        # 1. Đảm bảo thư mục staging sạch sẽ
        if fs.exists(staging_path_obj):
            fs.delete(staging_path_obj, True)
        fs.mkdirs(staging_path_obj)
        
        # 2. DI CHUYỂN (MOVE) file từ Input -> Staging
        # Chỉ di chuyển các file parquet (tránh di chuyển thư mục _spark_metadata nếu đang ghi)
        print(f"\nKiểm tra dữ liệu mới tại: {PARQUET_INPUT_PATH}")
        if not fs.exists(input_path_obj):
             print("Không tìm thấy thư mục input. Dừng.")
             return 0
             
        file_status_list = fs.listStatus(input_path_obj)
        moved_count = 0
        
        for status in file_status_list:
            path = status.getPath()
            name = path.getName()
            
            # Chỉ move file dữ liệu part-*, bỏ qua _spark_metadata, _SUCCESS
            if name.startswith("part-") and name.endswith(".parquet"):
                dest = sc._jvm.Path(f"{STAGING_PATH}/{name}")
                fs.rename(path, dest)
                moved_count += 1
                
        if moved_count == 0:
            print("Không có file dữ liệu mới (.parquet). Dừng.")
            # Dọn dẹp staging rỗng
            fs.delete(staging_path_obj, True)
            return 0
            
        print(f"-> Đã di chuyển {moved_count} file sang Staging để xử lý an toàn.")
        
        # 3. Đọc dữ liệu từ STAGING
        print(f"\nĐọc dữ liệu từ Staging: {STAGING_PATH}")
        df_parquet = spark.read.parquet(STAGING_PATH)
        row_count = df_parquet.count()
        print(f"-> Tổng số dòng cần xử lý: {row_count}")
        
        if row_count == 0:
            print("File rỗng. Dừng.")
            fs.delete(staging_path_obj, True)
            return 0
        
        # 4. Ghi ra thư mục tạm (để lấy file csv part-*)
        print(f"\nĐang chuyển đổi sang CSV...")
        temp_path_obj = sc._jvm.Path(TEMP_OUTPUT_PATH)
        if fs.exists(temp_path_obj):
            fs.delete(temp_path_obj, True)
            
        df_parquet.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(TEMP_OUTPUT_PATH)
            
        # 5. Tìm file CSV và đổi tên sang Final
        status_list = fs.listStatus(temp_path_obj)
        csv_source_file = None
        
        for status in status_list:
            fname = status.getPath().getName()
            if fname.startswith("part-") and fname.endswith(".csv"):
                csv_source_file = status.getPath()
                break
                
        if csv_source_file:
            dest_filename = f"batch_{timestamp_str}.csv"
            dest_path = sc._jvm.Path(f"{CSV_OUTPUT_DIR}/{dest_filename}")
            
            if not fs.exists(sc._jvm.Path(CSV_OUTPUT_DIR)):
                fs.mkdirs(sc._jvm.Path(CSV_OUTPUT_DIR))
                
            print(f"-> Output file: {dest_filename}")
            
            # Đổi tên và di chuyển vào Final
            rename_success = fs.rename(csv_source_file, dest_path)
            
            if not rename_success:
                print("LỖI NGHIÊM TRỌNG: Rename file CSV thất bại.")
                return 1
                
            print(f"-> Đã lưu thành công.")
            
            # 6. Dọn dẹp an toàn
            print("\nDọn dẹp...")
            fs.delete(temp_path_obj, True)     # Xóa temp csv
            fs.delete(staging_path_obj, True)  # Xóa staging (chứa các file parquet đã xử lý xong)
            # LOGIC CỐT LÕI: Không bao giờ xóa PARQUET_INPUT_PATH
            print("-> Đã xóa Staging area.")
            
            print("\n" + "="*60)
            print("HOÀN THÀNH AN TOÀN")
            print("="*60)
            return 0
        else:
            print("Lỗi: Không tìm thấy file CSV output.")
            return 1
            
    except Exception as e:
        print(f"\nLỖI: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
