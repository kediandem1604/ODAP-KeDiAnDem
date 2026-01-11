#!/usr/bin/env python3
"""
Spark Streaming Consumer cho giao dịch thẻ tín dụng
Đọc từ Kafka, xử lý real-time, lưu Parquet vào Hadoop
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, lit, concat, lpad, to_timestamp, substring, dayofweek, when, length, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
import os

# Import module lấy tỷ giá
sys.path.append(os.path.dirname(__file__))
from vietcombank_exchange_rate import VietcombankExchangeRate

# Khởi tạo Spark Session
print("Đang khởi tạo Spark Session...")
spark = SparkSession.builder \
    .appName("CreditCardTransactionProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Cấu hình
# Load config from .env (Hỗ trợ đọc từ thư mục cha hoặc hiện tại)
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Cấu hình
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "credit-card-transactions")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE_URL", "hdfs://localhost:9000")
PARQUET_OUTPUT_PATH = f"{HDFS_NAMENODE}{os.getenv('HDFS_PARQUET_PATH', '/credit_card_data/parquet')}"
CHECKPOINT_PATH = f"{HDFS_NAMENODE}{os.getenv('HDFS_CHECKPOINT_PATH', '/credit_card_data/checkpoint')}"

# Schema cho dữ liệu từ Kafka
schema = StructType([
    StructField("User", StringType(), True),
    StructField("Card", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Day", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# Khởi tạo module lấy tỷ giá
vcb = VietcombankExchangeRate()

def convert_usd_to_vnd(amount_str):
    """Chuyển đổi USD sang VNĐ"""
    try:
        amount_usd = abs(float(amount_str.replace('$', '').replace(',', '')))
        rate = vcb.get_usd_to_vnd_rate()
        amount_vnd = amount_usd * rate
        return round(amount_vnd, 0)
    except Exception as e:
        print(f"Lỗi chuyển đổi: {e}")
        return 0.0

def format_date(year_str, month_str, day_str):
    """Format ngày thành dd/mm/yyyy"""
    try:
        return f"{int(day_str):02d}/{int(month_str):02d}/{year_str}"
    except:
        return ""

def format_time(time_str):
    """Format giờ thành hh:mm:ss"""
    try:
        parts = time_str.split(':')
        if len(parts) == 2:
            return f"{parts[0]}:{parts[1]}:00"
        return time_str
    except:
        return ""

# Đăng ký UDFs (Chỉ giữ lại cái nào cần thiết hoặc chuyển sang native)
convert_usd_to_vnd_udf = udf(convert_usd_to_vnd, DoubleType())

print(f"Đang kết nối với Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Đọc từ topic: {KAFKA_TOPIC}")
print("Bắt đầu xử lý streaming...\n")

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Tách các cột từ JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Lọc dữ liệu
filtered_df = parsed_df.filter(
    (col("Is Fraud?") == "No") & 
    ((col("Errors?").isNull()) | (col("Errors?") == "")) &
    (col("Year").isNotNull()) & (length(col("Year")) > 0) &
    (col("Month").isNotNull()) & (length(col("Month")) > 0) &
    (col("Day").isNotNull()) & (length(col("Day")) > 0) &
    (col("Time").isNotNull()) & (length(col("Time")) > 0)
)

# Xử lý dữ liệu 
print("\nXử lý dữ liệu:")
print("  - Tạo cột event_time (yyyy-MM-dd HH:mm:ss)")
print("  - Tạo cột Hour, DayOfWeek (Tiếng Việt)")
print("  - Chuyển đổi tiền tệ")

processed_df = filtered_df \
    .withColumn("Amount_Cleaned", regexp_replace("Amount", "[$]", "").cast("float")) \
    .withColumn("Amount_VND", convert_usd_to_vnd_udf(col("Amount"))) \
    .withColumn("event_time",
        to_timestamp(
            concat(
                col("Year"), lit("-"),
                lpad(col("Month"), 2, '0'), lit("-"),
                lpad(col("Day"), 2, '0'), lit(" "),
                col("Time"), lit(":00")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    ) \
    .withColumn("Hour", substring(col("Time"), 1, 2).cast("int")) \
    .withColumn(
        "DayOfWeek",
        when(dayofweek(col("event_time")) == 1, "Chủ Nhật")
        .when(dayofweek(col("event_time")) == 2, "Thứ 2")
        .when(dayofweek(col("event_time")) == 3, "Thứ 3")
        .when(dayofweek(col("event_time")) == 4, "Thứ 4")
        .when(dayofweek(col("event_time")) == 5, "Thứ 5")
        .when(dayofweek(col("event_time")) == 6, "Thứ 6")
        .when(dayofweek(col("event_time")) == 7, "Thứ 7")
    ) \
    .withColumnRenamed("Errors?", "Errors") \
    .withColumnRenamed("Is Fraud?", "Is Fraud")

# Chọn cột output
output_df = processed_df.select(
    "User", "Card", "event_time", "Hour", "DayOfWeek", 
    col("Amount_VND").alias("Amount"), 
    "Merchant Name", "Merchant City", "Errors", "Is Fraud"
)

# Ghi vào console
query_console = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Ghi vào Hadoop HDFS dạng Parquet 
print(f"\nLưu dữ liệu vào Hadoop (Parquet): {PARQUET_OUTPUT_PATH}")

query_hadoop = output_df \
    .coalesce(1) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", PARQUET_OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime='30 seconds') \
    .start()

# Chờ streaming
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n\nDừng Spark Streaming...")
finally:
    spark.stop()