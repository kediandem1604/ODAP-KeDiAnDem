#!/bin/bash

echo "=================================================="
echo "      BẮT ĐẦU RESET HỆ THỐNG CREDIT CARD"
echo "=================================================="

# 1. Reset HDFS
echo "[1/4] Đang dọn dẹp HDFS..."

# Xóa thư mục checkpoint (QUAN TRỌNG: để Spark đọc lại từ đầu)
hdfs dfs -rm -r /credit_card_data/checkpoint 2>/dev/null
echo "  - Đã xóa Checkpoint"

# Xóa dữ liệu Parquet (Dữ liệu thô từ Spark)
hdfs dfs -rm -r /credit_card_data/parquet 2>/dev/null
echo "  - Đã xóa Parquet Raw Data"

# Xóa dữ liệu Final CSV (Dữ liệu báo cáo)
hdfs dfs -rm -r /credit_card_data/final 2>/dev/null
echo "  - Đã xóa Final CSV Data"

# Xóa các thư mục tạm/staging
hdfs dfs -rm -r /credit_card_data/staging_area 2>/dev/null
hdfs dfs -rm -r /credit_card_data/temp_processing 2>/dev/null
echo "  - Đã xóa Temp/Staging"

# Tạo lại thư mục gốc sạch sẽ
hdfs dfs -mkdir -p /credit_card_data
echo "  - Đã tạo lại folder gốc: /credit_card_data"

# 2. Reset Local Files (Logs, Cache)
echo -e "\n[2/4] Đang dọn dẹp file local..."

# Xóa log producer
if [ -f "kafka_producer/kafka_producer.log" ]; then
    rm kafka_producer/kafka_producer.log
    echo "  - Đã xóa kafka_producer.log"
fi

# Xóa cache tỷ giá
if [ -f "spark_streaming/vcb_rate_cache.json" ]; then
    rm spark_streaming/vcb_rate_cache.json
    echo "  - Đã xóa vcb_rate_cache.json"
fi

# 3. Reset Kafka Topic
echo -e "\n[3/4] Đang reset Kafka Topic..."
# Xóa topic cũ (bỏ qua lỗi nếu chưa tồn tại)
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic credit-card-transactions 2>/dev/null || true
echo "  - Đã xóa topic cũ"

# Tạo topic mới
bin/kafka-topics.sh --create --topic credit-card-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo "  - Đã tạo topic mới: credit-card-transactions"

echo -e "\n[4/4] HOÀN TẤT!"
echo "=================================================="
echo "Hệ thống đã sẵn sàng để chạy lại từ đầu."
echo "B1: Chạy Spark Streaming (spark-submit spark_streaming/spark_credit_card_consumer.py)"
echo "B2: Chạy Producer (python kafka_producer/credit_card_producer.py)"
echo "=================================================="
