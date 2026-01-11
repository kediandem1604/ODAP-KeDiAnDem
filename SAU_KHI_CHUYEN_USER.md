# Hướng Dẫn Sau Khi Chuyển Sang User HDFS

## Bước 1: Di Chuyển Đến Thư Mục Dự Án

```bash
cd ~/ODAP-KeDiAnDem-main
```

---

## Bước 2: Cấu Hình File .env

```bash
# Tạo hoặc chỉnh sửa file .env
nano .env
```

**Nội dung file .env:**
```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=credit-card-transactions

# HDFS Configuration
HDFS_NAMENODE_URL=hdfs://localhost:9000
HDFS_WEB_HOST=localhost:9870
HDFS_USER=khtn_22120300
HDFS_PARQUET_PATH=/credit_card_data/parquet
HDFS_CHECKPOINT_PATH=/credit_card_data/checkpoint
HDFS_FINAL_OUTPUT_PATH=/credit_card_data/final
HDFS_COMPACTED_PATH=/credit_card_data/compacted

# Power BI Configuration
POWERBI_PUSH_URL=https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D

# File lưu timestamp push cuối
LAST_PUSH_FILE=/tmp/last_push_time.txt
```

---

## Bước 3: Kiểm Tra và Khởi Động HDFS

```bash
# Kiểm tra HDFS đã chạy
jps
# Phải thấy: NameNode, DataNode, SecondaryNameNode

# Nếu chưa chạy, khởi động
export HADOOP_HOME=/usr/local/hadoop
cd $HADOOP_HOME
sbin/start-dfs.sh
```

**Tạo thư mục trong HDFS (nếu chưa có):**
```bash
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /credit_card_data/parquet
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /credit_card_data/final
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /credit_card_data/compacted
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /credit_card_data/checkpoint
```

---

## Bước 3.5: Khởi Động Kafka (Nếu Chạy Pipeline Real-time)

**⚠️ Chỉ cần nếu muốn chạy pipeline Kafka Producer → Spark Streaming**

```bash
# Kiểm tra Kafka đã chạy
jps | grep Kafka

# Nếu chưa chạy, khởi động Kafka
# Tìm thư mục Kafka (thường ở ~/kafka hoặc /opt/kafka)
export KAFKA_HOME=~/kafka  # Hoặc đường dẫn của bạn
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft/server.properties

# Hoặc nếu có script start-kafka.sh
~/start-kafka.sh
```

**Kiểm tra Kafka đã chạy:** Mở terminal mới và chạy `jps`, phải thấy process Kafka.

---

## Bước 4: Setup Python Environment

**Cách 1: Dùng Script Tự Động (Khuyến nghị)**

```bash
# Deactivate venv nếu đang active
deactivate 2>/dev/null || true

# Chạy script tự động fix
chmod +x fix_venv.sh
./fix_venv.sh
```

**Cách 2: Tạo Thủ Công**

```bash
# Bước 1: Deactivate venv nếu đang active
deactivate 2>/dev/null || true

# Bước 2: Xóa venv cũ
rm -rf venv 2>/dev/null || sudo rm -rf venv

# Bước 3: Sửa ownership thư mục (nếu cần)
sudo chown -R $USER:$USER ~/ODAP-KeDiAnDem-main

# Bước 4: Cài python3-venv
sudo apt install python3-venv python3-full -y

# Bước 5: Tạo venv mới (KHÔNG dùng sudo)
python3 -m venv venv

# Bước 6: Kích hoạt venv
source venv/bin/activate

# Bước 7: Upgrade pip
pip install --upgrade pip

# Bước 8: Cài đặt dependencies
pip install -r requirements.txt
```

**Kiểm tra:**
```bash
# Đảm bảo venv đã kích hoạt (thấy (venv) ở đầu prompt)
python3 -c "from hdfs import InsecureClient; print('✅ OK')"
```

---

## Bước 5: Upload CSV vào HDFS (BẮT BUỘC - Lần đầu chạy)

**⚠️ Bước này BẮT BUỘC nếu chưa có dữ liệu trong HDFS.**

Bước này upload file CSV từ `dataset/User0_credit_card_transactions.csv` vào HDFS `/credit_card_data/final` để:
- `compact_csv.py` có dữ liệu để gộp
- `load_data.py` có dữ liệu để push lên Power BI

```bash
source venv/bin/activate
python3 upload_csv_to_hdfs.py
```

**Kiểm tra đã upload thành công:**
```bash
# Kiểm tra file trong HDFS
/usr/local/hadoop/bin/hdfs dfs -ls /credit_card_data/final
```

**Lưu ý:** Nếu đã có dữ liệu trong HDFS từ lần chạy trước (từ Kafka Producer → Spark Streaming), có thể bỏ qua bước này.

---

## Bước 5.5: Chạy Pipeline Real-time (Kafka Producer → Spark Streaming)

**⚠️ Tùy chọn: Dùng để tạo dữ liệu real-time vào HDFS**

**Quy trình:**
1. Kafka Producer: Đọc CSV từ `dataset/` → Gửi vào Kafka Topic
2. Spark Streaming: Đọc từ Kafka → Xử lý (chuyển USD→VND, lọc fraud) → Lưu Parquet vào HDFS
3. Sau đó chạy `merge_parquet_to_csv.py` để gộp Parquet thành CSV batch

**Bước 1: Chạy Spark Streaming Consumer (Terminal 1)**

```bash
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate

# Chạy Spark Streaming Consumer (chờ nhận dữ liệu từ Kafka)
spark-submit spark_streaming/spark_credit_card_consumer.py
```

**Bước 2: Chạy Kafka Producer (Terminal 2 - Terminal mới)**

```bash
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate

# Chạy Producer để gửi dữ liệu vào Kafka
python3 kafka_producer/credit_card_producer.py
```

**Bước 3: Gộp Parquet thành CSV Batch (Sau khi Producer chạy xong)**

```bash
# Chờ Producer và Consumer chạy xong (hoặc dừng thủ công)
# Sau đó gộp Parquet thành CSV batch
spark-submit hadoop/merge_parquet_to_csv.py
```

**Kiểm tra kết quả:**
```bash
# Kiểm tra Parquet trong HDFS
/usr/local/hadoop/bin/hdfs dfs -ls /credit_card_data/parquet

# Kiểm tra CSV batch sau khi gộp
/usr/local/hadoop/bin/hdfs dfs -ls /credit_card_data/final
```

---

## Bước 6: Chạy Pipeline (Gộp CSV và Push Power BI)

**Quy trình:**
1. `compact_csv.py`: Đọc CSV từ `/credit_card_data/final` → Gộp thành 1 file → Lưu vào `/credit_card_data/compacted`
2. `load_data.py`: Đọc CSV từ `/credit_card_data/compacted` → Push lên Power BI

```bash
# Kích hoạt venv
source venv/bin/activate

# Bước 1: Gộp CSV
spark-submit hadoop/compact_csv.py

# Bước 2: Push lên Power BI
python3 powerbi/load_data.py
```

**⚠️ Nếu bỏ qua Bước 5:**
- `compact_csv.py` sẽ không tìm thấy file trong `/credit_card_data/final` → Lỗi hoặc không có dữ liệu
- `load_data.py` sẽ không có file compacted để đọc → Không có gì để push lên Power BI

---

## Bước 7: Chạy Tự Động qua Airflow

**Terminal 1:**
```bash
cd ~/ODAP-KeDiAnDem-main
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

**Terminal 2:**
```bash
cd ~/ODAP-KeDiAnDem-main
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

- Mở http://localhost:8080
- Tìm DAG `send_data_to_powerbi` và toggle ON

---

## Lệnh Nhanh

### Cách 1: Upload CSV Trực Tiếp (Nhanh nhất)

```bash
# 1. Di chuyển và kích hoạt venv
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate

# 2. Upload CSV vào HDFS
python3 upload_csv_to_hdfs.py

# 3. Gộp CSV và push lên Power BI
spark-submit hadoop/compact_csv.py
python3 powerbi/load_data.py
```

### Cách 2: Pipeline Real-time (Kafka → Spark Streaming)

**Terminal 1:**
```bash
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate
spark-submit spark_streaming/spark_credit_card_consumer.py
```

**Terminal 2:**
```bash
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate
python3 kafka_producer/credit_card_producer.py
```

**Sau khi Producer chạy xong:**
```bash
# Gộp Parquet thành CSV batch
spark-submit hadoop/merge_parquet_to_csv.py

# Gộp CSV batch thành compacted và push Power BI
spark-submit hadoop/compact_csv.py
python3 powerbi/load_data.py
```

### Cách 3: Chỉ Gộp và Push (Nếu Đã Có Dữ Liệu Trong HDFS)

```bash
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate
spark-submit hadoop/compact_csv.py
python3 powerbi/load_data.py
```
