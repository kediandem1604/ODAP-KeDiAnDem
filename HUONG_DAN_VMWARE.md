# Hướng Dẫn Chạy Hadoop trên VMware và Push Dữ Liệu lên Power BI

## ✅ Thông Tin API Power BI

**Push URL:** 
```
https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D
```

---

## Bước 1: Cấu Hình File .env

### 1.1. Mở hoặc tạo file `.env`

```bash
cd ODAP-22-TamBeo
nano .env
# hoặc
vim .env
# hoặc dùng editor bất kỳ
```

### 1.2. Thêm/ Cập nhật các dòng sau:

```bash
# Power BI Configuration
POWERBI_PUSH_URL=https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D

# HDFS Configuration
HDFS_NAMENODE_URL=hdfs://localhost:9000
HDFS_WEB_HOST=localhost:9870
HDFS_USER=panda
HDFS_COMPACTED_PATH=/credit_card_data/compacted

# File lưu timestamp push cuối
LAST_PUSH_FILE=/tmp/last_push_time.txt
```

### 1.3. Lưu file

---

## Bước 2: Khởi Động Hadoop trên VMware

### 2.1. Mở Terminal trong VMware

- Mở terminal trong máy ảo Linux (Ubuntu/CentOS)
- Hoặc SSH vào máy ảo từ máy host

### 2.2. Kiểm Tra Hadoop Đã Cài Đặt

```bash
# Kiểm tra Hadoop version
hadoop version

# Kiểm tra biến môi trường
echo $HADOOP_HOME
echo $JAVA_HOME
```

**Nếu chưa cài đặt Hadoop:**
- Xem hướng dẫn cài đặt Hadoop ở cuối file này

### 2.3. Khởi Động HDFS

```bash
# Di chuyển đến thư mục Hadoop
cd $HADOOP_HOME

# Khởi động HDFS
sbin/start-dfs.sh

# Hoặc nếu đã có trong PATH:
start-dfs.sh
```

### 2.4. Kiểm Tra HDFS Đã Chạy

```bash
# Kiểm tra các process
jps

# Kết quả mong đợi:
# - NameNode
# - DataNode  
# - SecondaryNameNode
```

### 2.5. Kiểm Tra Web UI

- **NameNode Web UI:** http://localhost:9870
- Mở trình duyệt và truy cập để xem HDFS đang chạy

### 2.6. Kiểm Tra Kết Nối HDFS

```bash
# Kiểm tra HDFS có hoạt động không
hdfs dfsadmin -report

# Kiểm tra thư mục gốc
hdfs dfs -ls /

# Tạo thư mục test (nếu cần)
hdfs dfs -mkdir -p /credit_card_data/parquet
hdfs dfs -mkdir -p /credit_card_data/final
hdfs dfs -mkdir -p /credit_card_data/compacted
```

---

## Bước 3: Kiểm Tra Dữ Liệu trong HDFS

### 3.1. Kiểm Tra Có Dữ Liệu Parquet Không

```bash
# Kiểm tra thư mục Parquet
hdfs dfs -ls /credit_card_data/parquet

# Nếu có file, xem chi tiết
hdfs dfs -ls -h /credit_card_data/parquet
```

### 3.2. Kiểm Tra Có CSV Batch Không

```bash
# Kiểm tra thư mục Final (CSV batch)
hdfs dfs -ls /credit_card_data/final

# Xem nội dung file (nếu có)
hdfs dfs -cat /credit_card_data/final/batch_*.csv | head -20
```

### 3.3. Kiểm Tra File Compacted

```bash
# Kiểm tra file compacted
hdfs dfs -ls /credit_card_data/compacted
```

---

## Bước 4: Chạy Pipeline Xử Lý Dữ Liệu

### 4.1. Gộp Parquet thành CSV Batch (Nếu Có Dữ Liệu Parquet Mới)

```bash
cd ODAP-22-TamBeo

# Chạy script gộp Parquet thành CSV batch
spark-submit hadoop/merge_parquet_to_csv.py
```

**Kết quả:**
- File CSV batch sẽ được tạo trong `/credit_card_data/final`
- Tên file: `batch_YYYYMMDD_HHMMSS.csv`

### 4.2. Gộp CSV Batch thành File Compacted

```bash
cd ODAP-22-TamBeo

# Chạy script gộp các batch CSV thành 1 file compacted
spark-submit hadoop/compact_csv.py
```

**Kết quả:**
- File CSV compacted sẽ được tạo trong `/credit_card_data/compacted`
- Tên file: `part-00000-*.csv`

### 4.3. Push Dữ Liệu lên Power BI

```bash
cd ODAP-22-TamBeo

# Chạy script push dữ liệu lên Power BI
python3 powerbi/load_data.py
```

**Kết quả:**
- Script đọc file CSV compacted từ HDFS
- Lọc dữ liệu mới (dựa trên timestamp)
- Tính toán các cột: TxnDate, Hour, TimeBucket60Min, TimeBucket2H, DayOfWeekNum, DayOfWeekName, IsWeekend, HasErrorFlag, IsFraudFlag
- Push dữ liệu lên Power BI qua API
- Dashboard tự động cập nhật

---

## Bước 5: Chạy Tự Động qua Airflow (Khuyến nghị)

### 5.1. Khởi Động Airflow Scheduler

**Terminal 1:**

```bash
cd ODAP-22-TamBeo
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

**Để terminal này chạy (không đóng)**

### 5.2. Khởi Động Airflow Webserver

**Terminal 2 (Terminal mới):**

```bash
cd ODAP-22-TamBeo
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

**Để terminal này chạy (không đóng)**

### 5.3. Truy Cập Airflow Web UI

1. **Mở trình duyệt:** http://localhost:8080
2. **Đăng nhập** (nếu cần):
   - Username: `admin`
   - Password: `admin` (hoặc password bạn đã set)

### 5.4. Kích Hoạt DAG

1. **Tìm DAG:** `send_data_to_powerbi`
2. **Toggle ON** (nút bật/tắt ở bên trái tên DAG) để kích hoạt
3. **DAG sẽ tự động:**
   - Chạy `compact_csv.py` mỗi 10 phút
   - Chạy `load_data.py` để push lên Power BI
   - Dashboard tự động cập nhật

---

## Bước 6: Kiểm Tra và Monitor

### 6.1. Kiểm Tra Airflow

- Vào http://localhost:8080
- Xem DAG có chạy đều không (mỗi 10 phút)
- Xem log nếu có lỗi

### 6.2. Kiểm Tra Power BI

1. **Vào Power BI Web:** https://app.powerbi.com
2. **Vào dataset "RealTimeData"**
3. **Xem tab "Data"** → Có dữ liệu chưa?
4. **Vào dashboard** → Visualizations có cập nhật không?

### 6.3. Kiểm Tra HDFS

```bash
# Xem tất cả dữ liệu trong HDFS
hdfs dfs -ls -R /credit_card_data

# Xem nội dung file CSV (10 dòng đầu)
hdfs dfs -cat /credit_card_data/compacted/part-*.csv | head -10

# Đếm số dòng trong file
hdfs dfs -cat /credit_card_data/compacted/part-*.csv | wc -l
```

---

## Troubleshooting

### Lỗi: "NameNode is not formatted"

```bash
# Format NameNode (CHỈ chạy lần đầu hoặc khi reset)
hdfs namenode -format
```

⚠️ **Cảnh báo:** Format sẽ xóa toàn bộ dữ liệu trong HDFS!

### Lỗi: "Connection refused" khi kết nối HDFS

- Kiểm tra HDFS đã khởi động chưa: `jps`
- Kiểm tra port: NameNode thường ở port 9000 hoặc 9870
- Kiểm tra firewall: `sudo ufw status`
- Kiểm tra file `.env` có cấu hình đúng `HDFS_NAMENODE_URL` không

### Lỗi: "Permission denied" trên HDFS

```bash
# Cấp quyền cho thư mục
hdfs dfs -chmod -R 777 /credit_card_data

# Hoặc đổi owner
hdfs dfs -chown -R panda:panda /credit_card_data
```

### Lỗi: "Không tìm thấy file CSV compacted"

- Chạy `spark-submit hadoop/compact_csv.py` trước
- Kiểm tra có dữ liệu trong `/credit_card_data/final` không
- Kiểm tra script có chạy thành công không

### Lỗi: "401 Unauthorized" khi push data

- Kiểm tra Push URL có đúng không
- Kiểm tra key trong URL có đầy đủ không
- Có thể URL đã hết hạn, cần lấy lại từ Power BI

### Lỗi: "Module not found" khi chạy Python

```bash
# Cài đặt các thư viện cần thiết
pip install -r requirements.txt

# Hoặc cài từng cái
pip install pandas hdfs requests python-dotenv
```

### DAG không chạy trong Airflow

- Kiểm tra Airflow scheduler có chạy không (Terminal 1)
- Kiểm tra DAG có được toggle ON chưa
- Xem log để tìm lỗi cụ thể: Click vào DAG → Task → Log

---

## Cài Đặt Hadoop trên VMware (Nếu Chưa Có)

### Yêu Cầu:
- Java JDK 8 hoặc 11
- SSH đã cấu hình
- Tài khoản user có quyền sudo

### Các Bước:

1. **Cài đặt Java:**
```bash
sudo apt update
sudo apt install openjdk-8-jdk
java -version
```

2. **Tải Hadoop:**
```bash
cd ~
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar -xzf hadoop-3.3.4.tar.gz
sudo mv hadoop-3.3.4 /opt/hadoop
```

3. **Cấu hình Environment Variables:**
```bash
# Thêm vào ~/.bashrc
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Reload
source ~/.bashrc
```

4. **Cấu hình Hadoop:**
- Chỉnh sửa các file trong `$HADOOP_HOME/etc/hadoop/`
- Xem hướng dẫn chi tiết: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

5. **Format và Khởi Động:**
```bash
hdfs namenode -format
start-dfs.sh
```

---

## Tóm Tắt Lệnh Quan Trọng

```bash
# 1. Khởi động HDFS
start-dfs.sh

# 2. Kiểm tra HDFS
jps
hdfs dfs -ls /credit_card_data

# 3. Gộp Parquet → CSV batch
spark-submit hadoop/merge_parquet_to_csv.py

# 4. Gộp CSV batch → Compacted
spark-submit hadoop/compact_csv.py

# 5. Push lên Power BI
python3 powerbi/load_data.py

# 6. Chạy Airflow (2 terminal)
# Terminal 1:
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler

# Terminal 2:
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

---

## Kết Quả Mong Đợi

✅ HDFS chạy và lưu trữ dữ liệu Parquet  
✅ Script tự động gộp Parquet → CSV batch → Compacted  
✅ Script tự động push dữ liệu từ HDFS lên Power BI  
✅ Dashboard Power BI tự động cập nhật mỗi 10 phút  
✅ Không cần can thiệp thủ công  

Chúc bạn thành công! 🎉

