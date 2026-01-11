# Hướng Dẫn Từ Đầu: Extract và Setup Dự Án

## Bước 1: Extract File ZIP

### 1.1. Di Chuyển Đến Thư Mục Downloads

```bash
cd ~/Downloads
```

### 1.2. Kiểm Tra File ZIP

```bash
ls -lh ODAP-KeDiAnDem-main.zip
```

### 1.3. Extract File ZIP

```bash
# Extract file zip
unzip ODAP-KeDiAnDem-main.zip

# Hoặc nếu chưa có unzip, cài đặt:
# sudo apt install unzip
# unzip ODAP-KeDiAnDem-main.zip
```

### 1.4. Kiểm Tra Thư Mục Đã Extract

```bash
# Xem thư mục đã extract
ls -la

# Di chuyển vào thư mục
cd ODAP-KeDiAnDem-main

# Xem cấu trúc thư mục
ls -la
```

### 1.5. Di Chuyển Thư Mục Đến Vị Trí Phù Hợp (Tùy chọn)

```bash
# Di chuyển về thư mục home
mv ~/Downloads/ODAP-KeDiAnDem-main ~/ODAP-22-TamBeo

# Hoặc giữ nguyên tên
cd ~/ODAP-KeDiAnDem-main
```

---

## Bước 2: Kiểm Tra Cấu Trúc Dự Án

### 2.1. Xem Cấu Trúc Thư Mục

```bash
# Trong thư mục dự án
tree
# hoặc
ls -R
```

**Cấu trúc mong đợi:**
```
ODAP-KeDiAnDem-main/
├── hadoop/
│   ├── compact_csv.py
│   └── merge_parquet_to_csv.py
├── powerbi/
│   └── load_data.py
├── spark_streaming/
│   ├── spark_credit_card_consumer.py
│   └── vietcombank_exchange_rate.py
├── airflow/
│   └── dags/
│       └── load_power_bi.py
├── requirements.txt
├── README.md
└── .env (cần tạo)
```

---

## Bước 3: Cài Đặt Python và Dependencies

### 3.1. Kiểm Tra Python

```bash
# Kiểm tra Python version
python3 --version

# Nếu chưa có, cài đặt:
# sudo apt update
# sudo apt install python3 python3-pip
```

### 3.2. Cài Đặt Thư Viện Python

**⚠️ Lưu ý:** Nếu gặp lỗi "externally-managed-environment", dùng một trong các cách sau:

**Cách 1: Tạo Virtual Environment (Khuyến nghị)**

```bash
# Di chuyển vào thư mục dự án
cd ~/ODAP-KeDiAnDem-main
# hoặc
cd ~/ODAP-22-TamBeo

# Tạo virtual environment
python3 -m venv venv

# Kích hoạt virtual environment
source venv/bin/activate

# Cài đặt các thư viện
pip install -r requirements.txt

# Sau khi cài xong, luôn nhớ kích hoạt venv trước khi chạy script:
# source venv/bin/activate
```

**Cách 2: Dùng --break-system-packages (Nhanh nhưng không khuyến nghị)**

```bash
# Di chuyển vào thư mục dự án
cd ~/ODAP-KeDiAnDem-main

# Cài đặt với flag --break-system-packages
pip3 install --break-system-packages -r requirements.txt
```

**Cách 3: Dùng --user (Cài cho user hiện tại)**

```bash
# Di chuyển vào thư mục dự án
cd ~/ODAP-KeDiAnDem-main

# Cài đặt cho user
pip3 install --user -r requirements.txt
```

### 3.3. Kiểm Tra Các Thư Viện Đã Cài

```bash
# Test import các thư viện
python3 -c "import pandas; import hdfs; import requests; print('OK')"
```

---

## Bước 4: Cấu Hình File .env

### 4.1. Tạo File .env

```bash
# Trong thư mục dự án
nano .env
# hoặc
vim .env
```

### 4.2. Thêm Nội Dung Vào File .env

```bash
# HDFS Configuration
HDFS_NAMENODE_URL=hdfs://localhost:9000
HDFS_WEB_HOST=localhost:9870
HDFS_USER=khtn_22120300
HDFS_COMPACTED_PATH=/credit_card_data/compacted

# Power BI Configuration
POWERBI_PUSH_URL=https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D

# File lưu timestamp push cuối
LAST_PUSH_FILE=/tmp/last_push_time.txt
```

**Lưu ý:** 
- Thay `khtn_22120300` bằng username HDFS của bạn (từ hình ảnh tôi thấy bạn dùng `khtn_22120300`)
- Lưu file: `Ctrl+O`, Enter, `Ctrl+X` (nano) hoặc `:wq` (vim)

---

## Bước 5: Kiểm Tra Hadoop HDFS

### 5.1. Kiểm Tra HDFS Đã Chạy

```bash
# Kiểm tra các process
jps

# Kết quả mong đợi:
# - NameNode
# - DataNode
# - SecondaryNameNode
```

### 5.2. Khởi Động HDFS (Nếu Chưa Chạy)

```bash
# Di chuyển đến thư mục Hadoop
cd $HADOOP_HOME
# hoặc
cd /usr/local/hadoop

# Khởi động HDFS
sbin/start-dfs.sh

# Kiểm tra lại
jps
```

### 5.3. Kiểm Tra Thư Mục Trong HDFS

**⚠️ Lưu ý:** Nếu gặp lỗi "Unknown command: dfs", xem phần Troubleshooting bên dưới.

```bash
# Kiểm tra thư mục đã tạo chưa
hdfs dfs -ls /credit_card_data

# Nếu chưa có, tạo thư mục
hdfs dfs -mkdir -p /credit_card_data/parquet
hdfs dfs -mkdir -p /credit_card_data/final
hdfs dfs -mkdir -p /credit_card_data/compacted
```

**Nếu lệnh `hdfs dfs` không hoạt động, thử:**
```bash
# Cách 1: Dùng đường dẫn đầy đủ
$HADOOP_HOME/bin/hdfs dfs -ls /credit_card_data

# Cách 2: Kiểm tra biến môi trường
echo $HADOOP_HOME
which hdfs

# Cách 3: Di chuyển đến thư mục Hadoop
cd $HADOOP_HOME
bin/hdfs dfs -ls /credit_card_data
```

### 5.4. Kiểm Tra Web UI

- Mở trình duyệt: http://localhost:9870
- Kiểm tra HDFS có hoạt động không

---

## Bước 6: Kiểm Tra Dữ Liệu trong HDFS

### 6.1. Kiểm Tra Có Dữ Liệu Parquet Không

```bash
# Kiểm tra thư mục Parquet
hdfs dfs -ls /credit_card_data/parquet

# Nếu có file, xem chi tiết
hdfs dfs -ls -h /credit_card_data/parquet
```

### 6.2. Kiểm Tra Có CSV Batch Không

```bash
# Kiểm tra thư mục Final
hdfs dfs -ls /credit_card_data/final

# Xem nội dung file (nếu có)
hdfs dfs -cat /credit_card_data/final/batch_*.csv | head -20
```

### 6.3. Kiểm Tra File Compacted

```bash
# Kiểm tra file compacted
hdfs dfs -ls /credit_card_data/compacted

# Nếu không có file, cần chạy compact_csv.py (xem Bước 8)
```

---

## Bước 7: Test Script Push Dữ Liệu

### 7.1. Kiểm Tra File Script

```bash
# Trong thư mục dự án
cd ~/ODAP-KeDiAnDem-main
# hoặc
cd ~/ODAP-22-TamBeo

# Kiểm tra script có tồn tại không
ls -la powerbi/load_data.py
```

### 7.2. Test Kết Nối HDFS

```bash
# Test kết nối HDFS từ Python
python3 -c "from hdfs import InsecureClient; client = InsecureClient('http://localhost:9870', user='khtn_22120300'); print(client.list('/credit_card_data'))"
```

### 7.3. Chạy Script Push Dữ Liệu

```bash
# Nếu dùng virtual environment, kích hoạt trước:
source venv/bin/activate

# Chạy script
python3 powerbi/load_data.py
# hoặc nếu dùng venv:
python powerbi/load_data.py
```

**⚠️ Lưu ý:** Nếu gặp lỗi "Không tìm thấy file CSV compacted", cần chạy `compact_csv.py` trước (xem Bước 8).

**Kết quả mong đợi:**
- Script đọc file CSV từ HDFS
- Lọc dữ liệu mới
- Tính toán các cột: TxnDate, Hour, TimeBucket60Min, TimeBucket2H, DayOfWeekNum, DayOfWeekName, IsWeekend, HasErrorFlag, IsFraudFlag
- Push dữ liệu lên Power BI
- Hiển thị số dòng đã push

---

## Bước 8: Tạo File CSV Compacted (QUAN TRỌNG!)

### 8.1. Kiểm Tra Có Dữ Liệu Trong HDFS

```bash
# Kiểm tra có CSV batch trong /credit_card_data/final không
hdfs dfs -ls /credit_card_data/final

# Kiểm tra có Parquet trong /credit_card_data/parquet không
hdfs dfs -ls /credit_card_data/parquet
```

### 8.2. Nếu Có CSV Batch - Gộp Thành Compacted

**Nếu thấy file CSV batch (batch_*.csv) trong `/credit_card_data/final`:**

```bash
# Trong thư mục dự án
cd ~/Downloads/ODAP-KeDiAnDem-main

# Kích hoạt venv (nếu dùng)
source venv/bin/activate

# Chạy script gộp CSV batch thành compacted
spark-submit hadoop/compact_csv.py
```

**Kiểm tra kết quả:**
```bash
# Kiểm tra file compacted đã được tạo
hdfs dfs -ls /credit_card_data/compacted
```

### 8.3. Nếu Có Parquet - Gộp Thành CSV Batch Trước

**Nếu chỉ có Parquet, cần gộp thành CSV batch trước:**

```bash
# Trong thư mục dự án
cd ~/Downloads/ODAP-KeDiAnDem-main

# Kích hoạt venv (nếu dùng)
source venv/bin/activate

# Bước 1: Gộp Parquet thành CSV batch
spark-submit hadoop/merge_parquet_to_csv.py

# Kiểm tra kết quả
hdfs dfs -ls /credit_card_data/final

# Bước 2: Gộp CSV batch thành compacted
spark-submit hadoop/compact_csv.py

# Kiểm tra kết quả
hdfs dfs -ls /credit_card_data/compacted
```

### 8.4. Nếu Không Có Dữ Liệu - Cần Tạo Dữ Liệu

**Nếu không có dữ liệu trong HDFS, bạn cần:**

1. **Chạy Kafka Producer** để tạo dữ liệu
2. **Chạy Spark Streaming Consumer** để xử lý và lưu vào HDFS
3. **Sau đó mới chạy các script gộp**

Xem hướng dẫn trong `README.md` để chạy toàn bộ pipeline.

---

## Bước 9: Chạy Tự Động qua Airflow (Khuyến nghị)

### 9.1. Kiểm Tra Airflow

```bash
# Kiểm tra Airflow
airflow version
```

### 9.2. Khởi Động Airflow Scheduler

**Terminal 1:**

```bash
cd ~/ODAP-KeDiAnDem-main
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

### 9.3. Khởi Động Airflow Webserver

**Terminal 2 (Terminal mới):**

```bash
cd ~/ODAP-KeDiAnDem-main
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

### 9.4. Truy Cập Airflow Web UI

1. **Mở trình duyệt:** http://localhost:8080
2. **Đăng nhập** (nếu cần):
   - Username: `admin`
   - Password: `admin` (hoặc password bạn đã set)

### 9.5. Kích Hoạt DAG

1. **Tìm DAG:** `send_data_to_powerbi`
2. **Toggle ON** để kích hoạt
3. **DAG sẽ tự động chạy mỗi 10 phút**

---

## Bước 10: Kiểm Tra Kết Quả

### 10.1. Kiểm Tra Power BI

1. **Vào Power BI Web:** https://app.powerbi.com
2. **Vào dataset "RealTimeData"**
3. **Xem tab "Data"** → Có dữ liệu chưa?
4. **Vào dashboard** → Visualizations có cập nhật không?

### 10.2. Kiểm Tra Log Script

```bash
# Xem log khi chạy script
python3 powerbi/load_data.py

# Kiểm tra:
# - Có đọc được file từ HDFS không?
# - Có push được dữ liệu lên Power BI không?
# - Có lỗi gì không?
```

---

## Troubleshooting

### Lỗi: "unzip: command not found"

```bash
sudo apt update
sudo apt install unzip
```

### Lỗi: "externally-managed-environment"

**Giải pháp:** Dùng virtual environment (Cách 1 ở trên) hoặc `--break-system-packages`

### Lỗi: "Module not found" khi chạy Python

```bash
# Nếu dùng virtual environment, đảm bảo đã kích hoạt:
source venv/bin/activate

# Cài đặt lại thư viện
pip install -r requirements.txt

# Hoặc cài từng cái
pip install pandas hdfs requests python-dotenv
```

### Lỗi: "Connection refused" khi kết nối HDFS

- Kiểm tra HDFS đã khởi động: `jps`
- Khởi động HDFS: `start-dfs.sh`
- Kiểm tra file `.env` có cấu hình đúng không

### Lỗi: "Unknown command: dfs"

**Nguyên nhân:** Hadoop chưa được cài đặt đúng hoặc biến môi trường chưa được cấu hình.

**Giải pháp:**

1. **Kiểm tra Hadoop đã cài chưa:**
```bash
# Kiểm tra biến môi trường
echo $HADOOP_HOME
echo $JAVA_HOME

# Kiểm tra file hdfs có tồn tại không
which hdfs
ls -la $HADOOP_HOME/bin/hdfs
```

2. **Nếu chưa có, cài đặt hoặc cấu hình:**
```bash
# Cấu hình biến môi trường trong ~/.bashrc
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Reload
source ~/.bashrc
```

3. **Dùng đường dẫn đầy đủ:**
```bash
# Thay vì: hdfs dfs -ls
# Dùng:
$HADOOP_HOME/bin/hdfs dfs -ls /credit_card_data

# Hoặc:
/usr/local/hadoop/bin/hdfs dfs -ls /credit_card_data
```

4. **Kiểm tra Hadoop đã khởi động chưa:**
```bash
jps
# Phải thấy: NameNode, DataNode, SecondaryNameNode
```

### Lỗi: "Permission denied" trên HDFS

```bash
# Cấp quyền cho thư mục
hdfs dfs -chmod -R 777 /credit_card_data

# Hoặc đổi owner
hdfs dfs -chown -R khtn_22120300:supergroup /credit_card_data

# Nếu lệnh hdfs dfs không hoạt động, dùng:
$HADOOP_HOME/bin/hdfs dfs -chmod -R 777 /credit_card_data
```

### Lỗi: "401 Unauthorized" khi push data

- Kiểm tra Push URL có đúng không
- Kiểm tra key trong URL có đầy đủ không
- Có thể URL đã hết hạn, cần lấy lại từ Power BI

---

## Checklist Hoàn Thành

- [ ] Đã extract file ZIP
- [ ] Đã cài đặt Python và dependencies
- [ ] Đã tạo file `.env` với cấu hình đúng
- [ ] Đã khởi động HDFS
- [ ] Đã tạo thư mục trong HDFS
- [ ] Đã test script `load_data.py`
- [ ] Đã kiểm tra dữ liệu trên Power BI
- [ ] Đã setup Airflow (nếu cần)

---

## Tóm Tắt Lệnh Nhanh

```bash
# 1. Extract
cd ~/Downloads
unzip ODAP-KeDiAnDem-main.zip
cd ODAP-KeDiAnDem-main

# 2. Tạo virtual environment và cài đặt dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Tạo file .env
nano .env
# (Paste nội dung từ Bước 4.2)

# 4. Khởi động HDFS
cd $HADOOP_HOME
start-dfs.sh

# 5. Test script (nhớ kích hoạt venv trước)
cd ~/ODAP-KeDiAnDem-main
source venv/bin/activate
python powerbi/load_data.py
```

---

Chúc bạn thành công! 🎉

