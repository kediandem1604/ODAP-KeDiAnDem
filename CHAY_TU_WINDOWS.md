# Hướng Dẫn Chạy từ Windows - Kết Nối với Hadoop trên VMware

## ⚠️ Quan Trọng: Bạn Có 2 Lựa Chọn

### Option 1: Chạy Script từ Windows, Kết Nối Hadoop trên VMware (Khuyến nghị)
- ✅ Giữ code trên Windows (dễ chỉnh sửa)
- ✅ Chạy Hadoop trên VMware
- ✅ Script Python chạy từ Windows, kết nối Hadoop qua network

### Option 2: Copy Code vào VMware, Chạy Tất Cả trên VMware
- ✅ Tất cả chạy trên cùng máy
- ❌ Phải copy code mỗi lần chỉnh sửa

---

## Option 1: Chạy từ Windows - Kết Nối Hadoop trên VMware (Khuyến nghị)

### Bước 1: Cấu Hình Network

#### 1.1. Lấy IP của VMware

**Trong VMware (Linux):**
```bash
# Kiểm tra IP của máy ảo
ifconfig
# hoặc
ip addr show

# Ghi lại IP (ví dụ: 192.168.1.100)
```

#### 1.2. Cấu Hình HDFS để Cho Phép Kết Nối Từ Bên Ngoài

**Trong VMware, chỉnh sửa file cấu hình Hadoop:**

```bash
# Chỉnh sửa core-site.xml
sudo nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Thêm hoặc sửa:
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://0.0.0.0:9000</value>
    </property>
</configuration>
```

**Chỉnh sửa hdfs-site.xml:**
```bash
sudo nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Đảm bảo có:
```xml
<configuration>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:9864</value>
    </property>
</configuration>
```

**Restart HDFS:**
```bash
stop-dfs.sh
start-dfs.sh
```

#### 1.3. Kiểm Tra Firewall trên VMware

```bash
# Kiểm tra firewall
sudo ufw status

# Nếu firewall đang bật, mở port
sudo ufw allow 9000/tcp
sudo ufw allow 9870/tcp
sudo ufw allow 9864/tcp
```

### Bước 2: Cấu Hình File .env trên Windows

**Mở file `.env` trong thư mục `ODAP-22-TamBeo` trên Windows:**

```bash
# Thay localhost bằng IP của VMware
HDFS_NAMENODE_URL=hdfs://192.168.1.100:9000
HDFS_WEB_HOST=192.168.1.100:9870
HDFS_USER=panda
HDFS_COMPACTED_PATH=/credit_card_data/compacted

# Power BI Configuration
POWERBI_PUSH_URL=https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D

LAST_PUSH_FILE=C:/temp/last_push_time.txt
```

**Lưu ý:** 
- Thay `192.168.1.100` bằng IP thực tế của VMware
- Thay `C:/temp/last_push_time.txt` bằng đường dẫn phù hợp trên Windows

### Bước 3: Cài Đặt Thư Viện Python trên Windows

**Mở PowerShell hoặc CMD trên Windows:**

```bash
cd D:\ODAP\ODAP-22-TamBeo

# Cài đặt các thư viện
pip install -r requirements.txt

# Hoặc cài từng cái
pip install pandas hdfs requests python-dotenv
```

### Bước 4: Chạy Script từ Windows

**Trong PowerShell/CMD trên Windows:**

```bash
cd D:\ODAP\ODAP-22-TamBeo

# Test kết nối HDFS
python -c "from hdfs import InsecureClient; client = InsecureClient('http://192.168.1.100:9870', user='panda'); print(client.list('/'))"

# Chạy script push dữ liệu
python powerbi\load_data.py
```

### Bước 5: Chạy Spark Script (Nếu Cần)

**Lưu ý:** Spark thường cần chạy trên Linux. Có 2 cách:

**Cách A: Chạy Spark trên VMware, Code trên Windows**

1. **Copy script vào VMware:**
```bash
# Từ Windows, dùng SCP hoặc shared folder
scp hadoop/compact_csv.py user@192.168.1.100:/home/user/
```

2. **Chạy trên VMware:**
```bash
spark-submit compact_csv.py
```

**Cách B: Dùng WSL2 trên Windows**

Nếu bạn có WSL2, có thể cài Spark trên WSL2 và chạy từ đó.

---

## Option 2: Copy Code vào VMware, Chạy Tất Cả trên VMware

### Bước 1: Copy Code vào VMware

**Cách 1: Dùng Shared Folder (Nếu có)**

1. **Cấu hình shared folder trong VMware:**
   - VM → Settings → Options → Shared Folders
   - Add folder: Chọn thư mục `ODAP-22-TamBeo` trên Windows
   - Enable

2. **Trong VMware, mount shared folder:**
```bash
# Thường mount tại /mnt/hgfs/
cd /mnt/hgfs/ODAP-22-TamBeo
```

**Cách 2: Dùng SCP (Từ Windows)**

```bash
# Trong PowerShell trên Windows
scp -r D:\ODAP\ODAP-22-TamBeo user@192.168.1.100:/home/user/
```

**Cách 3: Dùng WinSCP hoặc FileZilla**

- Download WinSCP: https://winscp.net/
- Kết nối đến VMware
- Copy thư mục `ODAP-22-TamBeo` vào VMware

### Bước 2: Cấu Hình trên VMware

```bash
# Di chuyển vào thư mục
cd ~/ODAP-22-TamBeo

# Cấu hình file .env
nano .env
```

Thêm:
```bash
HDFS_NAMENODE_URL=hdfs://localhost:9000
HDFS_WEB_HOST=localhost:9870
HDFS_USER=panda
HDFS_COMPACTED_PATH=/credit_card_data/compacted

POWERBI_PUSH_URL=https://api.powerbi.com/beta/40127cd4-45f3-49a3-b05d-315a43a9f033/datasets/d0d299e5-b210-4966-a1bf-38052d6ca14a/rows?experience=power-bi&clientSideAuth=0&key=cQ%2B0ZbVDMLYHRUmWaGYPkbxVf0ZNFTVno3lWOR0DA1yBVYKVShduHno7yIe6FOwLYG01Hdp7GtsI7iGPoxQyzw%3D%3D

LAST_PUSH_FILE=/tmp/last_push_time.txt
```

### Bước 3: Cài Đặt Thư Viện trên VMware

```bash
cd ~/ODAP-22-TamBeo
pip3 install -r requirements.txt
```

### Bước 4: Chạy Script trên VMware

```bash
# Gộp CSV
spark-submit hadoop/compact_csv.py

# Push lên Power BI
python3 powerbi/load_data.py
```

---

## So Sánh 2 Cách

| Tiêu chí | Option 1 (Windows) | Option 2 (VMware) |
|----------|-------------------|-------------------|
| Chỉnh sửa code | ✅ Dễ (dùng editor Windows) | ❌ Phải dùng editor Linux |
| Kết nối HDFS | ⚠️ Cần cấu hình network | ✅ Dễ (localhost) |
| Chạy Spark | ❌ Khó (cần WSL2) | ✅ Dễ |
| Chạy Python | ✅ Dễ | ✅ Dễ |
| Airflow | ⚠️ Có thể chạy trên Windows | ✅ Dễ trên Linux |

---

## Khuyến Nghị

### Nếu Chỉ Cần Push Dữ Liệu (Không Cần Spark):
→ **Option 1**: Chạy từ Windows, kết nối Hadoop trên VMware

### Nếu Cần Chạy Cả Spark và Python:
→ **Option 2**: Copy code vào VMware, chạy tất cả trên VMware

### Nếu Có WSL2:
→ Có thể dùng WSL2 để chạy Spark, code vẫn giữ trên Windows

---

## Troubleshooting

### Lỗi: "Connection refused" khi kết nối từ Windows

- Kiểm tra IP của VMware có đúng không
- Kiểm tra firewall trên VMware đã mở port chưa
- Kiểm tra HDFS đã bind đúng interface chưa (0.0.0.0)

### Lỗi: "Module hdfs not found" trên Windows

```bash
pip install hdfs
```

### Lỗi: "Cannot connect to HDFS" từ Windows

- Kiểm tra có thể ping được IP của VMware không
- Kiểm tra có thể truy cập http://IP:9870 không (Web UI)
- Kiểm tra file `.env` có cấu hình đúng IP không

---

## Tóm Tắt

**Bạn KHÔNG CẦN đưa code vào VMware nếu:**
- ✅ Chỉ cần chạy script Python push dữ liệu
- ✅ Có thể kết nối đến Hadoop trên VMware qua network
- ✅ Muốn giữ code trên Windows để dễ chỉnh sửa

**Bạn NÊN đưa code vào VMware nếu:**
- ✅ Cần chạy Spark script
- ✅ Muốn tất cả chạy trên cùng máy
- ✅ Không muốn cấu hình network phức tạp

---

Chúc bạn thành công! 🎉

