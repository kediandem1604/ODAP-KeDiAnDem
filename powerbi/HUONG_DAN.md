# Hướng Dẫn Từng Bước: Tự Động Cập Nhật Dữ Liệu lên Power BI

## ✅ Bạn Đã Hoàn Thành:
- ✅ Tạo dashboard từ CSV dataset "ODAP-Quoc"
- ✅ Thiết kế các visualization xong

## 🎯 Mục Tiêu:
Thiết lập tự động push dữ liệu từ Hadoop lên Power BI mỗi 10 phút qua Airflow.

## ⚠️ Lưu Ý Quan Trọng:
- Dashboard hiện tại của bạn được tạo từ **CSV dataset** → Không tự động cập nhật
- Cần tạo **Push Dataset** để tự động cập nhật
- Có 2 cách:
  - **Cách 1:** Tạo dashboard mới từ Push Dataset (khuyến nghị)
  - **Cách 2:** Giữ dashboard CSV để test, Push Dataset để production

---

## Bước 1: Tạo Push Dataset Mới (Vì Dashboard CSV Không Tự Động Cập Nhật)

⚠️ **Quan Trọng:** Dataset CSV "ODAP-Quoc" của bạn **KHÔNG có Push URL** và **KHÔNG tự động cập nhật**. Cần tạo Push Dataset mới.

### Option A: Tạo Push Dataset Mới (Khuyến nghị)

**Cách Nhanh: Dùng Power BI Desktop**

1. **Tải Power BI Desktop:** https://powerbi.microsoft.com/desktop/
2. **Mở Power BI Desktop** → Click **"Get Data"** → **"Blank Query"**
3. **Vào "Advanced Editor"**, paste code:

```m
let
    Source = #table(
        type table[
            Credit_Card = text,
            Transaction_Date = text,
            Transaction_Time = text,
            Merchant_Name = text,
            Merchant_City = text,
            Amount_VND = number,
            event_time = text
        ],
        {}
    )
in
    Source
```

4. **Click "Done"** → **"Close & Apply"**
5. **Publish:**
   - Click nút **"Publish"** (góc trên bên phải)
   - Chọn workspace "ODAP Credit Card Analytics"
   - Click **"Select"**
   - Đặt tên dataset: **"ODAP-Quoc-Push"** (hoặc tên khác)

6. **Lấy Push URL:**
   - Vào Power BI Web → Dataset vừa publish
   - Settings → API Information → Copy Push URL

### Option B: Dùng Link Trực Tiếp

1. **Truy cập:** https://app.powerbi.com/home?experience=power-bi#/create/streaming-dataset
2. **Tạo dataset:**
   - Chọn **"API"**
   - Đặt tên: "ODAP-Quoc-Push"
   - Thêm các trường (xem schema bên dưới)
   - Click **"Create"**
3. **Copy Push URL** ngay khi tạo xong

---

## Bước 1.5: Tạo Dashboard Mới từ Push Dataset (Hoặc Giữ Dashboard CSV)

### Cách 1: Tạo Dashboard Mới từ Push Dataset (Khuyến nghị)

1. **Vào Power BI Web** → Workspace
2. **Click "+ New item"** → **"Report"** hoặc **"Real-time dashboard"**
3. **Chọn Push Dataset** vừa tạo ("ODAP-Quoc-Push")
4. **Copy các visualization từ dashboard CSV:**
   - Mở dashboard CSV cũ
   - Xem các visualizations đã thiết kế
   - Tạo lại các visualizations tương tự trong dashboard mới
   - Hoặc export/import nếu có thể

5. **Pin các visualizations lên dashboard mới**

### Cách 2: Giữ Cả 2 Dashboard

- **Dashboard CSV:** Giữ để test và so sánh
- **Dashboard Push Dataset:** Dùng cho production, tự động cập nhật

---

## Bước 2: Lấy Push URL từ Push Dataset Mới

### 2.1. Vào Dataset Push Dataset trên Power BI Web

1. **Mở Power BI Web:** https://app.powerbi.com
2. **Vào workspace** "ODAP Credit Card Analytics"
3. **Tìm Push Dataset** vừa tạo ("ODAP-Quoc-Push")
4. **Click vào dataset** để mở

### 2.2. Vào Settings

**Cách 1: Từ Menu Dataset**
- Click vào **"..."** (More options) ở góc trên bên phải
- Chọn **"Settings"**

**Cách 2: Từ Icon Settings**
- Click biểu tượng **⚙️ (Settings)** ở góc trên bên phải màn hình
- Chọn **"Settings"** → **"Datasets"**
- Tìm và click vào Push Dataset

### 2.3. Tìm và Copy Push URL

1. Trong trang Settings, tìm **"API Information"** hoặc **"Dataset settings"**
2. Scroll xuống tìm **"Push URL"** hoặc **"REST API URL"**
3. **Copy toàn bộ URL** (có dạng: `https://api.powerbi.com/beta/.../rows?experience=power-bi&key=...`)

✅ **Push Dataset sẽ có Push URL**, khác với CSV dataset thông thường.

**Cách Nhanh: Dùng Power BI Desktop**

1. **Tải Power BI Desktop:** https://powerbi.microsoft.com/desktop/
2. **Mở Power BI Desktop** → Click **"Get Data"** → **"Blank Query"**
3. **Vào "Advanced Editor"**, paste code:

```m
let
    Source = #table(
        type table[
            Credit_Card = text,
            Transaction_Date = text,
            Transaction_Time = text,
            Merchant_Name = text,
            Merchant_City = text,
            Amount_VND = number,
            event_time = text
        ],
        {}
    )
in
    Source
```

4. **Click "Done"** → **"Close & Apply"**
5. **Publish:**
   - Click nút **"Publish"** (góc trên bên phải)
   - Chọn workspace "ODAP Credit Card Analytics"
   - Click **"Select"**
6. **Lấy Push URL:**
   - Vào Power BI Web → Dataset vừa publish
   - Settings → API Information → Copy Push URL

---

## Bước 3: Cấu Hình Push URL vào File .env

### 3.1. Mở File .env

1. **Mở file `.env`** trong thư mục `ODAP-22-TamBeo`
2. Nếu chưa có file `.env`, tạo file mới

### 3.2. Thêm Push URL

Thêm hoặc cập nhật dòng sau (thay URL bằng URL bạn vừa copy):

```bash
# Power BI Configuration
POWERBI_PUSH_URL=https://api.powerbi.com/beta/YOUR_WORKSPACE_ID/datasets/YOUR_DATASET_ID/rows?experience=power-bi&key=YOUR_KEY

# HDFS Configuration (nếu chưa có)
HDFS_WEB_HOST=localhost:9870
HDFS_USER=panda
HDFS_COMPACTED_PATH=/credit_card_data/compacted

# File lưu timestamp push cuối
LAST_PUSH_FILE=/tmp/last_push_time.txt
```

### 3.3. Lưu File

- **Lưu file `.env`**
- Đảm bảo URL đúng và đầy đủ (không bỏ sót ký tự nào)

---

## Bước 4: Test Script Load Data

Trước khi chạy Airflow, test script để đảm bảo hoạt động:

### 4.1. Kiểm Tra Dữ Liệu trong HDFS

```bash
cd ODAP-22-TamBeo

# Kiểm tra có dữ liệu trong HDFS không
hdfs dfs -ls /credit_card_data/final
```

### 4.2. Gộp CSV (Nếu Cần)

Nếu chưa có file compacted:

```bash
spark-submit hadoop/compact_csv.py
```

### 4.3. Test Script Load Data

```bash
python3 powerbi/load_data.py
```

### 4.4. Kiểm Tra Kết Quả

**Kiểm tra trên Terminal:**
- Script chạy thành công không?
- Có lỗi gì không?
- Số dòng đã push là bao nhiêu?

**Kiểm tra trên Power BI:**
- Vào **Push Dataset** vừa tạo ("ODAP-Quoc-Push")
- Xem tab **"Data"** → Có dữ liệu chưa?
- Dashboard từ Push Dataset có cập nhật không?

---

## Bước 5: Thiết Lập Airflow

### 5.1. Khởi Động Airflow Scheduler

**Mở Terminal 1:**

```bash
cd ODAP-22-TamBeo
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

Để terminal này chạy (không đóng).

### 5.2. Khởi Động Airflow Webserver

**Mở Terminal 2 (Terminal mới):**

```bash
cd ODAP-22-TamBeo
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

Để terminal này chạy (không đóng).

### 5.3. Truy Cập Airflow Web UI

1. **Mở trình duyệt:** http://localhost:8080
2. **Đăng nhập** (nếu cần):
   - Username: `admin`
   - Password: `admin` (hoặc password bạn đã set)

### 5.4. Kích Hoạt DAG

1. **Tìm DAG `send_data_to_powerbi`** trong danh sách
2. **Toggle ON** (nút bật/tắt ở bên trái tên DAG) để kích hoạt
3. **DAG sẽ tự động chạy:**
   - Lần đầu: Chạy ngay
   - Sau đó: Tự động chạy mỗi 10 phút

### 5.5. Kiểm Tra DAG Chạy

1. **Click vào DAG `send_data_to_powerbi`**
2. **Xem Graph View** để theo dõi các task
3. **Xem Logs** nếu có lỗi:
   - Click vào task → **"Log"** để xem chi tiết

---

## Bước 6: Kiểm Tra và Monitor

### 6.1. Kiểm Tra Airflow

- **Vào http://localhost:8080**
- Xem DAG có chạy đều không (mỗi 10 phút)
- Xem log nếu có lỗi

### 6.2. Kiểm Tra Power BI

- **Vào dataset** trên Power BI Web
- **Xem tab "Data"** → Dữ liệu có cập nhật không
- **Vào dashboard** → Visualizations có cập nhật không

### 6.3. Kiểm Tra HDFS

- Đảm bảo có dữ liệu mới trong `/credit_card_data/final`
- Script `compact_csv.py` có chạy thành công không

---

## Troubleshooting

### Lỗi: "Vui lòng cấu hình POWERBI_PUSH_URL"
- **Giải pháp:** Kiểm tra file `.env` đã có `POWERBI_PUSH_URL` chưa
- Đảm bảo URL đúng và đầy đủ

### Lỗi: "Không tìm thấy file CSV compacted"
- **Giải pháp:** Chạy `spark-submit hadoop/compact_csv.py` trước
- Kiểm tra có dữ liệu trong `/credit_card_data/final` không

### Lỗi: "401 Unauthorized" khi push data
- **Giải pháp:** Kiểm tra Push URL có đúng không
- Có thể URL đã hết hạn, cần lấy lại từ Power BI

### DAG không chạy:
- **Giải pháp:** 
  - Kiểm tra Airflow scheduler có chạy không (Terminal 1)
  - Kiểm tra DAG có được toggle ON chưa
  - Xem log để tìm lỗi cụ thể

### Dữ liệu không hiển thị trên Dashboard:
- **Giải pháp:**
  - Refresh dashboard
  - Kiểm tra dataset có nhận được dữ liệu không (tab Data)
  - Kiểm tra các filter trên visual có đang ẩn dữ liệu không

---

## Checklist Cuối Cùng

- [ ] Đã tạo Push Dataset mới ("ODAP-Quoc-Push")
- [ ] Đã lấy Push URL từ Push Dataset
- [ ] Đã thêm Push URL vào file `.env`
- [ ] Đã tạo dashboard mới từ Push Dataset (hoặc giữ dashboard CSV để test)
- [ ] Đã test script `load_data.py` thành công
- [ ] Đã khởi động Airflow (scheduler + webserver)
- [ ] Đã kích hoạt DAG `send_data_to_powerbi`
- [ ] Đã kiểm tra DAG chạy đều mỗi 10 phút
- [ ] Đã kiểm tra dữ liệu cập nhật trên Push Dataset
- [ ] Dashboard từ Push Dataset tự động cập nhật khi có dữ liệu mới

---

## Kết Quả Mong Đợi

✅ Dữ liệu tự động được push từ Hadoop lên Power BI mỗi 10 phút  
✅ Dashboard tự động cập nhật khi có dữ liệu mới  
✅ Không cần upload CSV thủ công  
✅ Hệ thống hoạt động tự động hoàn toàn  

Chúc bạn thành công! 🎉

