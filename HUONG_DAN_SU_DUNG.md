# Hướng Dẫn Sử Dụng Hệ Thống YHCT RAG Lakehouse

> Hệ thống chatbot Y Học Cổ Truyền kết hợp Data Engineering (Dagster, MinIO, Spark) và AI (RAG + Groq LLM).

---

## Mục Lục

1. [Yêu cầu](#1-yêu-cầu)
2. [Khởi động hệ thống](#2-khởi-động-hệ-thống)
3. [Các giao diện web](#3-các-giao-diện-web)
4. [Luồng dữ liệu](#4-luồng-dữ-liệu)
5. [Các tính năng DE nổi bật](#5-các-tính-năng-de-nổi-bật)
6. [Thêm sách PDF mới](#6-thêm-sách-pdf-mới)
7. [Chạy pipeline thủ công](#7-chạy-pipeline-thủ-công)
8. [Dừng hệ thống](#8-dừng-hệ-thống)
9. [Xử lý sự cố thường gặp](#9-xử-lý-sự-cố-thường-gặp)

---

## 1. Yêu Cầu

- **Docker Desktop** (bật trước khi chạy)
- **RAM tối thiểu**: 8 GB
- **Disk**: ~5 GB trống
- File `.env` đặt ở thư mục gốc (hỏi người tạo project để lấy)

---

## 2. Khởi Động Hệ Thống

```bash
# Clone repo về máy
git clone <repo_url>
cd traditional-medicine-rag-lakehouse

# Copy file cấu hình môi trường (hỏi nhóm để lấy file .env)
cp .env.example .env   # rồi điền các giá trị cần thiết

# Khởi động toàn bộ (lần đầu build image ~10-15 phút)
docker compose up -d

# Xem trạng thái các container
docker ps
```

Đợi đến khi tất cả container hiện `Up` hoặc `healthy` là xong.

---

## 3. Các Giao Diện Web

| Giao diện | URL | Tài khoản |
|-----------|-----|-----------|
| **Chatbot YHCT** (Streamlit) | http://localhost:8501 | — |
| **Web App** (React) | http://localhost:3000 | Đăng ký tài khoản mới |
| **Dagster** (Pipeline UI) | http://localhost:3001 | — |
| **MinIO** (Kho lưu trữ) | http://localhost:9001 | `minio` / `minio123` |
| **MLflow** (Experiment tracking) | http://localhost:5002 | — |
| **Superset** (Dashboard) | http://localhost:8088 | `admin` / `admin123` |
| **Spark Master** (Cluster UI) | http://localhost:8080 | — |

---

## 4. Luồng Dữ Liệu

```
PDF Sách YHCT
    │
    ▼
[Bronze Layer] ──── Trích xuất text từng trang, lưu Parquet vào MinIO
    │                (yhct-bronze bucket)
    ▼
[Silver Layer] ──── Lọc trang liên quan YHCT bằng Apache Spark
    │                (yhct-silver bucket)
    ▼
[Gold Layer]   ──── Chunking → Embedding → ChromaDB (vector search)
    │                (yhct-gold bucket)
    ▼
[Chatbot]      ──── Nhận câu hỏi → RAG retrieval → Groq LLM → Trả lời
```

Pipeline được tự động kích hoạt bởi:
- **Sensor** (`new_pdf_sensor`): phát hiện PDF mới trong `data/raw/` mỗi 30 giây
- **Schedule** (`daily_pipeline_2am`): chạy tự động lúc 2 giờ sáng mỗi ngày

---

## 5. Các Tính Năng DE Nổi Bật

Đây là các nghiệp vụ Data Engineering được implement trong project, giải thích tại sao cần và hoạt động như thế nào.

---

### Incremental Loading (Bronze Layer)

**Tác dụng**: Khi pipeline chạy lại (vì thêm sách mới, hoặc schedule hàng ngày), hệ thống **chỉ xử lý file PDF chưa từng được ingest**. File cũ đã có trong MinIO sẽ bị bỏ qua hoàn toàn.

**Tại sao cần**: Không có incremental loading thì mỗi lần chạy pipeline phải đọc lại toàn bộ sách từ đầu — nếu có 20 cuốn sách mà chỉ thêm 1 cuốn mới, vẫn phải parse lại 20 cuốn, rất lãng phí.

**Cách hoạt động**:
1. Bronze layer đọc Parquet hiện có từ MinIO, lấy danh sách `source_file` đã có
2. So sánh với danh sách PDF trong `data/raw/`
3. Chỉ parse các file chưa có trong danh sách
4. Ghép (concat) dữ liệu mới vào dữ liệu cũ, ghi lại vào MinIO

**Thấy ở đâu**: Dagster UI → Assets → `bronze_pdf_pages` → tab Metadata sau khi chạy, xem trường `status` và `skipped_files`.

---

### Asset Checks (Data Quality Gate)

**Tác dụng**: Sau mỗi lần pipeline chạy, hệ thống tự động kiểm tra chất lượng dữ liệu tại từng layer. Nếu dữ liệu có vấn đề (PDF bị corrupt, lọc quá mạnh, chunk bị trùng...) sẽ hiện cảnh báo hoặc lỗi ngay trong Dagster UI.

**Tại sao cần**: Không có checks thì pipeline chạy xong nhưng không biết dữ liệu có đúng không — chatbot có thể trả lời sai mà không ai biết tại sao.

**8 checks được tự động chạy**:

| Layer | Check | Ngưỡng | Mức độ |
|-------|-------|--------|--------|
| Bronze | Có dữ liệu sau ingest | > 0 trang | ERROR |
| Bronze | Text không bị corrupt | Trung bình ≥ 20 từ/trang | WARN |
| Bronze | Không có trang trùng lặp | 0 duplicates | ERROR |
| Silver | Tỷ lệ lọc hợp lý | Giữ lại 5%–95% | WARN |
| Silver | Không có text rỗng | 0 trang rỗng | ERROR |
| Gold | Chunk size hợp lý | Trung bình 50–400 từ | WARN |
| Gold | Không có chunk trùng | 0 duplicates | ERROR |
| Gold | Mỗi sách đủ chunks | ≥ 10 chunks/sách | WARN |

- **ERROR**: dữ liệu sai nghiêm trọng, cần xem xét ngay
- **WARN**: bất thường nhưng pipeline vẫn tiếp tục, cần theo dõi

**Thấy ở đâu**: Dagster UI → Assets → chọn asset → tab **Checks**.

---

### Apache Spark (Silver Layer)

**Tác dụng**: Bước lọc trang YHCT tại Silver layer được xử lý bởi Apache Spark thay vì Python đơn thuần. Spark phân tán công việc ra nhiều worker để xử lý song song.

**Tại sao cần**: Nếu hệ thống có hàng chục nghìn trang (scale lớn hơn), Python vòng lặp tuần tự sẽ rất chậm. Spark chia data thành các partition và chạy song song trên nhiều machine — thời gian xử lý scale theo số worker, không phải theo kích thước data.

**Cách hoạt động**:
1. Bronze DataFrame (Polars) → chuyển sang Spark DataFrame
2. Spark UDF (User Defined Function) phân loại từng trang: giữ lại nếu chứa keyword YHCT, loại bỏ nếu quá ngắn hoặc là mục lục/tài liệu tham khảo
3. UDF chạy **distributed** trên Spark Worker (`spark://spark-master:7077`)
4. Kết quả ghép lại → chuyển về Polars → ghi vào MinIO

Nếu Spark không khả dụng, tự động fallback về Polars (không mất tính năng, chỉ chậm hơn).

**Thấy ở đâu**:
- Spark Master UI: http://localhost:8080 → xem jobs đang chạy khi pipeline chạy tới Silver
- Dagster UI → Assets → `silver_filtered_pages` → tab Metadata → trường `engine` hiện `spark (spark://spark-master:7077)` hoặc `polars (fallback)`

---

### Schedule (Tự Động Hóa)

**Tác dụng**: Pipeline tự chạy lúc **2 giờ sáng mỗi ngày**, không cần ai bấm nút.

**Tại sao cần**: Dữ liệu người dùng (session, events từ MongoDB) cập nhật liên tục. Schedule đảm bảo các asset gold (phân tích dược liệu, tạng phủ, evaluation) luôn phản ánh dữ liệu mới nhất. Kết hợp với Incremental Loading, chỉ xử lý thứ gì thực sự thay đổi.

**Thấy ở đâu**: Dagster UI → **Automation** → `daily_pipeline_2am` → bật toggle để kích hoạt.

---

### Event-Driven Sensor

**Tác dụng**: Thay vì chờ đến 2 giờ sáng, khi có PDF mới được upload vào `data/raw/`, sensor **tự động phát hiện và trigger pipeline ngay trong vòng 30 giây**.

**Tại sao cần**: Khi admin upload sách mới, muốn chatbot có thể trả lời về cuốn sách đó ngay lập tức, không phải chờ đến sáng hôm sau.

**Cách hoạt động**: Sensor dùng **cursor** (trạng thái lưu trong PostgreSQL) để nhớ danh sách file đã xử lý. Mỗi 30 giây so sánh danh sách file hiện tại với cursor — nếu có file mới thì trigger một pipeline run với `run_key` duy nhất (đảm bảo không trigger trùng lặp dù sensor chạy nhiều lần).

**Thấy ở đâu**: Dagster UI → **Automation** → `new_pdf_sensor` → phải bật RUNNING sau mỗi lần restart hệ thống.

---

## 6. Thêm Sách PDF Mới

### Cách 1: Qua giao diện Streamlit (dễ nhất)

1. Mở http://localhost:8501
2. Chọn tab **Analytics**
3. Kéo thả file PDF vào ô upload → nhấn **Lưu vào hệ thống**
4. Sensor sẽ **tự động** phát hiện và chạy pipeline trong vòng 30 giây

### Cách 2: Copy file trực tiếp

```bash
# Copy PDF vào thư mục raw
cp /path/to/your/sach.pdf data/raw/

# Sensor sẽ tự phát hiện sau ~30 giây
# Xem log để theo dõi:
docker logs etl_pipeline -f
```

> **Lưu ý Incremental Loading**: Pipeline chỉ xử lý file PDF **mới**. File cũ đã có trong hệ thống sẽ **không** bị xử lý lại, tiết kiệm thời gian.

---

## 7. Chạy Pipeline Thủ Công

### Qua Dagster UI

1. Mở http://localhost:3001
2. Vào **Assets** → chọn assets muốn chạy → **Materialize**

Hoặc chạy toàn bộ pipeline:
1. Vào **Jobs** → `all_assets_job` → **Launch Run**

### Kích hoạt/Tắt Schedule & Sensor

1. Vào http://localhost:3001 → **Automation**
2. Bấm toggle để bật/tắt `new_pdf_sensor` hoặc `daily_pipeline_2am`

> **Lưu ý**: Sau mỗi lần khởi động lại hệ thống, sensor cần được **bật lại thủ công** trong Dagster UI.

### Xem Asset Checks (Data Quality)

Sau khi pipeline chạy xong:
1. Vào **Assets** → chọn một asset bất kỳ (bronze, silver, gold)
2. Tab **Checks** → xem kết quả validate dữ liệu

Có 8 checks tự động:
- Bronze: kiểm tra có dữ liệu, text không bị corrupt, không có page trùng lặp
- Silver: tỷ lệ lọc hợp lý (5%–95%), không có text rỗng
- Gold: chunk size trong khoảng 50–400 từ, không có chunk_id trùng, mỗi sách ≥ 10 chunks

### Xem Spark Processing

Khi pipeline chạy tới Silver layer:
- Mở http://localhost:8080 (Spark Master UI)
- Xem jobs đang chạy và worker đang sử dụng

---

## 8. Dừng Hệ Thống

```bash
# Dừng tất cả (giữ nguyên dữ liệu)
docker compose down

# Dừng và XÓA dữ liệu (cẩn thận!)
docker compose down -v
```

---

## 9. Xử Lý Sự Cố Thường Gặp

### Container không start được

```bash
# Xem log của container bị lỗi
docker logs <tên_container>

# Ví dụ
docker logs etl_pipeline
docker logs dagster
```

### Chatbot không trả lời / lỗi ChromaDB

```bash
# Kiểm tra ChromaDB
curl http://localhost:8000/api/v1/heartbeat

# Restart Streamlit
docker compose restart streamlit
```

### Pipeline không chạy sau khi thêm PDF

```bash
# Kiểm tra sensor có đang RUNNING không
# Vào http://localhost:3001 → Automation

# Xem log sensor
docker logs etl_pipeline | grep -i "sensor\|pdf"
```

### Dagster không load được code

```bash
# Rebuild và restart etl_pipeline
docker compose up --build -d etl_pipeline

# Sau đó bật lại sensor trong UI
```

### Superset không thấy data mới

```bash
# Reinit DuckDB views từ MinIO
docker compose exec superset python /app/pythonpath/init_duckdb.py
```

---

## Kiến Trúc Kỹ Thuật (Tóm Tắt)

```
┌─────────────────────────────────────────────────────┐
│                   USER INTERFACES                    │
│  Streamlit :8501  │  React :3000  │  Superset :8088  │
└────────┬──────────┴───────┬───────┴──────────────────┘
         │                  │
    RAG Pipeline       Web Backend
    (Groq LLM)        FastAPI :5000
         │                  │
┌────────▼──────────────────▼──────────────────────────┐
│                    DATA LAYER                         │
│  MinIO :9000   │  ChromaDB :8000  │  MongoDB :27017   │
│  (Parquet S3)  │  (Vectors)       │  (Users/Sessions) │
└────────▲───────┴──────────────────┴──────────────────┘
         │
┌────────▼─────────────────────────────────────────────┐
│               ETL PIPELINE (Dagster :3001)            │
│  Bronze (PyMuPDF)  →  Silver (Spark)  →  Gold (Embed) │
│  Incremental Load     Distributed        Sentence-BERT │
│  Asset Checks         Filtering          + ChromaDB    │
└──────────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────┐
│           INFRASTRUCTURE                              │
│  Spark Master :8080  │  MLflow :5002  │  PostgreSQL   │
│  Spark Worker        │  (Experiments) │  (Dagster DB) │
└──────────────────────────────────────────────────────┘
```

---

## Liên Hệ

Nếu gặp vấn đề không giải quyết được, liên hệ người setup project để được hỗ trợ.
