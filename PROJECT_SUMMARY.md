# PROJECT SUMMARY: YHCT RAG Lakehouse System

Hệ thống **Traditional Medicine RAG Lakehouse** là một giải pháp hoàn chỉnh tích hợp giữa **Data Engineering** (kiến trúc Lakehouse để quản lý dữ liệu lớn) và **Artificial Intelligence** (RAG Chatbot hỗ trợ tra cứu kiến thức Y Học Cổ Truyền).

---

## 1. Kiến Trúc Tổng Quan (System Architecture)

Dự án được phân tách rõ ràng thành các tầng nghiệp vụ từ lưu trữ, xử lý dữ liệu cho đến hiển thị và AI:

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

## 2. Bản đồ Thư Mục & Vai Trò Các Tệp Tin

*   [docker-compose.yml](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/docker-compose.yml): Cấu hình khởi động toàn bộ môi trường Docker (MinIO, Spark, MongoDB, PostgreSQL, Superset, MLflow, ChromaDB, Backend, Frontend, Streamlit, AI Service).
*   [HUONG_DAN_SU_DUNG.md](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/HUONG_DAN_SU_DUNG.md): Hướng dẫn chi tiết cách deploy, chạy pipeline và xử lý sự cố.
*   [etl_pipeline/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline): Thư mục chứa code Dagster ETL Pipeline:
    *   [etl_pipeline/etl_pipeline/assets/bronze.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/bronze.py): Trích xuất văn bản thô từ PDF.
    *   [etl_pipeline/etl_pipeline/assets/silver.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/silver.py): Lọc trang YHCT bằng Apache Spark.
    *   [etl_pipeline/etl_pipeline/assets/gold_chunks.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_chunks.py): Cắt nhỏ văn bản (chunking).
    *   [etl_pipeline/etl_pipeline/assets/gold_embeddings.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_embeddings.py): Sinh vector embeddings và đẩy vào ChromaDB.
    *   [etl_pipeline/etl_pipeline/assets/gold_herb_mentions.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_herb_mentions.py) & [gold_tang_phu_mentions.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_tang_phu_mentions.py): Khai phá dữ liệu dược liệu và tạng phủ.
    *   [etl_pipeline/etl_pipeline/assets/gold_mongodb_sync.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_mongodb_sync.py): Đồng bộ dữ liệu người dùng từ MongoDB sang Lakehouse phục vụ phân tích.
    *   [etl_pipeline/etl_pipeline/checks.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/checks.py): 8 bài kiểm tra chất lượng dữ liệu (Data Quality Gates).
    *   [etl_pipeline/etl_pipeline/sensors.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/sensors.py): Định nghĩa sensor tự động hóa phát hiện PDF và lịch biểu chạy.
*   [ai_service/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service): FastAPI service phục vụ logic RAG Chatbot:
    *   [ai_service/main.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py): Điểm cuối API Chat, phân loại hội thoại, tìm kiếm Vector DB và sinh câu trả lời LLM.
*   [backend/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/backend): Express backend phục vụ xác thực người dùng, lưu trữ hội thoại vào MongoDB & Redis cache.
*   [frontend/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/frontend): React App (Vite) cung cấp giao diện Web Portal thân thiện cho người bệnh.
*   [streamlit_app/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app): Streamlit App cung cấp giao diện quản trị viên (Admin), Upload sách PDF mới và Dashboard theo dõi chất lượng.
*   [evaluation/](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/evaluation): Khung đánh giá chất lượng câu trả lời RAG (Faithfulness, Relevance...) tích hợp với MLflow.

---

## 3. Quy Trình Ingestion và Xử Lý Dữ Liệu Lớn (ETL Lakehouse)

Pipeline dữ liệu được viết bằng Dagster và lưu trữ dạng Parquet trên MinIO S3 qua 3 tầng (Medallion Architecture):

### Tầng Bronze
*   **Asset:** [bronze_pdf_pages](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/bronze.py#L52-L174)
*   **Chức năng:** Quét thư mục `data/raw/` để lấy các tệp PDF. Dùng thư viện `PyMuPDF` trích xuất thông tin từng trang (`doc_id`, `page_num`, `page_text`, `word_count`, `source_file`).
*   **Incremental Loading:** So sánh danh sách file hiện có trong MinIO với thư mục local. Chỉ trích xuất file mới, sau đó ghép (`concat`) với dữ liệu cũ và ghi đè file Parquet tổng hợp trên MinIO, giúp tiết kiệm tối đa tài nguyên tính toán.

### Tầng Silver
*   **Asset:** [silver_filtered_pages](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/silver.py#L242-L300)
*   **Chức năng:** Lọc loại bỏ các trang rác hoặc không thuộc chủ đề YHCT.
*   **Engine tính toán:** Sử dụng **Apache Spark** chạy song song trên các worker. Hàm lọc UDF sẽ kiểm tra sự hiện diện của danh sách keywords YHCT và các stopwords (mục lục, tài liệu tham khảo, lời nói đầu...). Nếu Spark không khả dụng, hệ thống tự động fallback sử dụng **Polars** cục bộ.

### Tầng Gold
*   **Asset:** [gold_yhct_chunks](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_chunks.py#L39-L79)
    *   Cắt nhỏ dữ liệu trang từ tầng Silver bằng thuật toán Sliding Window với kích thước `CHUNK_SIZE = 250` từ và `CHUNK_OVERLAP = 50` từ.
*   **Asset:** [gold_embeddings](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_embeddings.py#L31-L115)
    *   Mã hóa các chunk văn bản thành vector biểu diễn ngữ nghĩa bằng mô hình `keepitreal/vietnamese-sbert`.
    *   Kết nối và đẩy dữ liệu vector trực tiếp vào **ChromaDB HttpClient** (bỏ qua những chunk đã tồn tại để tối ưu).
*   **Asset Khai thác Dữ liệu:**
    *   [gold_herb_mentions](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_herb_mentions.py): Sử dụng LLM Groq để phát hiện các tên dược liệu xuất hiện trong sách theo phương pháp Two-pass, sau đó dùng Regex đếm tần suất xuất hiện động từ sách.
    *   [gold_tang_phu_mentions](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_tang_phu_mentions.py): Lọc các thuật ngữ liên quan đến 5 cặp tạng phủ YHCT (tỳ vị, can đởm, thận, phế đại tràng, tâm tiểu tràng).
    *   [gold_mongodb_sync](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/gold_mongodb_sync.py): Đồng bộ người dùng, phiên làm việc và hoạt động của người dùng từ MongoDB sang Lakehouse phục vụ cho phân tích hành vi.

---

## 4. Cơ Chế AI RAG và API Chatbot

API xử lý chatbot chính nằm ở [api_chat](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py#L211-L297):

1.  **Phân Loại Tin Nhắn (Message Classification):**
    *   LLM Groq (`llama-3.1-8b-instant`) đóng vai trò phân loại tin nhắn bằng hàm [_needs_rag](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py#L153-L172). Nếu là tin nhắn chào hỏi xã giao hoặc thông thường, hệ thống trả lời trực tiếp mà không cần tốn tài nguyên truy vấn cơ sở dữ liệu Vector (ChromaDB).
2.  **Tìm Kiếm Ngữ Nghĩa (Vector Search):**
    *   Khi cần hỗ trợ y khoa, câu hỏi của người dùng được embed thành vector và so khớp cosine trong ChromaDB với threshold tối thiểu `MIN_SIM = 0.40`. Lấy ra 5 chunks tài liệu phù hợp nhất làm ngữ cảnh.
3.  **Tích hợp Lịch Sử & Sinh Câu Trả Lời:**
    *   Hệ thống lưu giữ lịch sử tối đa 10 tin nhắn trước đó. Ngữ cảnh tài liệu cùng với lịch sử và thông tin cá nhân của người bệnh (nếu có) được định dạng theo cấu trúc chặt chẽ và gửi đến Groq LLM kèm theo [system_prompt](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py#L101-L122) quy định giọng điệu và định dạng đầu ra (Tổng quan, Nguyên nhân YHCT, Lối sống & Ăn uống, Bài thuốc tham khảo, Lưu ý).
4.  **Trích xuất Thực thể (Medical Entity Extraction):**
    *   Hàm [extract_entities](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py#L60-L74) thực hiện quét nhanh cả câu hỏi và câu trả lời để phát hiện các triệu chứng, bệnh danh, tạng phủ và vị dược liệu được nhắc tới, trả về client phục vụ việc hiển thị UI sinh động.

---

## 5. Tự Động Hóa & Kiểm Soát Chất Lượng (Quality Gates & Automation)

*   **Giám sát chất lượng dữ liệu (Asset Checks):** Tích hợp 8 bài kiểm tra chất lượng tự động ([checks.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/checks.py)) chạy ngay khi kết thúc mỗi tầng dữ liệu (như phát hiện văn bản lỗi, trùng trang ở tầng Bronze; kiểm tra tỷ lệ lọc hợp lý, phát hiện text rỗng ở tầng Silver; kiểm tra độ dài chunk từ 50-400 từ, phát hiện trùng chunk_id, và độ bao phủ tài liệu ở tầng Gold).
*   **Sensor Tự động hóa:** [new_pdf_sensor](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/sensors.py#L40-L85) quét thư mục raw 30 giây một lần. Khi phát hiện tệp sách PDF mới, sensor cập nhật con trỏ trạng thái (cursor) và tự động kích hoạt một lượt chạy pipeline mới nhằm cập nhật cơ sở tri thức cho chatbot gần như ngay lập tức.
*   **Schedule Định kỳ:** Pipeline cũng được lên lịch [daily_pipeline_schedule](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/sensors.py#L92-L101) tự động khởi chạy lúc 2:00 AM hàng ngày để đồng bộ hóa hoàn toàn và thu thập chỉ số đánh giá.
