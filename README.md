# 🌿 HỆ THỐNG TRỢ LÝ AI & LAKEHOUSE DỮ LIỆU Y HỌC CỔ TRUYỀN (YHCT)

Hệ thống tích hợp công nghệ **RAG (Retrieval-Augmented Generation)** và kiến trúc hồ dữ liệu **Lakehouse (Medallion: Bronze -> Silver -> Gold)** để lưu trữ, làm sạch, khai phá thông tin tài liệu YHCT Việt Nam, phục vụ tra cứu thông tin y học cổ truyền và theo dõi, phân tích hành vi của người dùng trên dashboard thời gian thực.

---

## 🛠 1. Kiến Trúc Dịch Vụ & Bản Đồ Cổng Kết Nối (Port Mapping)

Hệ thống được thiết kế dưới dạng các microservices kết hợp thông qua **Docker Compose**. Dưới đây là bảng tổng hợp các dịch vụ và cổng truy cập trên máy vật lý (Host Machine):

| Dịch vụ / Giao diện | Địa chỉ truy cập | Công nghệ sử dụng | Mô tả chức năng |
| :--- | :--- | :--- | :--- |
| **Nginx Web Gateway** | [http://localhost](http://localhost) (Cổng 80) | Nginx | **Cổng vào chính** của người dùng. Proxy ngược tới Frontend, Backend, và MinIO PDF viewer. |
| **React Web Portal** | [http://localhost:3000](http://localhost:3000) | ReactJS, Vite | Giao diện Chatbot AI chính cho người dùng cuối và Dashboard theo dõi sức khoẻ cá nhân. |
| **Node.js API Gateway**| [http://localhost:5001](http://localhost:5001) | Express, Mongoose | Xử lý logic nghiệp vụ, xác thực người dùng (JWT), phân quyền, lưu trữ hội thoại và log sự kiện. |
| **Streamlit Operations**| [http://localhost:8501](http://localhost:8501) | Python, Streamlit | Dashboard giám sát kỹ thuật, quản lý người dùng, cài đặt tham số RAG, và xem nhật ký hoạt động. |
| **Dagster Orchestrator**| [http://localhost:3001](http://localhost:3001) | Dagster Web UI | Quản lý, điều phối và kích hoạt toàn bộ luồng xử lý dữ liệu tự động (ETL / Lakehouse). |
| **Apache Superset** | [http://localhost:8088](http://localhost:8088) | Apache Superset | Giao diện phân tích trực quan (BI) dành cho Quản trị viên theo dõi hoạt động và chỉ số hệ thống. |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | MinIO Object Storage | Giao diện quản lý file và Object Storage (lưu trữ PDF gốc, Parquet và mô hình MLflow). |
| **MLflow Server** | [http://localhost:5002](http://localhost:5002) | MLflow Tracking | Quản lý vòng đời mô hình AI, theo dõi tham số, log metrics và ghi chép nhật ký đánh giá (Evaluation). |
| **AI Python Service** | [http://localhost:8001](http://localhost:8001) | FastAPI, ChromaDB | Dịch vụ AI nội bộ thực hiện trích xuất dữ liệu, RAG, tạo embeddings và kết nối với Groq API. |
| **Chroma Vector DB** | Cổng nội bộ `8000` | ChromaDB | Cơ sở dữ liệu vector lưu trữ các chunks văn bản YHCT đã được nhúng vector hóa. |
| **MongoDB Database** | Cổng nội bộ `27017` | MongoDB | Cơ sở dữ liệu chính lưu thông tin người dùng, lịch sử chat và các sự kiện người dùng click chuột. |
| **PostgreSQL Database**| Cổng nội bộ `5432` | PostgreSQL (`de_psql`) | Cơ sở dữ liệu lưu trữ cấu trúc siêu dữ liệu Dagster, MLflow backend và các bảng phân tích Gold. |
| **Redis Cache** | Cổng nội bộ `6379` | Redis | Bộ nhớ đệm dùng cho kiểm soát tần suất truy cập (rate limit) và tối ưu hiệu năng API Backend. |

---

## 📋 2. Yêu Cầu Chuẩn Bị (Prerequisites)

Trước khi chạy hệ thống, hãy đảm bảo máy tính đã cài đặt các phần mềm sau:
1. **Docker Desktop** (Đã bao gồm `docker` và `docker-compose`).
2. **Git** (Dùng để clone dự án nếu cần).
3. **Groq API Key**: Bạn cần đăng ký một tài khoản trên [Groq Console](https://console.groq.com/) và tạo API Key để sử dụng mô hình ngôn ngữ lớn Llama 3 (Miễn phí tốc độ cao).

---

## 🚀 3. Hướng Dẫn Khởi Chạy Hệ Thống Chi Tiết

Vui lòng thực hiện tuần tự các bước dưới đây để chạy hệ thống trên máy của Hội đồng chấm đồ án:

### Bước 3.1: Cấu hình biến môi trường (`.env`)

Tại thư mục gốc của dự án, mở file `.env` và thiết lập các biến môi trường cần thiết:

```env
# MinIO Object Storage Configuration
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123

# PostgreSQL Configuration
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DB=dagster

# Groq API Key (QUAN TRỌNG: Thay thế bằng key Groq thực tế của bạn)
GROQ_API_KEY=gsk_your_groq_api_key_here

# JWT config cho Web Backend
JWT_SECRET=super_secret_jwt_key_yhct_2026
JWT_REFRESH_SECRET=super_secret_refresh_key_yhct_2026
```

> [!NOTE]
> File `.env` chứa API Key mặc định của Groq. Nếu bị giới hạn băng thông (Rate Limit), bạn vui lòng thay thế bằng Groq API Key cá nhân của bạn để hệ thống hoạt động ổn định nhất.

---

### Bước 3.2: Khởi chạy các Container

Mở terminal (PowerShell hoặc CMD trên Windows, Terminal trên Linux/macOS) tại thư mục gốc của dự án và chạy lệnh sau để tải ảnh và build dịch vụ:

```bash
docker-compose up -d --build
```

Lệnh này sẽ khởi tạo và khởi chạy đồng thời **13 container** trong mạng ảo `yhct_network`. 

*Để kiểm tra xem tất cả container đã chạy thành công chưa, chạy lệnh:*
```bash
docker-compose ps
```
*(Hãy đợi khoảng 20-30 giây để các dịch vụ PostgreSQL, MongoDB, MinIO tự khởi động hoàn tất và kiểm tra sức khỏe).*

---

### Bước 3.3: Đồng bộ dữ liệu PDF gốc lên MinIO

Các liên kết nguồn tham khảo trên Chatbot AI hoạt động bằng cách hiển thị trực tiếp file PDF gốc được lưu trữ công khai trên MinIO. Bạn cần chạy một lệnh để tải các file PDF từ thư mục `./data/raw` trên máy tính lên bucket `yhct-docs` của MinIO:

Chạy lệnh terminal sau để thực thi script bên trong container `streamlit`:

```bash
docker exec -it streamlit python set_up_minio_docs.py
```

**Kết quả mong đợi:**
```text
✅ Đã tạo bucket: yhct-docs
✅ Đã thiết lập quyền Public Read-Only cho bucket: yhct-docs
📂 Tìm thấy 12 file PDF trong thư mục raw.
  📤 Đang upload: 16_GT Y SY_ Y Hoc Co Truyen.pdf ...
  ✅ Upload thành công: 16_GT Y SY_ Y Hoc Co Truyen.pdf
  ...
```

---

### Bước 3.4: Chạy Pipeline ETL dữ liệu trên Dagster UI

Khi mới khởi tạo hệ thống lần đầu, Vector DB (ChromaDB) hoàn toàn trống. Bạn cần chạy Pipeline Dagster để xử lý văn bản PDF thành dạng cấu trúc Parquet (Bronze & Silver Layer), sau đó chia nhỏ (chunking), tạo Vector Embedding (Gold Layer), và nạp vào cơ sở dữ liệu ChromaDB.

Có **2 cách** để kích hoạt pipeline:

#### Cách 1: Kích hoạt thông qua Dagster UI (Khuyên dùng vì trực quan)
1. Truy cập vào địa chỉ [http://localhost:3001](http://localhost:3001) trên trình duyệt.
2. Chọn mục **Deployment** -> **Jobs** -> Chọn job `all_assets_job` (hoặc truy cập trực tiếp đường dẫn [http://localhost:3001/jobs/all_assets_job](http://localhost:3001/jobs/all_assets_job)).
3. Bấm nút **Launch Run** ở góc phải màn hình.
4. Bạn có thể theo dõi quá trình chạy thực tế dưới dạng sơ đồ khối (Gantt Chart) từ các tầng Bronze -> Silver -> Gold.

#### Cách 2: Kích hoạt thông qua trang Vận hành Streamlit
1. Truy cập giao diện Streamlit tại [http://localhost:8501](http://localhost:8501).
2. Di chuyển sang trang **⚙️ Vận hành** ở menu bên trái.
3. Trong phần **🤖 Giám sát chất lượng dữ liệu & Pipeline**, nhấn nút: **🚀 Khởi chạy toàn bộ Pipeline (Bronze -> Silver -> Gold)**.

> [!TIP]
> Quá trình chạy lần đầu tiên sẽ mất khoảng **3 - 5 phút** để tải mô hình nhúng ngôn ngữ (`keepitreal/vietnamese-sbert` khoảng ~500MB) và tiến hành tạo embeddings cho toàn bộ sách thuốc Đông Y trong kho lưu trữ. Các lần chạy tiếp theo sẽ là **Incremental** (chỉ xử lý file mới) nên thời gian chạy chỉ mất vài giây.

---

### Bước 3.5: Cấu hình trực quan hóa Dashboard trên Apache Superset

Sau khi chạy xong Dagster, dữ liệu hành vi người dùng và tương tác Chatbot từ MongoDB đã được ETL sang Postgres để phân tích.
1. Truy cập Apache Superset tại [http://localhost:8088](http://localhost:8088).
2. Đăng nhập bằng tài khoản Quản trị mặc định:
   - **Tài khoản:** `admin`
   - **Mật khẩu:** `admin123`
3. Tại giao diện Superset, bạn có thể thực hiện kết nối với CSDL Postgres (`postgresql://superset:superset@superset-db:5432/superset` hoặc CSDL Gold ở `de_psql`) và import cấu hình dashboard (nếu có file export `.zip` của dashboard) hoặc tự tạo Chart dựa trên các bảng tầng Gold như `gold_user_engagement`, `gold_chat_performance`, `gold_medical_insights`.

---

## 🎯 4. Hướng Dẫn Sử Dụng Các Phân Hệ Chính

Khi hệ thống đã chạy hoàn chỉnh, Hội đồng có thể trải nghiệm các tính năng cốt lõi:

### 4.1. Cổng thông tin Web Portal chính (Dành cho Người Dùng)
- **Địa chỉ:** [http://localhost](http://localhost) (Truy cập qua Nginx Port 80).
- **Chức năng:**
  - **Đăng ký / Đăng nhập** tài khoản mới trực tiếp trên giao diện.
  - **Nhắn tin với Trợ lý AI**: Đặt các câu hỏi về bài thuốc Đông Y, các vị thuốc Việt Nam, hoặc các chứng bệnh.
  - **Xem nguồn trích dẫn tài liệu**: Khi AI phản hồi, click vào nguồn trích dẫn để mở trực tiếp trang tài liệu PDF gốc liên quan để đối chứng (Dữ liệu được tải trực tiếp từ MinIO).
  - **Theo dõi chỉ số sức khỏe cá nhân**: Giao diện trực quan thống kê lịch sử sức khỏe.

### 4.2. Giao diện Giám sát kỹ thuật & Quản trị (Dành cho Quản trị viên)
- **Địa chỉ:** [http://localhost:8501](http://localhost:8501) (Streamlit).
- **Chức năng:**
  - **1. Trợ lý AI**: Thử nghiệm nhanh các câu hỏi RAG thô.
  - **2. Quản lý người dùng**: Tra cứu danh sách tài khoản, chi tiết lịch sử trò chuyện và hành vi click xem tài liệu nguồn.
  - **3. Phân tích dữ liệu**: Các biểu đồ nhanh về từ khóa thảo dược được hỏi nhiều nhất, các tạng phủ bị ảnh hưởng nhiều nhất, và biểu đồ liên quan.
  - **4. Vận hành**: Cho phép điều chỉnh trực tiếp các tham số RAG (Ngưỡng tương đồng cosine similarity, số lượng tài liệu trích xuất Top-K, mô hình Groq LLM và System Prompt) theo thời gian thực mà không cần khởi động lại container.

---

## ⚠️ 5. Các Lỗi Thường Gặp & Cách Khắc Phục (Troubleshooting)

### 1. Lỗi Không Xem Được File PDF Khi Click Vào Nguồn Tham Khảo
* **Nguyên nhân:** Biến `VITE_MINIO_PUBLIC_URL` trong `docker-compose.yml` đang trỏ đến địa chỉ ngrok cũ hoặc sai cổng kết nối từ trình duyệt.
* **Cách xử lý:** Mở file `.env` hoặc file `docker-compose.yml`, tìm dịch vụ `frontend` và đổi biến `VITE_MINIO_PUBLIC_URL` và `VITE_BACKEND_URL` thành địa chỉ IP local của bạn hoặc `http://localhost` (để gọi qua nginx port 80). Sau đó build lại frontend:
  ```bash
  docker-compose up -d --build frontend
  ```

### 2. Lỗi Groq API "Rate Limit Exceeded" hoặc "Invalid API Key"
* **Nguyên nhân:** API Key mặc định dùng chung trong file `.env` đã hết hạn mức sử dụng miễn phí trong ngày, hoặc không hợp lệ.
* **Cách xử lý:** Truy cập [https://console.groq.com/keys](https://console.groq.com/keys) tạo một API Key mới. Dán key mới vào dòng `GROQ_API_KEY` trong file `.env`. Khởi động lại hệ thống bằng:
  ```bash
  docker-compose down
  docker-compose up -d
  ```

### 3. Dagster Báo Lỗi "Could not resolve host" Khi Chạy Pipeline
* **Nguyên nhân:** Một số container cơ sở dữ liệu chưa sẵn sàng trước khi Dagster thực hiện kết nối.
* **Cách xử lý:** Kiểm tra trạng thái các database bằng `docker-compose ps`. Hãy đảm bảo `de_psql`, `minio` và `chromadb` đang ở trạng thái `healthy` hoặc `running`. Bạn có thể khởi động lại toàn bộ stack để đảm bảo trật tự khởi chạy:
  ```bash
  docker-compose restart
  ```

### 4. Cần Giải Phóng Dữ Liệu Hoặc Chạy Lại Từ Đầu (Reset System)
Nếu muốn làm sạch dữ liệu cũ và nạp lại toàn bộ từ đầu, hãy chạy lệnh xóa các volume Docker:
```bash
docker-compose down -v
```
Lưu ý lệnh này sẽ xóa sạch dữ liệu trong MongoDB, Postgres, MinIO và ChromaDB. Sau đó thực hiện lại các bước từ **Bước 3.2**.

---
Chúc Hội đồng nghiệm thu đồ án thành công tốt đẹp!
