# 🌿 Trợ lý AI & Hồ dữ liệu Y học Cổ truyền Việt Nam (YHCT)

Hệ thống chatbot tư vấn sức khỏe bằng Y học Cổ truyền tích hợp RAG, điều phối dữ liệu qua Dagster và trực quan hóa Dashboard phân tích trên Apache Superset.

---

## 🚀 HƯỚNG DẪN KHỞI CHẠY NHANH (QUAN TRỌNG)

### Bước 1: Khởi chạy toàn bộ hệ thống bằng Docker
Mở cửa sổ dòng lệnh (Terminal/Command Prompt) tại thư mục dự án và chạy:
```bash
docker-compose up -d
```

### Bước 2: Kích hoạt đường dẫn công khai Ngrok (Bắt buộc)
Mở **thêm một cửa sổ dòng lệnh mới** và gõ lệnh sau để mở cổng kết nối internet:
```bash
ngrok http --url=smirk-video-attest.ngrok-free.dev 80
```
*➔ Sau khi chạy xong 2 bước trên, bạn truy cập hệ thống tại: **[https://smirk-video-attest.ngrok-free.dev/](https://smirk-video-attest.ngrok-free.dev/)***

### Bước 3: Đồng bộ sách PDF lên MinIO (Chỉ chạy lần đầu tiên)
Để chatbot hiển thị trích dẫn nguồn sách PDF chính xác cho người dùng, hãy chạy lệnh đồng bộ:
```bash
docker exec -it streamlit python set_up_minio_docs.py
```

---

## 🌐 DANH SÁCH ĐỊA CHỈ TRUY CẬP HỆ THỐNG

| Dịch vụ / Phân hệ | Đường dẫn truy cập | Tài khoản mặc định | Mô tả chức năng |
| :--- | :--- | :--- | :--- |
| **Cổng Chatbot AI chính** | [https://smirk-video-attest.ngrok-free.dev/](https://smirk-video-attest.ngrok-free.dev/) | *Đăng ký trực tiếp trên Web* | Giao diện nhắn tin tư vấn y học cổ truyền |
| **Apache Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin123` | Dashboard trực quan hóa dữ liệu hệ thống |
| **Dagster Pipeline** | [http://localhost:3001](http://localhost:3001) | *Không yêu cầu* | Quản lý và vận hành luồng ETL Lakehouse |
| **Streamlit Admin** | [http://localhost:8501](http://localhost:8501) | *Không yêu cầu* | Vận hành, điều chỉnh tham số RAG thời gian thực |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minio` / `minio123` | Quản lý file PDF nguồn và Object Storage |

---

## ⚙️ CẤU HÌNH GROQ API KEY (Khi bị giới hạn lượt gọi)
Nếu Chatbot báo lỗi hết hạn hoặc quá giới hạn lượt gọi API (Rate Limit), bạn làm như sau:
1. Truy cập [Groq Console](https://console.groq.com/keys) tạo một API Key miễn phí mới.
2. Mở file `.env` ở thư mục gốc, thay thế key mới vào dòng:
   ```env
   GROQ_API_KEY=gsk_your_new_key_here
   ```
3. Restart lại hệ thống để áp dụng key mới:
   ```bash
   docker-compose down
   docker-compose up -d
   ```
