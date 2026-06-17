# Kế hoạch nâng cấp Streamlit Admin Portal cho Traditional Medicine RAG Lakehouse

Kế hoạch này đề xuất thiết lập một hệ thống **Admin Portal** hoàn chỉnh trên Streamlit để quản trị hệ thống dữ liệu, giám sát tương tác người dùng, kiểm định chất lượng phản hồi của chatbot RAG và cấu hình nóng các tham số hệ thống mà không cần sửa code.

---

## User Review Required

> [!IMPORTANT]
> **Cấu hình tham số RAG động**:
> - Các thay đổi tham số RAG (như `MIN_SIM`, `TOP_K`, `SYSTEM_PROMPT`) sẽ được cập nhật và áp dụng ngay lập tức trong bộ nhớ (in-memory) của `ai_service`. 
> - Nếu container `ai_service` bị restart, các cấu hình này sẽ quay về mặc định. Để duy trì lâu dài, chúng ta sẽ hướng tới lưu trữ cấu hình vào file/DB ở giai đoạn tiếp theo nếu cần thiết.

> [!NOTE]
> **Bảo mật dữ liệu (PII Protection)**:
> - Dữ liệu email của người dùng hiển thị trên Admin Portal sẽ là email đã ẩn danh (hashed) từ Gold/Silver layer để đảm bảo tuân thủ bảo mật thông tin cá nhân.

---

## Open Questions

> [!NOTE]
> 1. Bạn có muốn lưu trữ cấu hình RAG lâu dài (persistent) sau khi chỉnh sửa trên Streamlit không? Nếu có, chúng ta có thể ghi đè trực tiếp lên một file cấu hình JSON được mount trong `ai_service` hoặc lưu vào MongoDB.
> 2. Dashboard Analytics của Superset đang chạy ở cổng 8088. Bạn muốn vẽ biểu đồ trực tiếp trên Streamlit bằng Plotly (tải dữ liệu từ MinIO Parquet) hay muốn nhúng (embed) dashboard của Superset vào Streamlit? (Phương án vẽ trực tiếp bằng Plotly được khuyên dùng để tăng tính chủ động và giao diện đồng nhất).

---

## Proposed Changes

### 1. AI Service Configuration

Cung cấp khả năng tinh chỉnh nóng các tham số AI RAG (như threshold độ tương đồng, system prompt) trực tiếp từ giao diện quản trị Admin Streamlit.

#### [MODIFY] [main.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/ai_service/main.py)
- Chuyển các hằng số cấu hình (`MIN_SIM`, `TOP_K`, `_SYSTEM_PROMPT`) thành biến cấu hình động (in-memory config object).
- Thêm endpoint `GET /api/config` trả về cấu hình RAG hiện tại.
- Thêm endpoint `POST /api/config` để cập nhật cấu hình RAG lập tức trong bộ nhớ.

---

### 2. Streamlit Admin Pages

Mở rộng cấu hình ứng dụng Streamlit thành đa trang để phân chia rõ ràng các nghiệp vụ quản trị hệ thống.

#### [NEW] [1_user_management.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/1_user_management.py)
- **Danh sách người dùng**: Đọc bảng `silver_mongodb_users.parquet` trên MinIO, hiển thị thông tin dạng bảng có lọc/tìm kiếm.
- **Danh sách phiên hội thoại**: Lọc và hiển thị các phiên chat (`session_id`) của người dùng được chọn từ `silver_mongodb_conversations.parquet`.
- **Trình duyệt lịch sử chat**: Hiển thị chi tiết nội dung trao đổi dạng bong bóng chat. Đính kèm siêu dữ liệu kỹ thuật dưới mỗi phản hồi (độ trễ phản hồi, trạng thái RAG Fallback/Tin xã giao, độ khớp Cosine, link tải nguồn PDF trích dẫn từ MinIO).

#### [MODIFY] [2_analytics.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/2_analytics.py)
- Giữ nguyên các tab cũ ("🌿 Dược liệu", "🫀 Tạng phủ", "📄 Chunks", "📚 Nguồn tài liệu", "📥 Thêm tài liệu").
- Thêm tab **"👥 Tương tác Người dùng"**: Thống kê số lượng active users hàng ngày (DAU), số lượng new users, page views, retention rate, và phân bố thiết bị truy cập từ `gold_user_engagement.parquet`.
- Thêm tab **"💬 Hiệu năng Chatbot"**: Thống kê độ trễ phản hồi trung bình (latency trend), phân bố xếp hạng đánh giá chatbot (satisfaction rate), tỷ lệ tin nhắn xã giao từ `gold_chat_performance.parquet`.
- Thêm tab **"🩺 Xu hướng Dịch tễ"**: Phân tích top triệu chứng thường gặp, các bộ phận cơ thể và dược liệu được người dùng hỏi nhiều nhất từ `gold_medical_insights.parquet`.

#### [NEW] [3_operations.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/3_operations.py)
- **Giám sát Data Quality**: Gọi GraphQL API của Dagster để hiển thị lịch sử và trạng thái Pass/Fail của 8 bài kiểm tra chất lượng dữ liệu (Data Quality checks) gần nhất.
- **Cấu hình tham số RAG**: Giao diện slider và textbox để cấu hình các tham số của `ai_service`. Gửi yêu cầu HTTP POST để cập nhật cấu hình động của chatbot.
- **Bảng điều khiển Pipeline**: Cho phép Admin bấm nút cưỡng bức khởi chạy lại (Force run/Materialize) Dagster pipeline ngay tại Streamlit.

---

## Verification Plan

### Automated Tests
- Kiểm tra các endpoint cấu hình mới bằng lệnh `curl`:
  ```bash
  # Lấy cấu hình hiện tại
  curl http://localhost:8001/api/config
  
  # Cập nhật cấu hình mới
  curl -X POST -H "Content-Type: application/json" -d '{"min_sim": 0.45, "top_k": 4}' http://localhost:8001/api/config
  ```

### Manual Verification
1. **Xác minh Quản lý Người dùng & Chat**:
   - Truy cập trang `1_user_management` trên Streamlit.
   - Chọn một người dùng ngẫu nhiên, xác nhận danh sách session chat hiển thị đúng.
   - Chọn một session chat, kiểm tra xem bong bóng chat có hiển thị đúng câu hỏi của user và câu trả lời của AI kèm các siêu dữ liệu (độ trễ, link download nguồn) hay không.
2. **Xác minh Dashboard Analytics mới**:
   - Truy cập trang `2_analytics`, kiểm tra các tab tương tác, hiệu năng chatbot và xu hướng dịch tễ có vẽ biểu đồ Plotly chính xác từ MinIO hay không.
3. **Xác minh Giám sát & Vận hành**:
   - Chỉnh sửa tham số `MIN_SIM` từ trang cấu hình, thực hiện chat thử trên chatbot để kiểm chứng sự thay đổi threshold lọc tài liệu.
   - Kiểm tra bảng thống kê chất lượng dữ liệu xem có hiển thị đúng 8 bài test từ Dagster hay không.
