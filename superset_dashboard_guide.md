# 🌿 Hướng dẫn Vận hành & Phát triển Superset Dashboard (Dự án DA_YHCT)

Tài liệu này ghi lại toàn bộ trạng thái công việc đã hoàn thành hôm nay và các bước hướng dẫn cụ thể cho ngày mai (Định dạng hiển thị - Formatting và Thiết lập Bộ lọc - Filters).

---

## 📅 1. Tóm tắt Công việc đã hoàn thành (Ngày 04/07/2026)

### A. Hạ tầng & Sửa lỗi Kiểu dữ liệu (Data Migration)
Chúng ta đã chuyển đổi kiểu dữ liệu các cột ngày tháng từ dạng chuỗi chữ (`TEXT`) sang đúng định dạng `DATE` và `TIMESTAMP` trực tiếp trong cơ sở dữ liệu PostgreSQL (`superset-db`) để tránh lỗi truy vấn khi vẽ biểu đồ thời gian:
1. `gold_user_engagement.date` ➔ Chuyển sang kiểu **`DATE`**.
2. `gold_chat_performance.session_start_time` ➔ Chuyển sang kiểu **`TIMESTAMP`**.
3. `gold_medical_insights.timestamp` ➔ Chuyển sang kiểu **`TIMESTAMP`**.

*Đồng thời, đã cập nhật code Python trong file [user_gold.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/etl_pipeline/etl_pipeline/assets/user_gold.py) tại hàm `write_to_postgres` để tự động gán kiểu dữ liệu chuẩn khi chạy Dagster Pipeline trong tương lai, tránh bị ghi đè thành dạng Text.*

### B. Tạo các Bảng ảo (SQL Views) phục vụ Tab 3
Đã tạo sẵn **5 SQL Views** trong CSDL PostgreSQL để bóc tách mảng JSON của thảo dược, triệu chứng, bệnh lý nhằm hỗ trợ vẽ biểu đồ mà không cần thao tác viết SQL thủ công phức tạp trong Superset:
- `gold_device_share`: Phục vụ biểu đồ tròn tỉ lệ Thiết bị.
- `gold_view_herbs`: Phục vụ biểu đồ Top 10 Thảo dược.
- `gold_view_symptoms`: Phục vụ biểu đồ đám mây Triệu chứng (Word Cloud).
- `gold_view_diseases`: Phục vụ biểu đồ Top 10 Chứng bệnh YHCT.
- `gold_view_body_parts`: Phục vụ biểu đồ phân bố Tạng phủ.

### C. Trạng thái Dashboard hiện tại
- Đã khởi tạo Dashboard: **`🌿 Dashboard Trợ lý Y học Cổ truyền AI`**
- Đã chia làm 3 Tabs chính:
  1. `📊 Tổng quan Tương tác`: Đã hoàn thành 5 biểu đồ cơ bản (Lượt xem, người dùng hoạt động, đăng ký mới, donut thiết bị).
  2. `💬 Trải nghiệm Chatbot AI`: Đã hoàn thành các biểu đồ đánh giá sao, độ trễ AI, độ tuổi và giới tính người dùng.
  3. `🔬 Phân tích Dịch tễ & Y văn`: Đã tạo xong biểu đồ đám mây triệu chứng, biểu đồ tròn tạng phủ, và đang dở dang ở biểu đồ số lớn cuối cùng.

---

## 🛠️ 2. Kế hoạch Ngày mai: Hướng dẫn Định dạng (Formatting) & Thiết lập Bộ lọc (Filters)

### 🎨 PHẦN I: Định dạng Hiển thị đẹp mắt (Formatting & Layout styling)

#### 1. Định dạng Nhãn hiển thị số lớn (Format Metrics)
* Đối với chỉ số phần trăm (như tỷ lệ giữ chân, tỷ lệ thoát): Khi chọn Metric, nhấp vào phần **Number format** và chọn định dạng `.0%` hoặc `.2%` để tự động thêm ký hiệu `%` thay vì chỉ hiện số thập phân thô.
* Đối với điểm đánh giá sao: Ở phần **Number format**, chọn định dạng `.1f` (1 chữ số thập phân) và điền vào ô **Suffix** (Ký tự hậu tố) là ` ★` (ví dụ hiển thị ra: `4.5 ★`).
* Đối với độ trễ: Đặt **Suffix** là ` ms` hoặc viết custom SQL chia cho 1000 để đổi thành giây (`s`).

#### 2. Kéo thả tùy chỉnh Kích thước (Dashboard Grid)
* Bấm **EDIT DASHBOARD** ở góc phải màn hình Dashboard.
* Rê chuột vào cạnh bên của các ô biểu đồ, bạn có thể **kéo giãn rộng ra** hoặc **thu hẹp lại** theo tỷ lệ mong muốn (mỗi dòng tối đa 12 grid).
* Kéo thả các biểu đồ Số lớn (Big Number) lên trên cùng theo hàng ngang (mỗi dòng chứa được 4 số lớn rất đẹp mắt).
* Nhớ nhấn **SAVE** sau khi đổi bố cục.

---

### 🔍 PHẦN II: Thiết lập Bộ lọc Tương tác (Native Filters)
Bộ lọc cho phép người dùng click chọn Thành phố, Giới tính, hoặc Khoảng thời gian trên Dashboard để toàn bộ các biểu đồ tự động cập nhật số liệu theo lựa chọn đó.

#### Các bước tạo Bộ lọc chung trên Dashboard:
1. Mở Dashboard ➔ Nhấp vào **biểu tượng Bộ lọc (Filter icon)** ở thanh sidebar bên trái Dashboard (kế bên tiêu đề biểu đồ đầu tiên).
2. Nhấp nút **+ ADD/EDIT FILTERS** ở góc trái.
3. Ở bảng thiết lập bộ lọc (Native Filters):
   - Nhấp **+ Create Filter**.
   - **Filter Name:** Nhập tên bộ lọc (ví dụ: `Thành phố` hoặc `Giới tính`).
   - **Filter Type:** Chọn **Value** (Chọn giá trị lọc).
   - **Dataset:** Chọn bộ dữ liệu tương ứng (ví dụ: `gold_medical_insights` để lọc thành phố).
   - **Column:** Chọn cột cần lọc (ví dụ: `user_city` để lọc thành phố, `user_gender` để lọc giới tính).
4. Ở tab **Scoping** (Phạm vi ảnh hưởng):
   - Mặc định bộ lọc sẽ ảnh hưởng tới **tất cả biểu đồ** có chung cột dữ liệu đó.
   - Bạn có thể tùy chỉnh chọn biểu đồ nào bị ảnh hưởng hoặc loại trừ.
5. Nhấp **APPLY** ở dưới cùng để lưu bộ lọc.
6. Lúc này, ở cạnh trái Dashboard sẽ xuất hiện bảng bộ lọc. Bạn chỉ cần chọn một thành phố hoặc giới tính, bấm **Apply Filters**, toàn bộ các biểu đồ trên dashboard sẽ thay đổi số liệu theo thời gian thực rất ấn tượng!
