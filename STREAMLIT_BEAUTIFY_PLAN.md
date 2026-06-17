# Kế hoạch thiết kế đồng bộ giao diện Xanh lá cho Streamlit Admin Portal

Kế hoạch này đề xuất cải tiến toàn diện thẩm mỹ (UI/UX) của ứng dụng Streamlit Admin Portal để đồng bộ hoàn toàn với giao diện chính của người dùng (Web Portal), sử dụng tông màu chủ đạo xanh lá dược liệu (`#1a6b3c` / `#2d9e5f`), phông chữ hiện đại và hiệu ứng cao cấp.

---

## User Review Required

> [!IMPORTANT]
> **Thiết lập Theme Toàn cục qua `.streamlit/config.toml`**:
> - Chúng tôi sẽ tạo tệp cấu hình [config.toml](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/.streamlit/config.toml) để ép Streamlit sử dụng một bảng màu xanh lá sạch sẽ, đồng bộ.
> - Việc này đảm bảo ứng dụng luôn hiển thị đồng nhất ở mọi thiết bị và trình duyệt của người dùng mà không bị ảnh hưởng bởi cài đặt "Force Dark Mode" của hệ thống làm lỗi màu chữ.

> [!TIP]
> **Font chữ hiện đại (Outfit)**:
> - Sử dụng Google Fonts `Outfit` thay cho phông chữ mặc định của hệ thống để mang lại cảm giác cao cấp, hiện đại giống như các ứng dụng SaaS chuyên nghiệp.

---

## Proposed Changes

### 1. Global Streamlit Configuration

#### [NEW] [.streamlit/config.toml](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/.streamlit/config.toml)
- Định nghĩa cấu hình theme mặc định cho Streamlit:
  - `primaryColor`: `#2d9e5f` (Xanh lá sáng)
  - `backgroundColor`: `#f8faf8` (Nền sáng xanh nhẹ cực dịu mắt)
  - `secondaryBackgroundColor`: `#eef4f0` (Nền các nút và selectbox)
  - `textColor`: `#1c3225` (Chữ màu lục đậm)
  - `font`: `sans serif`

---

### 2. Main Page Styles (Chat page)

#### [MODIFY] [app.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/app.py)
- Import font `Outfit` từ Google Fonts.
- Thiết kế lại khung chat chào mừng (`welcome-box`) và các nút gợi ý (`suggestion-btn`) với hiệu ứng đổ bóng mềm, viền xanh lá bo góc tròn.
- Định dạng lại bong bóng chat:
  - Bong bóng của AI (`.assistant-bubble`): Màu nền trắng sữa, viền mỏng xanh nhạt, chữ lục tối `#1a3020`.
  - Bong bóng của User (`.user-bubble`): Màu xanh lá đậm `#1a6b3c`, chữ trắng.
  - Các liên kết trích dẫn (`.source-tag`): Thiết kế lại dạng thẻ badge bo tròn chuyên nghiệp.

---

### 3. User & Chat Management Page

#### [MODIFY] [1_user_management.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/1_user_management.py)
- Áp dụng phông chữ `Outfit` và đồng bộ thiết kế bong bóng chat giống như trang chính.
- Định dạng lại các khung chứa (`.chat-container`) và bảng chi tiết thông tin người dùng với viền bo góc tròn mềm mại và đổ bóng nhẹ (`box-shadow`).

---

### 4. Analytics Dashboard Page

#### [MODIFY] [2_analytics.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/2_analytics.py)
- Áp dụng phông chữ `Outfit` và đồng bộ màu sắc.
- Viết CSS tùy biến để làm đẹp các tab (`st.tabs`):
  - Tab được chọn sẽ có chữ màu xanh lá đậm và đường gạch chân nổi bật.
  - Các tab khác có màu xám nhẹ, khi hover chuột sẽ sáng lên xanh lục.
- Nâng cấp thẩm mỹ các thẻ chỉ số KPI (`st.metric`) với nền bo tròn góc màu xanh lá nhạt.

---

### 5. Technical Operations Page

#### [MODIFY] [3_operations.py](file:///d:/Khoa_Luan_Tot_Nghiep/DA_YHCT/streamlit_app/pages/3_operations.py)
- Áp dụng phông chữ `Outfit`.
- Trang trí lại các thẻ kiểm thử chất lượng dữ liệu (`.card`) với hiệu ứng bo tròn góc và đổ bóng nhẹ chuyên nghiệp.
- Làm nổi bật nút bấm kích hoạt pipeline bằng màu xanh lá chủ đạo.

---

## Verification Plan

### Manual Verification
1. **Kiểm tra tính đồng nhất của giao diện**:
   - Truy cập vào Streamlit Admin Portal.
   - Kiểm tra phông chữ `Outfit` đã được áp dụng đồng bộ ở tất cả các trang chưa.
   - Chuyển đổi qua lại giữa các trang `App`, `User Management`, `Analytics`, và `Operations`, kiểm tra xem các nút bấm, selectbox, và nhãn chữ có hiển thị đồng bộ tông màu xanh lá hay không.
2. **Kiểm tra tính hiển thị và tương phản**:
   - Xác nhận chữ không bị mờ hoặc trùng màu nền trên các bảng chọn hay input form.
   - Kiểm tra các tab của Analytics có hiệu ứng chuyển đổi mượt mà và trực quan.
