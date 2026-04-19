# Báo Cáo Tiền Xử Lý và Luồng ETL

## 1. Mục tiêu

- Tự động hóa quá trình tiền xử lý dữ liệu bán hàng thô (Raw Data) thành dạng cấu trúc tối ưu (Numpy 2D Array) phục vụ cho thuật toán khai phá dữ liệu (Data Mining / Star-Tree).
- Xử lý triệt để các vấn đề về dữ liệu nhiễu (Noise) và tự động hóa mã hóa đặc trưng (Integer Encoding) theo phân tán dữ liệu (Cardinality).

---

## 2. Kết quả đã hoàn thành

- [x] **Tách module rõ ràng:** Cấu trúc code thành 2 file riêng biệt: `ETL.py` (chứa core logic) và `main.py` (entry point để thực thi).
- [x] **Xây dựng hàm `clean_noise_data` (Silver Layer):** Xử lý 4 loại nhiễu bao gồm Duplicate, Null/NaN, Lỗi logic kinh doanh (Sales <= 0), và Outliers (sử dụng phương pháp IQR).
- [x] **Xây dựng hàm `etl_pipeline` (Gold Layer):** Tự động loại bỏ cột nhiễu (`Transaction_ID`), chuẩn hóa Date, tự động sắp xếp cột theo Cardinality và mã hóa Integer Encoding có bảo toàn thứ tự.
- [x] **Tích hợp Data Visualization:** Thêm hàm `compare_boxplot` sử dụng seaborn và matplotlib để trực quan hóa và kiểm chứng hiệu quả trước/sau khi cắt tỉa Outlier.

---

## 3. Phân tích

- **Làm sạch ngoại lai bằng IQR:** Thay vì hardcode ngưỡng cố định (VD: `Quantity < 1000`), việc sử dụng phân vị (Quantile) $Q1$, $Q3$ và $1.5 \times IQR$ giúp hệ thống tự động thích ứng linh hoạt với các tập dữ liệu có quy mô bán hàng khác nhau, tránh xóa nhầm tập khách sỉ.

- **Tối ưu cây Star-Tree bằng Cardinality Ordering:** Thuật toán tự động quét và tính số lượng unique value của từng Dimension. Cột có Cardinality thấp (như `Customer_Type` - 2 nhánh) sẽ được đẩy lên đầu, giúp giảm thiểu độ phân mảnh của cây ở những tầng cao nhất, tiết kiệm đáng kể RAM khi chạy thuật toán.

- **Bảo toàn thứ tự Thời gian (Ordinal Encoding):** Trong vòng lặp sinh `mapping_dict`, việc bọc hàm `sorted()` quanh `unique()` đảm bảo cột Date được đánh số tịnh tiến (VD: `202502 -> 1`, `202503 -> 2`), giúp bảo toàn ý nghĩa thời gian thay vì gán số lộn xộn theo thứ tự xuất hiện.

---

## 4. Luồng xử lý (Processing Flow)

Luồng dữ liệu đi qua các bước sau (thực thi tuần tự trong bộ nhớ):

1. **Extract (Đọc dữ liệu):** Đọc file CSV bằng `pandas.read_csv`.

2. **Data Cleaning:**
   - Bỏ dòng trùng lặp (`drop_duplicates`).
   - Xóa dòng có cột trọng yếu bị rỗng (`dropna`).
   - Xóa giao dịch hoàn tiền hoặc lỗi (`Sales_Amount > 0` & `Quantity > 0`).
   - Lọc Outlier của cột `Quantity` qua giới hạn cận trên $Q3 + 1.5 \times IQR$.

3. **Transform:**
   - Tính toán Dimension vs Measure.
   - Sắp xếp thứ tự cột theo thứ tự độ phân tán (Cardinality) tăng dần.
   - Áp dụng Hash-map (`mapping_dict`) để Integer Encoding toàn bộ giá trị String sang Int (Bắt đầu từ 1).

4. **Load (Phục vụ thuật toán):**
   - Reorder lại cột (Dimensions đứng trước, Measures đứng cuối).
   - Xuất ra `numpy.ndarray` 2D.

---

## 5. Ví dụ sử dụng

Thực thi thông qua file `main.py`:

```python
from ETL import clean_noise_data, etl_pipeline, compare_boxplot

file_path = r".\test_data.csv"

# 1. Làm sạch dữ liệu
df_clean = clean_noise_data(file_path)

# 2. Vẽ biểu đồ so sánh Outlier
compare_boxplot(file_path)

# 3. Chạy Pipeline và lấy đầu ra cho thuật toán
processed_array, mappings, col_names = etl_pipeline(df_clean)

print("Thứ tự cột sau khi sắp xếp theo Cardinality:", col_names)
```

---

## 6. Kiểm thử

- Quá trình chạy với `test_data.csv` đã bắt và loại bỏ thành công các dòng nhiễu (in ra console rõ ràng số lượng dòng bị loại bỏ).
- Biểu đồ `compare_boxplot` hiển thị rõ rệt sự biến mất của các râu Outliers trên cột `Quantity`.
- Output `processed_array` đúng chuẩn dạng số thực/nguyên (`float`/`int`), không chứa String hay object type, đảm bảo sẵn sàng nạp thẳng vào RAM cho hàm thuật toán chạy.

---

## 7. Kết luận

Luồng xử lý dữ liệu hiện tại đáp ứng được tính chính xác, an toàn bộ nhớ và tối ưu hoá cực kỳ tốt cho đặc thù của thuật toán Star-Tree. Pipeline đã sẵn sàng để merge và tích hợp với thuật toán Data Mining chính.

> **Gợi ý mở rộng tương lai:** Nếu kích thước file CSV đầu vào vượt ngưỡng 2-3 triệu dòng, có thể cân nhắc chuyển đổi core library từ `pandas` sang `polars` ở các PR sau.
