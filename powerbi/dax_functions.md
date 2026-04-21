# Tổng hợp các hàm DAX chống trùng lặp cho `tbl_IcebergCube`

Tài liệu này tổng hợp và trình bày lại các measure DAX quan trọng để xử lý **double counting** khi trực quan hóa dữ liệu Iceberg Cube trong Power BI.

## 1. Vì sao bị trùng lặp dữ liệu?

Bảng `tbl_IcebergCube` không chỉ chứa các dòng chi tiết mà còn chứa nhiều dòng tổng hợp với giá trị `"ALL"` ở các chiều như `Region`, `City`, `Category`, `Customer_Type`, `Payment_Method`. Khi kéo trực tiếp `Total_Sales` hoặc `Count_Txn` vào visual, Power BI sẽ cộng cả dòng chi tiết lẫn dòng tổng hợp, làm số liệu bị nhân lên nhiều lần.

Ví dụ:

- `Hanoi | Electronics | Member` là dòng chi tiết
- `Hanoi | ALL | ALL` là dòng tổng hợp
- `ALL | ALL | ALL` là dòng tổng toàn cục

Nếu cộng tất cả cùng lúc, dashboard sẽ bị **double counting**.

---

## 2. Nguyên tắc xử lý

Có 2 hướng chính:

1. **Dùng DAX** để ép Power BI chỉ lấy đúng cấp dữ liệu đang cần.
2. **Dùng filter visual / slicer** để loại bỏ các dòng `"ALL"`.

Trong repo này, phần dưới tập trung vào hướng **DAX**, vì đây là cách linh hoạt và thể hiện rõ logic BI hơn.

---

## 3. Nhóm measure DAX cơ sở chống trùng lặp

### 3.1. Doanh thu chi tiết — `Sales_Chi_Tiet`

Dùng cho các bảng Matrix hoặc Table khi muốn xem dữ liệu ở **mức chi tiết hoàn toàn**, tức là loại bỏ toàn bộ các dòng tổng hợp có `"ALL"`.

```dax
Sales_Chi_Tiet =
CALCULATE(
    SUM(tbl_IcebergCube[Total_Sales]),
    tbl_IcebergCube[Time_Period] <> "ALL",
    tbl_IcebergCube[Region] <> "ALL",
    tbl_IcebergCube[City] <> "ALL",
    tbl_IcebergCube[Category] <> "ALL",
    tbl_IcebergCube[Customer_Type] <> "ALL",
    tbl_IcebergCube[Payment_Method] <> "ALL"
)
```

**Khi nào dùng**
- Matrix liệt kê các tổ hợp chi tiết
- Bảng top pattern chi tiết
- Các visual cần loại bỏ hoàn toàn roll-up

**Ý nghĩa**
- Chỉ giữ các dòng granular
- Tránh cộng lẫn dữ liệu chi tiết với dữ liệu tổng hợp

---

### 3.2. Doanh thu thông minh theo ngữ cảnh lọc — `Sales_Thong_Minh`

Đây là measure quan trọng nhất. Nó kiểm tra người dùng đang lọc chiều nào, rồi tự động chọn:
- giá trị cụ thể nếu chiều đó đang được filter
- `"ALL"` nếu chiều đó không được filter

Nhờ vậy, visual sẽ lấy đúng dòng aggregate phù hợp trong cube, thay vì quét toàn bộ dữ liệu chi tiết.

```dax
Sales_Thong_Minh =
CALCULATE(
    SUM(tbl_IcebergCube[Total_Sales]),
    tbl_IcebergCube[Region] =
        IF(
            ISFILTERED(tbl_IcebergCube[Region]),
            VALUES(tbl_IcebergCube[Region]),
            "ALL"
        ),
    tbl_IcebergCube[Category] =
        IF(
            ISFILTERED(tbl_IcebergCube[Category]),
            VALUES(tbl_IcebergCube[Category]),
            "ALL"
        ),
    tbl_IcebergCube[Customer_Type] =
        IF(
            ISFILTERED(tbl_IcebergCube[Customer_Type]),
            VALUES(tbl_IcebergCube[Customer_Type]),
            "ALL"
        )
    -- Thêm các chiều còn lại theo cùng cấu trúc:
    -- Time_Period, City, Payment_Method
)
```

**Khi nào dùng**
- Bar chart theo vùng
- Line chart theo thời gian
- KPI card có slicer
- Visual cần tự thích nghi theo filter context

**Ý nghĩa**
- Tận dụng đúng bản chất pre-aggregated của Iceberg Cube
- Tránh double counting mà vẫn giữ được khả năng tương tác

> Lưu ý: trong triển khai thực tế, nên mở rộng measure này cho đầy đủ các chiều:
> `Time_Period`, `Region`, `City`, `Category`, `Customer_Type`, `Payment_Method`.

#### Phiên bản đầy đủ khuyến nghị

```dax
Sales_Thong_Minh_Full =
CALCULATE(
    SUM(tbl_IcebergCube[Total_Sales]),
    tbl_IcebergCube[Time_Period] =
        IF(
            ISFILTERED(tbl_IcebergCube[Time_Period]),
            VALUES(tbl_IcebergCube[Time_Period]),
            "ALL"
        ),
    tbl_IcebergCube[Region] =
        IF(
            ISFILTERED(tbl_IcebergCube[Region]),
            VALUES(tbl_IcebergCube[Region]),
            "ALL"
        ),
    tbl_IcebergCube[City] =
        IF(
            ISFILTERED(tbl_IcebergCube[City]),
            VALUES(tbl_IcebergCube[City]),
            "ALL"
        ),
    tbl_IcebergCube[Category] =
        IF(
            ISFILTERED(tbl_IcebergCube[Category]),
            VALUES(tbl_IcebergCube[Category]),
            "ALL"
        ),
    tbl_IcebergCube[Customer_Type] =
        IF(
            ISFILTERED(tbl_IcebergCube[Customer_Type]),
            VALUES(tbl_IcebergCube[Customer_Type]),
            "ALL"
        ),
    tbl_IcebergCube[Payment_Method] =
        IF(
            ISFILTERED(tbl_IcebergCube[Payment_Method]),
            VALUES(tbl_IcebergCube[Payment_Method]),
            "ALL"
        )
)
```

---

### 3.3. Số lượng giao dịch thông minh — `Giao_Dich_Thong_Minh`

Tương tự `Sales_Thong_Minh`, nhưng áp dụng cho `Count_Txn`.

```dax
Giao_Dich_Thong_Minh =
CALCULATE(
    SUM(tbl_IcebergCube[Count_Txn]),
    tbl_IcebergCube[Region] =
        IF(
            ISFILTERED(tbl_IcebergCube[Region]),
            VALUES(tbl_IcebergCube[Region]),
            "ALL"
        ),
    tbl_IcebergCube[Category] =
        IF(
            ISFILTERED(tbl_IcebergCube[Category]),
            VALUES(tbl_IcebergCube[Category]),
            "ALL"
        )
    -- Thêm các chiều khác theo cùng cấu trúc
)
```

#### Phiên bản đầy đủ khuyến nghị

```dax
Giao_Dich_Thong_Minh_Full =
CALCULATE(
    SUM(tbl_IcebergCube[Count_Txn]),
    tbl_IcebergCube[Time_Period] =
        IF(
            ISFILTERED(tbl_IcebergCube[Time_Period]),
            VALUES(tbl_IcebergCube[Time_Period]),
            "ALL"
        ),
    tbl_IcebergCube[Region] =
        IF(
            ISFILTERED(tbl_IcebergCube[Region]),
            VALUES(tbl_IcebergCube[Region]),
            "ALL"
        ),
    tbl_IcebergCube[City] =
        IF(
            ISFILTERED(tbl_IcebergCube[City]),
            VALUES(tbl_IcebergCube[City]),
            "ALL"
        ),
    tbl_IcebergCube[Category] =
        IF(
            ISFILTERED(tbl_IcebergCube[Category]),
            VALUES(tbl_IcebergCube[Category]),
            "ALL"
        ),
    tbl_IcebergCube[Customer_Type] =
        IF(
            ISFILTERED(tbl_IcebergCube[Customer_Type]),
            VALUES(tbl_IcebergCube[Customer_Type]),
            "ALL"
        ),
    tbl_IcebergCube[Payment_Method] =
        IF(
            ISFILTERED(tbl_IcebergCube[Payment_Method]),
            VALUES(tbl_IcebergCube[Payment_Method]),
            "ALL"
        )
)
```

**Khi nào dùng**
- KPI tổng số giao dịch
- Chart theo vùng / thời gian / nhóm khách hàng
- Các visual nghiệp vụ cần measure transaction đúng ngữ cảnh

---

## 4. Nhóm measure nghiệp vụ xây trên nền measure chống trùng lặp

### 4.1. Giá trị đơn hàng trung bình — `AOV_Chi_Tieu_TB`

Tính AOV bằng doanh thu thông minh chia cho số giao dịch thông minh.

```dax
AOV_Chi_Tieu_TB =
DIVIDE([Sales_Thong_Minh], [Giao_Dich_Thong_Minh], 0)
```

**Ý nghĩa**
- Không bị sai số do double counting
- Phù hợp để so sánh nhóm khách hàng hoặc khu vực có mức chi tiêu cao

---

### 4.2. Tỷ trọng doanh thu theo nhóm — `%_Doanh_Thu_Theo_Nhom`

Dùng cho pie chart hoặc donut chart để tính tỷ trọng của một nhóm trong tổng doanh thu đang được chọn.

```dax
%_Doanh_Thu_Theo_Nhom =
DIVIDE(
    [Sales_Thong_Minh],
    CALCULATE([Sales_Thong_Minh], ALLSELECTED(tbl_IcebergCube))
)
```

**Ý nghĩa**
- Tử số: doanh thu của nhóm hiện tại
- Mẫu số: tổng doanh thu trong phạm vi filter đang chọn
- Dùng để thể hiện contribution %

---

## 5. Nhóm measure đánh giá thuật toán

### 5.1. Số dòng Iceberg Cube — `So_Dong_Iceberg`

```dax
So_Dong_Iceberg = COUNTROWS(tbl_IcebergCube)
```

**Ý nghĩa**
- Đếm số cuboids còn lại sau khi đã prune
- Phù hợp để hiển thị trong KPI card

---

### 5.2. Tỷ lệ nén dữ liệu — `Ty_Le_Nen_Du_Lieu`

Giả sử dữ liệu gốc có 5,000,000 dòng.

```dax
Ty_Le_Nen_Du_Lieu =
VAR Raw_Rows = 5000000
VAR Cube_Rows = [So_Dong_Iceberg]
RETURN
    DIVIDE(Raw_Rows - Cube_Rows, Raw_Rows, 0)
```

**Ý nghĩa**
- Đo mức độ nén dữ liệu của Star-Cubing
- Nên format dạng Percentage trong Power BI

---

## 6. Khuyến nghị sử dụng measure theo từng visual

### Dùng `Sales_Chi_Tiet` khi:
- Bảng Matrix/Table đi sâu tới mức chi tiết
- Cần bỏ toàn bộ dòng `"ALL"`

### Dùng `Sales_Thong_Minh` khi:
- KPI card
- Bar chart
- Line chart
- Dashboard có slicer nhiều chiều

### Dùng `Giao_Dich_Thong_Minh` khi:
- Cần số giao dịch đúng theo filter context

### Dùng `AOV_Chi_Tieu_TB` khi:
- So sánh giá trị trung bình mỗi đơn hàng

### Dùng `%_Doanh_Thu_Theo_Nhom` khi:
- Pie / Donut chart
- Phân tích contribution theo nhóm

### Dùng `So_Dong_Iceberg` và `Ty_Le_Nen_Du_Lieu` khi:
- Dashboard đánh giá hiệu năng thuật toán
- Báo cáo khóa luận / thuyết trình

---

## 7. Lưu ý triển khai thực tế

1. Không kéo trực tiếp `Total_Sales` hoặc `Count_Txn` vào visual nếu bảng cube có chứa dòng `"ALL"`.
2. Nên chuẩn hóa tất cả các measure quan trọng theo cùng một logic filter context.
3. Nếu visual chỉ cần dữ liệu chi tiết, có thể kết hợp thêm filter visual để loại bỏ `"ALL"` như một lớp an toàn.
4. Với measure kiểu `ISFILTERED + VALUES`, cần đảm bảo mỗi slicer trả về đúng một ngữ cảnh phù hợp.
5. Nên đặt tên measure rõ ràng để phân biệt:
   - measure chi tiết
   - measure thông minh theo cube
   - measure nghiệp vụ

---

## 8. Bộ measure đề xuất tối thiểu cho repo

Nếu chỉ giữ bộ measure cốt lõi, nên ưu tiên:

- `Sales_Chi_Tiet`
- `Sales_Thong_Minh_Full`
- `Giao_Dich_Thong_Minh_Full`
- `AOV_Chi_Tieu_TB`
- `%_Doanh_Thu_Theo_Nhom`
- `So_Dong_Iceberg`
- `Ty_Le_Nen_Du_Lieu`

---

## 9. Kết luận

Cốt lõi của bài toán Power BI với Iceberg Cube không nằm ở việc “vẽ chart”, mà nằm ở việc **kiểm soát đúng cấp độ dữ liệu cần lấy**. Vì bảng cube chứa đồng thời dữ liệu chi tiết và dữ liệu tổng hợp (`"ALL"`), nên nếu không dùng DAX hoặc filter phù hợp thì dashboard sẽ bị **double counting**.

Do đó, bộ measure chống trùng lặp là phần bắt buộc để:
- bảo toàn tính đúng đắn của số liệu,
- tận dụng lợi thế pre-aggregated của Iceberg Cube,
- và biến dashboard thành công cụ phân tích đáng tin cậy.
