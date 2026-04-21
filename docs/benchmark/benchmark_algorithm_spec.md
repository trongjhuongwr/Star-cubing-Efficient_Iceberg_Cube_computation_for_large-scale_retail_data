# Benchmark - Đặc Tả Thuật Toán & Hướng Dẫn Cài Đặt

## Mục tiêu

Tài liệu này mô tả luồng benchmark hiện tại:

- Đặc tả cách benchmark 3 hướng tiếp cận: Star-cubing (Star-tree), BUC, Bottom-up.
- Đặc tả cách benchmark 2 biến thể Star-cubing: baseline (trước tăng cường) và enhanced (sau tăng cường).
- Chuẩn hóa quy trình cài đặt/chạy để tái lập số liệu thực nghiệm.
- Mô tả luồng dữ liệu từ input tổng hợp đến log/charts dùng cho báo cáo Word.

## Data Contract dùng cho benchmark

- Dữ liệu đầu vào: file CSV gốc `data/pos_data.csv` đi qua ETL để làm sạch và integer encoding.
- Benchmark full-data cho Phase 6 dùng toàn bộ 5,000,000 dòng raw: `--raw-limit 5000000`.
- Thứ tự chiều: `Time_Period`, `Region`, `City`, `Category`, `Customer_Type`, `Payment_Method`.
- Measure:
  - `total_sales` (float) dùng làm điều kiện iceberg.
  - `count_txn` (int) dùng làm chỉ số số giao dịch.
- Ngưỡng cắt tỉa: `min_sup` áp dụng duy nhất trên `total_sales`.
- Giá trị roll-up: dùng chuỗi `'ALL'`.

## Thiết kế giải thuật trong Phase 6

### 1) Star-cubing (Star-tree)

- Dữ liệu nạp vào cây tiền tố theo thứ tự chiều.
- Tại mỗi node cộng dồn `total_sales` và `count_txn`.
- Khi tổng hợp, các prefix hoặc value hỗ trợ thấp được cuộn thành `'ALL'`.
- Ưu điểm trong benchmark: kết quả có xu hướng nén tốt, output nhỏ hơn.

### 2) Star-cubing enhanced

- Sử dụng cơ chế tăng cường top-down và bottom-up để giảm số nhánh/cuboid cần duyệt.
- Vẫn giữ chuẩn output Iceberg Cube theo ngưỡng `min_sup`.

## Luồng thực thi benchmark

1. Script đọc một prefix của `data/pos_data.csv` (`--raw-limit`), làm sạch nhiễu bằng ETL, rồi integer encoding theo cardinality.
2. Dữ liệu sau ETL được shuffle deterministically bằng `--seed`.
3. Chạy lần lượt 4 thuật toán (Star-cubing baseline, Star-cubing enhanced, BUC, Bottom-up) trên cùng một batch dữ liệu.
4. Đo các metric:
   - `elapsed_sec`
   - `cpu_sec`, `cpu_utilization_pct`
   - `rss_before_mb`, `rss_after_mb`, `rss_delta_mb`
   - `tracemalloc_peak_mb`
   - `cube_rows`, `output_storage_kb`
5. Ghi log vào `docs/benchmark/logs`.
6. Vẽ chart vào `docs/benchmark/charts`.

## Hướng dẫn cài đặt

### Yêu cầu môi trường

- Python >= 3.14 (hoặc 3.10+ nếu tương thích dependency).
- Pip package manager.

### Cài dependency

```bash
pip install -r requirements.txt
```

### Chạy benchmark full-data

```bash
python scripts/benchmark.py --data-path data/pos_data.csv --algorithm-set full --sizes 2000,5000,10000 --repeats 1 --raw-limit 15000 --min-sup 18000000 --chunk-size 50000 --seed 20260418
```

### Chạy benchmark với quy mô raw khác

```bash
python scripts/benchmark.py --data-path data/pos_data.csv --algorithm-set full --sizes full --repeats 1 --raw-limit 5000000 --min-sup 18000000 --chunk-size 100000 --seed 20260418
```

## Đầu ra chuẩn dùng cho báo cáo

- `docs/benchmark/logs/performance_log.csv`
- `docs/benchmark/logs/performance_log.json`
- `docs/benchmark/logs/summary_by_algorithm.csv`
- `docs/benchmark/charts/runtime_line.png`
- `docs/benchmark/charts/memory_bar.png`
- `docs/benchmark/charts/storage_line.png`

## Kết luận kỹ thuật ngắn

- Với profile full-data hiện tại, baseline và enhanced có runtime tương đương, nhưng enhanced tiết kiệm bộ nhớ và cho output gọn hơn.
