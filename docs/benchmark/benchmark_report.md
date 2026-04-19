# Benchmark Report - POS CSV

## 1. Mục tiêu

So sánh 2 biến thể Star-cubing trên dữ liệu bán lẻ thật:

- Star-cubing baseline (trước tăng cường)
- Star-cubing enhanced (sau tăng cường)

## 2. Cấu hình benchmark

- Input: `data/pos_data.csv`
- Raw limit: `5000000` dòng CSV thô (full raw data)
- Cleaned rows sau ETL: `4965008`
- Dataset sizes benchmark: `full` (`4965008` rows)
- Repeat: `1`
- Iceberg threshold: `min_sup = 18000000`
- Shuffle seed: `20260418`
- Metrics: runtime, CPU time, peak tracemalloc, cube rows, output storage

## 3. Kết quả tổng hợp

Nguồn: `docs/benchmark/logs/summary_by_algorithm.csv`

| Algorithm | Mean Runtime (s) | Mean CPU (s) | Mean Peak RAM (MB) | Mean Output (KB) | Mean Cube Rows |
| :-- | --: | --: | --: | --: | --: |
| Star-cubing baseline | 127.7213 | 126.7344 | 46.4490 | 1925.4940 | 57637.00 |
| Star-cubing enhanced | 128.3914 | 127.2813 | 45.3560 | 1912.9970 | 57252.00 |

### Nhận xét chính

- Runtime giữa baseline và enhanced gần tương đương trên full-data.
- Enhanced dùng peak memory thấp hơn baseline (45.356MB vs 46.449MB).
- Enhanced tạo output gọn hơn baseline (1912.997KB vs 1925.494KB).
- Enhanced sinh ít cube rows hơn baseline (57252 vs 57637), thể hiện hiệu quả từ cơ chế tăng cường.

## 4. Biểu đồ bằng chứng

Các file được sinh tại:

- `docs/benchmark/charts/runtime_line.png`
- `docs/benchmark/charts/memory_bar.png`
- `docs/benchmark/charts/storage_line.png`

Diễn giải nhanh:

- `runtime_line.png`: runtime baseline và enhanced xấp xỉ nhau.
- `memory_bar.png`: enhanced có peak memory thấp hơn baseline.
- `storage_line.png`: enhanced có kích thước output thấp hơn baseline.

## 5. Artifact phục vụ báo cáo

- Log chi tiết: `docs/benchmark/logs/performance_log.csv`
- Log JSON: `docs/benchmark/logs/performance_log.json`
- Bảng tổng hợp: `docs/benchmark/logs/summary_by_algorithm.csv`
- Tài liệu kỹ thuật: `docs/benchmark/benchmark_algorithm_spec.md`

## 6. Kết luận

Benchmark trên `pos_data.csv` đã hoàn tất với full 5,000,000 dòng raw. Kết quả cho thấy phiên bản Star-cubing enhanced cải thiện về memory và kích thước output so với baseline, đồng thời giữ runtime ở mức tương đương.
