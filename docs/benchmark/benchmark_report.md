# Benchmark Report - POS CSV

## 1. Mục tiêu

So sánh hiệu năng và không gian lưu trữ của 4 thuật toán trên dữ liệu bán lẻ:

- Star-cubing baseline (trước tăng cường)
- Star-cubing enhanced (sau tăng cường)
- BUC
- Bottom-up

## 2. Cấu hình benchmark

- Input: `data/pos_data.csv`
- Algorithm set: `full`
- Raw limit: `1500000` dòng CSV thô
- Cleaned rows sau ETL: `1488881`
- Dataset sizes benchmark: `1000000`
- Repeat: `1`
- Iceberg threshold: `min_sup = 18000000`
- Shuffle seed: `20260418`
- Metrics: runtime, CPU time, peak tracemalloc, cube rows, output storage

## 3. Kết quả tổng hợp

Nguồn: `docs/benchmark/logs/summary_by_algorithm.csv`

| Algorithm             | Mean Runtime (s) | Mean CPU (s) | Mean Peak RAM (MB) | Mean Output (KB) | Mean Cube Rows |
| :-------------------- | ---------------: | -----------: | -----------------: | ---------------: | -------------: |
| Star-cubing enhanced  |          39.9522 |      35.9844 |            41.5210 |        1645.1640 |       50851.00 |
| BUC                   |         209.4830 |     171.9531 |            42.3420 |        1688.8130 |       52182.00 |
| Star-cubing baseline  |         403.2314 |     337.7969 |            41.8540 |        1636.1460 |       50583.00 |
| Bottom-up             |         726.2356 |     609.6406 |            53.5730 |        1688.8130 |       52182.00 |

### Nhận xét chính

- Star-cubing enhanced có runtime trung bình thấp nhất trong 4 thuật toán.
- Bottom-up có peak tracemalloc cao nhất trong profile này.
- Bottom-up là thuật toán chậm nhất ở mốc 1,000,000 rows.
- Star-cubing baseline cho output nhỏ nhất (1636.146 KB), nhưng runtime vẫn chậm hơn enhanced.

## 4. Biểu đồ bằng chứng

Các file được sinh tại:

- `docs/benchmark/charts/runtime_line.png`
- `docs/benchmark/charts/memory_bar.png`
- `docs/benchmark/charts/storage_line.png`

Diễn giải nhanh:

- `runtime_line.png`: do profile chỉ có 1 mốc dữ liệu (`1000000`) nên biểu đồ được render dạng bar để dễ so sánh.
- `memory_bar.png`: thể hiện peak memory trung bình theo thuật toán trên mốc 1,000,000 rows.
- `storage_line.png`: với profile single-size cũng được render dạng bar để dễ quan sát chênh lệch.

## 5. Artifact phục vụ báo cáo

- Log chi tiết: `docs/benchmark/logs/performance_log.csv`
- Log JSON: `docs/benchmark/logs/performance_log.json`
- Bảng tổng hợp: `docs/benchmark/logs/summary_by_algorithm.csv`
- Tài liệu kỹ thuật: `docs/benchmark/benchmark_algorithm_spec.md`

## 6. Kết luận

Benchmark mốc `1000000` đã hoàn tất trên dữ liệu lớn và sinh đủ log/charts cho 4 thuật toán. Ở profile mới nhất này, Star-cubing enhanced có thời gian chạy thấp nhất, trong khi Bottom-up là phương án chậm nhất. Về kích thước output, Star-cubing baseline nhỏ nhất nhưng đánh đổi bằng thời gian chạy cao hơn enhanced.
