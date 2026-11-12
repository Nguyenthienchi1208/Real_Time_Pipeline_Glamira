# Runbook

## 1. Mục tiêu

Tài liệu này mô tả cách khởi động, giám sát và xử lý sự cố cho pipeline real-time.

## 2. Kiến trúc hệ thống

- Kafka: ingest sự kiện người dùng
- PySpark Structured Streaming: xử lý và enrich dữ liệu
- PostgreSQL: lưu fact và dimension tables
- batch_runs: lưu trạng thái batch và health metrics
- HDFS checkpoint: lưu offset và trạng thái streaming

## 3. Môi trường cần chuẩn bị

1. Copy file mẫu môi trường:
   ```bash
   cp .env.example .env
   ```
2. Điền các giá trị thực cho Kafka và PostgreSQL.
3. Không commit file .env.
4. Khởi động dịch vụ bằng Docker Compose:
   ```bash
   docker compose up -d
   ```

## 4. Khởi chạy pipeline

### Live mode
```bash
PYTHONPATH=src python src/main.py
```

### Dry run
```bash
PYTHONPATH=src python src/main.py --dry-run
```

### Backfill
```bash
PYTHONPATH=src python src/main.py --backfill-days 7
```

## 5. Giám sát

### Kiểm tra batch status
```sql
SELECT batch_id, status, rows_in, started_at, finished_at, error_message
FROM batch_runs
ORDER BY started_at DESC;
```

### Kiểm tra số lượng dữ liệu trong fact table
```sql
SELECT COUNT(*) FROM fact_product_view;
```

### Kiểm tra pipeline health
Có thể dùng script hoặc gọi helper health check trong code nếu đã triển khai trong môi trường vận hành.

## 6. Xử lý sự cố

### 6.1 Pipeline dừng
- Kiểm tra container Kafka/PostgreSQL/Hadoop đang chạy.
- Kiểm tra log Spark và Kafka.
- Kiểm tra checkpoint path.

### 6.2 Batch thất bại
- Duyệt bảng `batch_runs` để xác định `batch_id` bị lỗi.
- Xem `error_message` và `rows_in`.
- Re-run lại batch hoặc chạy backfill nếu cần khôi phục dữ liệu.

### 6.3 Dữ liệu trùng lặp
- Kiểm tra unique constraint và staging unique keys.
- Kiểm tra dữ liệu đầu vào có duplicate `event_id` không.
- Sử dụng lại logic idempotency dựa trên `event_id` / payload hash.

### 6.4 SLA vi phạm
- Nếu freshness hoặc lag vượt ngưỡng, kiểm tra Kafka consumer lag, Postgres write lag và checkpoint.
- So sánh thời gian `updated_at` trong `batch_runs` với thời điểm hiện tại.

## 7. Recovery checklist

- [ ] Kiểm tra Docker services
- [ ] Kiểm tra Kafka topic và server
- [ ] Kiểm tra PostgreSQL connectivity
- [ ] Kiểm tra `batch_runs` cuối cùng
- [ ] Kiểm tra checkpoint path
- [ ] Chạy lại pipeline hoặc backfill phù hợp

## 8. Tài liệu liên quan

- README.md: overview và hướng dẫn khởi tạo
- sql/schema.sql: schema fact/dimension/staging tables
- src/main.py: pipeline entry point và run config
- src/utils/postgres_handler.py: batch status và health helpers
