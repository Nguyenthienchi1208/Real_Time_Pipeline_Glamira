Bạn là Senior Data Engineer. Repo: /home/chi/Project_DE/Real_Time_Pipeline_Glamira
(Kafka -> Spark Structured Streaming -> Postgres, star schema).

MỤC TIÊU
Chạy LOCAL và đo các chỉ số THẬT để điền vào 4 CV bullet ở cuối prompt.
Không sửa logic nghiệp vụ. Chỉ viết script đo, chạy, báo cáo số.

QUY TẮC BẮT BUỘC
1. KHÔNG bịa số, không làm tròn cho đẹp. Mọi số phải lấy từ output của lệnh đã chạy thật,
   dán output gốc. Không đo được thì ghi "CHƯA ĐO" + lý do cụ thể.
2. Script đặt trong scripts/benchmarks/, chạy lại được bằng 1 lệnh.
3. Dùng Postgres/schema TEST riêng (vd schema bench_test), không đụng data live.
4. Benchmark phải IMPORT và GỌI đúng hàm production (validate, transform, ghi Postgres).
   Cấm viết lại logic validate/dedupe riêng trong script đo.
5. Không in secret. Che password nếu xuất hiện trong output.
6. Làm tuần tự từng bước, xong mỗi bước báo cáo rồi mới làm tiếp.
7. Nếu phát hiện số xấu (dedupe không hoạt động, bad record lọt vào fact, benchmark cũ
   dùng fixture/hardcode), BÁO NGAY, không che, không chỉnh cho đẹp.

BƯỚC 0 - Kiểm toán và dựng môi trường
- Đọc scripts/benchmarks/01..05_*.sh cũ: với mỗi script cho biết số lấy từ DB/log thật
  hay hardcode/fixture. Đặc biệt 05_monitoring (nghi dùng sample data).
- Kiểm tra: docker, Java, Python venv, Postgres. Dựng bằng `docker compose up -d` các
  service cần cho từng bước (Postgres cho bước 2-3; Kafka + Spark cho bước 5-6).
- Ghi cấu hình máy: CPU, RAM, OS, Docker hay native, số partition Spark.

BƯỚC 1 - [K] Credential (chỉ cần git)
- git log --oneline -- docker-compose.yml run_spark.sh conf/kafka_jaas.conf src/config/settings.py
- Tìm commit CÒN credential literal. Chạy
  git grep -nE "UnigapPostgres|UnigapKafka|password|secret" <commit>
  và trên HEAD. Báo số dòng khớp theo từng file, tổng K trước, K sau.
- Nếu mọi commit đều 0: ghi "KHÔNG CÓ MỐC TRƯỚC", không ép số.
- .env.example chỉ được chứa placeholder.

BƯỚC 2 - [N, M, M/N] Data Quality
- Sinh N=10,000 event, đúng 5% bị hỏng, trộn đủ loại: JSON lỗi, thiếu event_id /
  product_id / time_stamp / store_id, sai kiểu dữ liệu. Dùng seed cố định.
- Chạy qua hàm validate/transform của pipeline, ghi vào Postgres test
  (Spark local[*] nếu hàm dùng PySpark, không cần Kafka).
- Query và báo: N, rows_valid, rows_rejected, M/N %, COUNT(*) quarantine_events,
  số bad record trong fact_product_view (PHẢI = 0), số record hỏng theo từng loại lỗi.

BƯỚC 3 - [N, 2N] Idempotency / replay
- Batch N=10,000 dòng, event_id duy nhất. Chạy 2 lần cùng batch qua đường ghi thật.
- Query: SELECT COUNT(*) FROM fact_product_view;
         SELECT event_id, COUNT(*) FROM fact_product_view GROUP BY event_id HAVING COUNT(*)>1;
  Kỳ vọng COUNT=N, duplicate=0.
- Số "trước": trên DB test, tắt tạm dedupe/ON CONFLICT (không commit, dùng git stash hoặc
  env flag), chạy lại 2 lần, báo COUNT(*) (kỳ vọng 2N). Sau đó khôi phục code và xác nhận
  git diff sạch.

BƯỚC 4 - [T] Recovery time
- Chạy batch lớn trên local, kill -9 job giữa chừng, kiểm tra batch_runs.status = FAILED/STARTED.
- Replay cùng batch, đo giây từ lúc replay đến batch_runs.status='SUCCEEDED'.
- Verify COUNT(*) đúng số kỳ vọng và 0 duplicate. Lặp 3 lần, báo từng lần và trung bình.
- Verify checkpoint backfill tách khỏi live: chạy backfill (--backfill-days) với --dry-run
  và không dry-run trên DB test, xác nhận data live không đổi (so sánh COUNT trước/sau).

BƯỚC 5 - [X, Y, Z] Throughput và latency (cần Kafka + Spark local)
- docker compose up đủ stack. Bơm >= 50,000 event vào Kafka bằng producer script,
  với time_stamp = thời điểm bơm (không dùng data cũ).
- Chạy >= 5 micro-batch. Lấy rows_in, batch duration, inputRowsPerSecond,
  processedRowsPerSecond từ StreamingQueryProgress hoặc batch_runs.
- Báo: X (rows_in trung bình/batch), Y (events/s trung bình và p95), kèm cấu hình máy.
- Z (end-to-end latency) = thời điểm commit vào Postgres - time_stamp lúc bơm.
  Báo trung bình, p95, max. Nếu không đo được thì "CHƯA ĐO".

BƯỚC 6 - [L] Freshness lag và health check
- Khi pipeline đang chạy ổn định với data mới: 10 mẫu cách nhau 30s của
  SELECT EXTRACT(EPOCH FROM (NOW() - MAX(time_stamp))) AS lag_seconds FROM fact_product_view;
  Báo trung bình và max.
- Chạy health-check script thật, dán output, xác nhận nó đọc từ batch_runs thật.
- Nếu pipeline không chạy liên tục được local thì ghi "CHƯA ĐO".

BƯỚC 7 - [T_ci] Thời gian CI
- gh run list --limit 5 và gh run view <id> lấy thời gian 3 lần chạy gần nhất.
  Báo từng lần và trung bình. Chưa push hoặc chưa có workflow run thì ghi "CHƯA ĐO"
  và hướng dẫn tôi đọc từ tab Actions.

OUTPUT CUỐI CÙNG
1. Cấu hình máy.
2. Bảng: Chỉ số | Giá trị | Lệnh | Output gốc (rút gọn).
3. Danh sách "CHƯA ĐO" kèm lý do.
4. Cảnh báo: số nào yếu hoặc dễ bị hỏi vặn khi phỏng vấn.
5. Viết lại 4 bullet CV CHỈ với số đã đo thật, bỏ hẳn placeholder nào chưa đo:

 - Bullet 1: Kafka -> Spark -> Postgres, star schema, [X] events/batch, ~[Y] events/s,
   latency < [Z]s (kèm "on local Docker, <cấu hình>" nếu ghi số throughput).
 - Bullet 2: DQ validation (required-field, malformed-JSON quarantine, rejection tracking),
   N events, M malformed (M/N %), 0 bad record vào fact. KHÔNG nhắc 15/15 tests.
 - Bullet 3: Idempotent batch (staging + event_id), replay N-row batch 2 lần = 0 duplicate
   (vs 2N khi tắt dedupe), recovery T giây, live/backfill checkpoint tách riêng.
 - Bullet 4: Docker, K credentials -> 0 qua env var, GitHub Actions CI ~T phút,
   runbook + data dictionary, health check SLA 5 phút, lag ~L giây.

