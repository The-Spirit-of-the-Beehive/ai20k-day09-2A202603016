# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Đỗ Hoàng Quân` |
| MSSV (đúng bằng `2A202603016` trong `.env`) | `<điền>` |
| GitHub | `The-Spirit-of-the-Beehive` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

**1. Các khác biệt giữa Telemetry A và Telemetry B quan sát được từ Bronze:**
- **Thời gian (Timestamp):** Nguồn A dùng cột `ts_utc` định dạng chuỗi ISO-8601 UTC (ví dụ: `2025-10-07T08:00:00.014Z`), trong khi nguồn B dùng cột `timestamp_ms` dạng số nguyên epoch mili-giây (ví dụ: `1759933801509`).
- **Tốc độ (Speed):** Nguồn A dùng cột `speed_mps` với đơn vị mét/giây (`m/s`), trong khi nguồn B dùng cột `spd_kmh` với đơn vị kilômét/giờ (`km/h`).
- **Tọa độ GPS & Độ cao:** Nguồn A đặt tên cột rút gọn (`lat`, `lon`, `alt_m`), nguồn B đặt tên đầy đủ (`latitude`, `longitude`, `altitude`).
- **Góc hướng & Gia tốc:** Nguồn A dùng `heading_deg`, `acc_x`, `acc_y`; nguồn B rút gọn thành `hdg`, `ax`, `ay`.
- **Chân ga/phanh:** Nguồn A dùng `gas`, `brake` (giá trị chuẩn hóa thập phân, ví dụ `0.028`), nguồn B dùng `throttle_pct`, `brake_pct` (theo phần trăm, ví dụ `2.0`).
- **Độ lệch đồng hồ:** Video `drive_b` có `telemetry_clock_offset_ms = 1500` ms (lệch 1.5 giây so với video), trong khi `drive_a` có offset bằng `0`.

**2. Vì sao pipeline giữ mỗi nguồn một bảng Bronze và để mọi cột ở dạng chữ:**
- **Mỗi nguồn một bảng riêng:** Do hai nguồn bị **Schema Drift** nghiêm trọng (khác tên cột, khác đơn vị đo, khác cấu trúc timestamp), việc ép chung vào một bảng ngay từ Bronze sẽ làm mất tính toàn vẹn hoặc sinh ra bảng chứa quá nhiều cột thưa (sparse columns) không nhất quán.
- **Để mọi cột ở dạng chữ (string) và không ép kiểu ngay:** Đây là nguyên tắc **Raw Fidelity** của Medallion Architecture. Tầng Bronze có nhiệm vụ lưu giữ nguyên trạng 100% dữ liệu gốc để phục vụ audit/lineage. Nếu ép kiểu (cast sang float, timestamp) ngay tại Bronze, các ô rỗng, giá trị dị biệt hay dòng lỗi format sẽ làm crash quá trình ingest hoặc bị biến thành `NULL` âm thầm (silent data corruption). Việc làm sạch, ép kiểu và cách ly dòng lỗi vào bảng Quarantine sẽ được dời sang tầng Silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**1. Cách pipeline đưa hai nguồn về một schema:**
- **Đồng bộ tên cột:** Chuyển các cột khác biệt của B về chuẩn chung: `spd_kmh` → `speed_mps`, `hdg` → `heading_deg`, `latitude`/`longitude` → `lat`/`lon`, `altitude` → `alt_m`.
- **Chuẩn hóa đơn vị và kiểu thời gian:** Tốc độ của B được chia cho 3.6 (`spd_kmh / 3.6`) để chuyển từ km/h sang m/s; thời gian epoch `timestamp_ms` được parse sang định dạng timestamp UTC.
- **Trừ độ lệch đồng hồ (Clock Offset):** Cột thời gian của `drive_b` được trừ đi 1500 ms (1.5 giây) theo đúng cấu hình khai báo trong `sources.json`.

**2. Bằng chứng kết quả cho thấy đã đúng:**
- Thời điểm bắt đầu của telemetry B sau khi trừ offset là `2025-10-08 14:30:00.009000+00:00`, trùng khớp hoàn toàn (lệch dưới 0.5s) với mốc bắt đầu của video `drive_b` (`14:30:00`).
- Tốc độ tối đa sau khi lọc sạch là `71.0 km/h` (drive_a) và `100.0 km/h` (drive_b), đều nằm trong ngưỡng vật lý thực tế hợp lý (< 130 km/h).
- Tổng số dòng sạch và dòng cách ly khớp chính xác 100% với dữ liệu thô ban đầu:
  + Drive A: 2968 (sạch) + 53 (cách ly) = 3021 dòng thô.
  + Drive B: 3181 (sạch) + 53 (cách ly) = 3234 dòng thô.

**3. Bảng Quarantine (Dòng bị cách ly theo từng drive và lý do):**

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |
*(Tổng cộng có 106 dòng bị cách ly)*

**4. Vì sao dòng lỗi vào `rejects` thay vì bị xoá:**
- **Khả năng kiểm toán (Auditability) & Lineage:** Giữ lại toàn bộ dữ liệu lỗi kèm lý do giúp đội ngũ kỹ sư biết chính xác dữ liệu thất thoát ở đâu mà không gặp hiện tượng "mất dữ liệu âm thầm" (silent drop).
- **Giám sát phần cứng cảm biến:** Theo dõi bảng `rejects` giúp phát hiện thiết bị trên xe bị lỗi (ví dụ: cảm biến tốc độ mất tín hiệu định kỳ hoặc bắn ra giá trị dị biệt 250 m/s ~ 900 km/h như thấy trong truy vấn CLI).
- **Khả năng tái xử lý (Reprocessing):** Nếu sau này có thuật toán nội suy (interpolation) tốt hơn để khôi phục các dòng `speed_missing`, ta có thể chạy lại từ bảng `rejects` mà không cần thu thập lại dữ liệu gốc.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**1. Vì sao `max_gap_ms` là `warn` còn `in_range` là `error`:**
- **`max_gap_ms` đặt `warn`:** Trong thực tế vận hành xe tự lái, việc tín hiệu cảm biến bị ngắt quãng nhẹ (khi xe đi qua hầm, khuất bóng cây) là hiện tượng vật lý bình thường. Test này chỉ đóng vai trò cảnh báo chất lượng đường truyền (SLA), không phá vỡ cấu trúc dữ liệu downstream nên không được dừng toàn bộ pipeline.
- **`in_range` đặt `error`:** Đây là quy chuẩn vật lý bắt buộc (tốc độ xe phải trong khoảng 0..70 m/s). Một giá trị vô lý như 250 m/s (~900 km/h) nếu lọt vào tầng Gold sẽ làm sai lệch nghiêm trọng trọng số mô hình AI hoặc gây nổ gradient trong quá trình huấn luyện. Do đó, đây là vi phạm chất lượng nghiêm trọng, bắt buộc cổng chất lượng phải chặn đứng pipeline (`ERROR`).

**2. Kết quả thí nghiệm `break_gate.json`:**
- Khi nới luật làm sạch tốc độ lên 1000 m/s, các giá trị dị biệt đã lọt vào tầng Silver và ngay lập tức bị Cổng chất lượng dbt bắt được: `PASS=5 WARN=1 ERROR=1 SKIP=0`.
- Test chặn lỗi là `in_range_silver_telemetry_speed_mps__70__0`, bắt chính xác **6 dòng** vượt ngưỡng (3 dòng từ drive_a và 3 dòng từ drive_b).
- Khi script khôi phục lại luật gốc, test trở lại trạng thái an toàn: `PASS=6 WARN=1 ERROR=0 SKIP=0`.

**3. Vai trò của Cổng chất lượng (Quality Gate):**
- Đóng vai trò như một "cầu dao tự động" (fail-safe circuit breaker), đảm bảo rằng dù mã nguồn làm sạch bị sửa sai hay có dữ liệu ngoại lai lọt qua lớp biến đổi, dữ liệu rác vẫn bị chặn đứng trước khi đi vào các tầng phân tích và huấn luyện downstream.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**1. Dựa vào `idempotency.json`:**
- **Chạy lại với Delta Overwrite (chuẩn pipeline):**
  + Trước khi chạy lại: 1246 dòng, mã vân tay (fingerprint) là `6c7480dfde7e`.
  + Sau 2 lần chạy lại partition `trip-00003`: vẫn đúng **1246 dòng**, mã vân tay giữ nguyên `6c7480dfde7e` (**KHÔNG ĐỔI**).
- **Mô phỏng kiểu ghi `append` sai lầm:**
  + Đối với một trip có 60 khung hình, sau khi chạy lại 2 lần với chế độ append, số dòng tăng vọt lên **120 dòng** (**NHÂN ĐÔI - 2x**).

**2. Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại (`DELETE` then `INSERT`):**
- **Tính nguyên tử (ACID Atomicity):** Delta Lake sử dụng cơ chế transaction log (`_delta_log`). Khi thực hiện `overwrite` theo phân vùng (`trip_id`), các tệp Parquet mới được ghi ra trước; chỉ khi quá trình ghi hoàn tất 100%, commit mới được tráo đổi nguyên tử (atomic commit).
- **An toàn khi có sự cố sập giữa chừng:** Nếu hệ thống bị sập nguồn hoặc lỗi mạng giữa quá trình thực thi:
  + Với `DELETE` rồi `INSERT`: Dữ liệu cũ đã bị xóa nhưng dữ liệu mới chưa kịp ghi xong, dẫn đến tình trạng mất dữ liệu vĩnh viễn hoặc bảng rơi vào trạng thái rỗng/nửa vời (partial state).
  + Với Delta `overwrite`: Transaction sẽ tự động bị rollback/bỏ qua, partition cũ vẫn còn nguyên vẹn 100% ở phiên bản commit trước đó, đảm bảo tính toàn vẹn dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

**1. Khác biệt giữa khung hình đen và khung hình tối:**
- **Khung hình đen (`den`):** Xuất hiện do lỗi phần cứng camera, nắp che ống kính hoặc mất tín hiệu video hoàn toàn (giá trị pixel xấp xỉ 0). Đây là khung hình rác, không mang thông tin thị giác nên **bị loại bỏ** khi chuyển lên tầng Gold.
- **Khung hình tối (`toi_giu_lai`):** Xuất hiện khi xe đi vào môi trường thực tế thiếu sáng như đường hầm, bóng râm hay ban đêm (như ở drive B có tới 61 khung). Đây là **dữ liệu cạnh (edge case) rất giá trị** giúp mô hình AI học cách xử lý trong điều kiện ánh sáng yếu, do đó pipeline **gắn cờ nhận diện nhưng GIỮ LẠI**.
- **Quan điểm:** Hoàn toàn đồng ý với cách xử lý này vì việc phân loại giúp làm sạch dữ liệu rác (đen) mà không làm mất đi các trường hợp biên quan trọng (tối) của bài toán xe tự lái.

**2. Bảng khảo sát ngưỡng khung trùng dHash (từ `threshold.json`):**

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**3. Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ xe:**
- **Chọn ngưỡng: t = 2 (ngưỡng hiện tại của pipeline):**
  + Nếu chọn **t = 0 (quá khắt khe)**: Chỉ bắt được 61 khung ở A và 8 ở B. Khi xe dừng đèn đỏ, nhiễu hạt cảm biến camera hoặc độ rung xe vẫn làm sai lệch 1-2 bit dHash, dẫn đến bỏ sót các khung hình thực chất là trùng lặp.
  + Nếu chọn **t = 6 (quá lỏng)**: Loại tới 363 khung ở A và 244 khung ở B (chiếm hơn 50% dữ liệu). Nhiều khung hình khi xe lăn bánh chậm hoặc đánh lái góc nhỏ sẽ bị nhận nhầm là trùng lặp và bị xóa oan, làm mất thông tin chuyển động liên tục.
  + **Ngưỡng t = 2** là điểm cân bằng lý tưởng (sweet spot): vừa triệt tiêu được rung lắc/nhiễu hạt khi đứng yên, vừa giữ được sự thay đổi cảnh khi xe di chuyển.
- **Sự phụ thuộc vào tốc độ xe:**
  + Khi xe chạy nhanh: Bối cảnh cảnh quan thay đổi lớn sau mỗi 0.5s (2 fps), khoảng cách Hamming dHash giữa các frame liền kề rất lớn nên ít bị dính ngưỡng trùng.
  + Khi xe chạy chậm hoặc dừng chờ đèn đỏ: Bối cảnh gần như đứng yên, dHash biến thiên rất nhỏ (0..2 bit). Ngưỡng dHash càng lớn thì số lượng khung bị loại ở các đoạn xe đi chậm càng nhiều.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
