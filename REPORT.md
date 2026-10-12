# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Quốc Việt |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602973` |
| GitHub | vietvuivui |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-00722` |

## Kết quả cuối

- `dataset_version` của bạn: `8dfe4968256f`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **727 / 116 / 176** (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Số dòng bronze khớp đúng file CSV: A có **3021** dòng, B có **3234** dòng; chạy `step 1` lần hai mọi thứ báo `already present` (không thêm dòng). Khác biệt giữa hai nguồn:

1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, gas, brake`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, throttle_pct, brake_pct`.
2. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili-giây (`1759933801509`).
3. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
4. **Bàn đạp**: A là tỉ lệ 0–1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: thiết bị B chạy nhanh hơn video **1500 ms** (khai báo trong `sources.json`), A lệch 0.

Kiểm tra trực tiếp ở bronze cho thấy:
- `speed_mps` của A có kiểu `VARCHAR` và **30 ô trống**;
- `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, WRITE 3021 dòng) dù đã nạp hai lần;
- `bronze.video_files` có **2 dòng với 2 sha256 khác nhau** (`636ba1a9…` cho A, `bfa62703…` cho B), lần nạp lặp không nhân đôi video.

Giữ mỗi nguồn một bảng và để mọi cột là chữ vì bronze là **bản ghi gốc bất biến**: nếu ép kiểu ngay, giá trị lỗi (ô tốc độ trống, chữ lạ) sẽ thành NULL hoặc làm job sập, và ta mất bằng chứng dữ liệu thô trông thế nào. Gộp ngay thì phải chọn một schema khi chưa biết B "drift" ra sao. Để thô, khi luật làm sạch thay đổi ta chỉ cần chạy lại silver từ bronze, không phải ingest lại; các cột `_source_file`, `_batch_id`, `_row_no` giúp truy ngược từng dòng về file gốc.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_a` và `stg_telemetry_b` đổi tên cột về một schema chung, sau đó `silver_telemetry_classified` gộp hai nguồn (`union all by name`). Riêng với B:
- tốc độ `spd_kmh / 3.6` để ra m/s;
- bàn đạp `throttle_pct / 100`, `brake_pct / 100` để ra tỉ lệ 0–1;
- thời gian `to_timestamp((timestamp_ms - offset_ms) / 1000)`, trong đó `offset_ms = 1500` lấy từ `bronze.video_files` (khai báo trong `sources.json`). Tất cả được đưa về UTC.

Bằng chứng ở bước 2:
- mẫu đầu tiên của B là `2025-10-08 14:30:00.009`, chỉ cách thời điểm bắt đầu video (`14:30:00`) 9 ms; verify kiểm tra "within 0.5 s" và báo PASS;
- tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), đều hợp lý; mọi tốc độ nằm trong 0..70 m/s.

Bảng quarantine của tôi:

| drive | duplicate | speed_missing | speed_out_of_range | tổng cách ly | sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Phép cộng sạch + cách ly = nhận về khớp đúng (2968 + 53 = 3021, 3181 + 53 = 3234), nên **không dòng nào biến mất**. Dòng lỗi vào `telemetry_rejects` kèm `reject_reason` thay vì bị xoá vì bốn lý do:
- có thể **đếm và theo dõi** chất lượng nguồn theo thời gian, ví dụ cảm biến B bỗng thiếu nhiều tốc độ;
- **gỡ lỗi ngược** về đúng dòng CSV nhờ `_row_no`;
- nếu luật sai (ví dụ ngưỡng 70 m/s quá chặt), có thể **xử lý lại** các dòng đó;
- xoá im lặng che mất sự cố thượng nguồn và làm số liệu không đối soát được.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng hở giữa hai mẫu > 1000 ms) có `severity: warn` vì cảm biến **mất tín hiệu là chuyện có thật** (đường hầm, mất kết nối). Dữ liệu vẫn đúng, chỉ thưa hơn, nên pipeline báo nhưng không dừng. Ở bước 2 của tôi, test này ra `WARN=1`, còn pipeline vẫn chạy tiếp (`PASS=6 WARN=1 ERROR=0`).

`in_range` (tốc độ 0..70 m/s) là `error` vì giá trị ngoài khoảng đó **vô lý về vật lý** (70 m/s ≈ 252 km/h). Nếu chúng lọt vào silver, mọi phân tích và nhãn ở phía sau sẽ sai, nên phải chặn ngay.

Thí nghiệm `break-gate` (`break_gate.json`):
- nới `max_speed_mps` từ 70 lên **1000**, nên luật làm sạch không còn loại các tốc độ vô lý;
- `dbt test` đổi thành `PASS=5 WARN=1 ERROR=1`, trong đó test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**, đúng bằng 3 + 3 dòng `speed_out_of_range` của hai drive;
- `gate_failed: true`;
- khôi phục luật gốc thì quay về `PASS=6 WARN=1 ERROR=0`.

Ý nghĩa: cổng chất lượng là **lớp phòng thủ thứ hai, độc lập với code biến đổi**. Khi một luật làm sạch bị nới, dù cố ý hay do bug, test vẫn giữ hợp đồng dữ liệu và chặn dữ liệu xấu trước khi xuống gold. Trong Airflow, task `dbt_test` đỏ thì các task sau không chạy.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json`, chạy lại partition `trip-00003` (60 khung):
- **Cách của pipeline** (Delta `overwrite` với predicate `trip_id = 'trip-00003'`): trước có **1246** dòng, dấu vân tay `6c7480dfde7e`. Sau **2 lần chạy lại** vẫn **1246** dòng, dấu vân tay `6c7480dfde7e`, nghĩa là bảng không đổi cả số dòng lẫn nội dung (`unchanged: true`).
- **Cách sai** (`append`, chạy 2 lần cho cùng trip): ra **120** dòng cho một trip chỉ có 60 khung, tức dữ liệu bị nhân đôi.

Delta `overwrite` theo partition an toàn hơn "xoá rồi ghi lại" vì nó là **một commit nguyên tử** trong `_delta_log`: phần xoá dữ liệu cũ của trip và phần thêm dữ liệu mới nằm chung một version. Nếu job sập giữa chừng, commit chưa được ghi nên người đọc vẫn thấy version cũ đầy đủ. Các file Parquet dở dang chỉ là file "mồ côi", không ai đọc tới.

Với "xoá rồi ghi" là hai thao tác tách rời: sập sau bước xoá thì trip **mất dữ liệu**, còn người đọc ở giữa hai bước thấy bảng thiếu. Ghi đè chỉ đúng predicate cũng giúp chạy lại **một trip** mà không đụng 25 trip còn lại, đúng điều Airflow cần khi một task `silver_frames` lỗi. Bonus: lịch sử version (`cli history silver.frames`) cho phép time travel nếu cần quay lại.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

Hai loại khung khác nhau ở độ sáng trung bình (luma):
- **Khung đen**: luma < `dark_luma = 12`. Đây là ảnh hỏng (camera tắt, lỗi giải mã) và không chứa thông tin, nên bị **loại** khỏi gold.
- **Khung tối**: 12 ≤ luma < `dark_soft = 40`. Đây là cảnh thật nhưng thiếu sáng (đường hầm, bóng râm, chiều tối), nên chỉ được **gắn cờ `is_dark` và giữ lại**.

Số liệu của tôi:
- drive A: 6 khung đen, 4 khung tối giữ lại;
- drive B: 4 khung đen, **61** khung tối giữ lại;
- verify xác nhận "low-light flagged but not black 10/10".

Tôi đồng ý với cách xử lý này: bỏ khung tối sẽ làm mô hình **mù ở điều kiện thiếu sáng**, vốn là lúc lái xe nguy hiểm nhất. Giữ kèm cờ cho phép cân bằng lại hoặc đánh giá riêng sau này.

`threshold.json` (số khung bị coi là trùng với khung liền trước, theo số bit dHash được phép khác):

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn **ngưỡng 2** (ngưỡng đang dùng), vì ba lý do:
- ngưỡng 0 chỉ bắt khung giống hệt từng bit, nên bỏ sót camera đứng hình có nhiễu nén;
- ngưỡng 6 loại **363/602 (60 %)** khung của A và 244/644 của B, tức xoá cả những khung khác nhau thật;
- ngưỡng 2 bắt được camera đứng hình 6 giây ở A (verify: 11/12).

Ngưỡng phụ thuộc tốc độ xe. Với mẫu 2 khung/giây, khi xe chạy chậm hoặc đứng đèn đỏ, hai khung liền nhau gần như y hệt, nên số khung trùng tăng mạnh. Đó là lý do drive A (trip test chỉ 11 km/h trung bình) có 149 khung trùng, so với 53 ở B (đa số nhanh, train 79 km/h). Khi xe chạy nhanh, cảnh đổi nhiều nên một ngưỡng cao vẫn ít khi loại nhầm. Một cách hợp lý hơn là ngưỡng thích ứng theo `speed_mps`, hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: có **1019** khung đáng dùng, thuộc **26** trip.
- **Chia theo khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** nằm ở nhiều hơn một split, và cả **26 trip** đều có khung ở cả train lẫn test.
- **Chia theo trip** (gold của pipeline): **0** trip bị chia cắt. Mỗi trip nằm trọn trong một split: 18 trip train, 4 val, 4 test. Test dbt và verify "no trip is in two splits" đều PASS.

Điểm kiểm thử đẹp giả tạo vì các khung trong một trip, lấy cách nhau 0,5 giây, **gần như giống hệt**: cùng con đường, cùng ánh sáng, cùng thời tiết, cùng xe phía trước. Khi chia theo khung, khung ở test thường có "anh em sinh đôi" ở train. Mô hình chỉ cần **nhớ** cảnh là đoán đúng, không cần **tổng quát hoá**. Điểm test vì thế đo khả năng nhớ, và sẽ sụp khi xe chạy trên con đường mới. Chia theo trip (theo cảnh) mới giống thực tế triển khai: test toàn là cảnh mô hình chưa từng thấy.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version = sha256(danh sách "frame_id|split" đã sắp xếp)[:12]`, của tôi là **`8dfe4968256f`**. Đây là **băm nội dung** vì version phải trả lời được câu hỏi "hai dataset này có giống nhau không":
- cùng khung, cùng split thì cùng version, dù build lúc nào;
- chỉ cần một khung đổi split hoặc bị loại thêm thì version đổi.

Ngày giờ hay số đếm thì đổi ở mỗi lần chạy, kể cả khi nội dung y hệt (vô nghĩa cho việc so sánh), và hai máy khác nhau có thể sinh cùng số đếm cho nội dung khác nhau.

Hai lần chạy của tôi: `step all --reset` (từ lake trống) ra `8dfe4968256f`, sau đó chạy lại `step 4` cũng ra `8dfe4968256f`. Kết quả hoàn toàn tái lập được.

Version của tôi khác của bạn bè dù cùng dữ liệu vì gold sắp trip bằng `md5(trip_id || LAB_STUDENT_ID)` rồi cắt 70/15/15 theo thứ tự đó (phân tầng theo drive). MSSV `2A202602973` là "muối" quyết định trip nào vào train/val/test. Với tôi, kết quả là 727/116/176 khung (18/4/4 trip); test của drive A là 2 trip chậm (11 km/h). MSSV khác thì split khác, nên danh sách `frame_id|split` khác và hash khác. Giảng viên chạy lại với MSSV của tôi phải ra đúng `8dfe4968256f`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`manifest.json` tại `lake/gold/datasets/8dfe4968256f/manifest.json`, phần `lineage` (lần chạy cuối):

```
bronze.raw_telemetry_a: 0, bronze.raw_telemetry_b: 0, bronze.trips_raw: 0, bronze.video_files: 1,
silver.frames: 25, silver.telemetry: 0, silver.frames_flagged: 0, silver.frame_telemetry: 0,
input_video_sha256: drive_a = 636ba1a98dfc…e7e9aa, drive_b = bfa62703f645…bee9fdc
```

Trả lời "dataset này tạo từ dữ liệu nào": từ hai video có sha256 như trên (`drive_a.mp4`, `drive_b.mp4`) và telemetry ở **version 0** của `bronze.raw_telemetry_a/b`. Dữ liệu đi qua silver ở các version ghi trên. `silver.frames` ở version 25 vì mỗi trip ghi một partition, tức 26 commit (version 0 đến 25), khi chạy từ lake trống. Ở lần chạy trước, sau thí nghiệm idempotent, con số này là 53, nhưng `manifest_sha256` vẫn y hệt: version Delta đếm số lần ghi, còn nội dung không đổi. Kết quả là 1019 khung, `manifest_sha256 = b6a92df4…3a002`.

Ba tháng sau, để tái tạo đúng dataset này cần:
1. sha256 của video đầu vào, để chắc đúng file gốc;
2. version các bảng Delta, để **time travel** đọc lại đúng snapshot (`versionAsOf`);
3. phần `rules` (loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`; split theo trip, phân tầng theo drive, thứ tự md5) cùng các ngưỡng trong `dbt_project.yml`, lấy theo commit git của mã;
4. MSSV (muối của split).

Chạy lại xong, so `dataset_version` và `manifest_sha256` để xác nhận trùng khớp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip của tôi là `trip-00722`, tính bằng `202602973 % 26 = 1`. MSSV có chữ `A` nên tôi lấy phần số. Tôi đặt `LAB_FAIL_ONCE=trip-00722`, trigger `day09_reset`, rồi trigger `day09_video_pipeline` (run `manual__2026-10-12T10:25:58`).

- **1 task đỏ**: `silver_frames` map_index 1. Log ghi `RuntimeError: simulated crash while extracting trip-00722; run it again` (ảnh `crash_log.png`).
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` (ảnh `crash_graph_red.png`).
- **29 task vẫn xanh**: 3 task bronze, `trip_commands` và 25 task `silver_frames` của các trip khác. Các task này **không chạy lại**.
- Tôi **chỉ clear task đỏ** (kèm các task phía sau đang `upstream_failed`, tương đương nút "Clear task" với lựa chọn Downstream và Only failed). Tổng cộng 8 task chạy lại, và DAG chuyển **xanh 37/37** (ảnh `crash_rerun_green.png`).

Dữ liệu **không nhân đôi**:
- `trip-00722` có **60 dòng, 60 `frame_id` riêng biệt** trong `silver.frames`;
- tổng `silver.frames` vẫn là **1246** dòng.

Lý do là lần chạy lại ghi đè đúng partition `trip_id` (xem câu 4). `verify` vẫn **27/27**, `dataset_version` vẫn là **`8dfe4968256f`**: nội dung giống hệt nên hash giống hệt. Đây chính là lợi ích của việc mỗi trip là một task idempotent: lỗi ở một node chỉ tốn chạy lại 1/26 phần việc trích khung.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Hiện tại 2 drive khoảng 10 phút video cho ra 1246 khung, và cả pipeline mất khoảng 2 phút trên một máy. Với 500 xe/ngày, mỗi xe vài giờ, lượng dữ liệu gấp khoảng 10⁴–10⁵ lần. Ba chỗ vỡ đầu tiên:

1. **Trích khung bằng FFmpeg trong worker Airflow** (`silver_frames`, mỗi trip một task trên cùng container). CPU và đĩa của một máy không kham nổi hàng nghìn giờ video. Airflow cũng không phải nơi chạy compute nặng: hàng chục nghìn mapped task sẽ làm nghẽn scheduler. **Thay bằng**: xử lý video phân tán (Spark/Ray, hoặc job Kubernetes tự co giãn, có GPU decode NVDEC). Airflow chỉ còn vai trò điều phối, mỗi task là một batch hoặc một ngày, không phải một trip (slide 12, 23).
2. **DuckDB + dbt chạy trong một tiến trình** cho silver/gold. DuckDB là engine một máy, và join telemetry–khung hình theo event time trên hàng tỷ dòng sẽ vượt RAM. **Thay bằng**: engine phân tán (Spark/Trino/Databricks SQL) đọc thẳng các bảng Delta, giữ nguyên dbt làm lớp mô hình và test. Partition theo `date/vehicle_id` và xử lý **incremental**, chỉ partition mới, thay vì rebuild toàn bộ (slide 22).
3. **Ingest theo lô từ file CSV/mp4 đặt sẵn** cùng warehouse Postgres một node. 500 xe đổ dữ liệu liên tục cần ingest dạng **streaming** (Kafka/Kinesis vào bronze, có schema registry để bắt drift như nguồn B). Postgres một node cũng không chứa nổi `mart_frame_gallery` hàng chục triệu dòng; thay bằng warehouse cột (BigQuery/Snowflake/ClickHouse), hoặc query trực tiếp lakehouse (slide 12, 23).

Thêm vào đó, `dataset_version` hiện băm toàn bộ danh sách khung mỗi lần. Ở quy mô lớn nên version theo snapshot Delta cùng manifest phân mảnh. Object storage MinIO một node cũng phải chuyển sang S3/GCS.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm vào dashboard câu hỏi **"[MSSV 2A202602973] Tỷ lệ khung bị loại theo trip"** (biểu đồ cột, nằm đầu dashboard):

```sql
select trip_id, drive_id, split, avg_speed_kmh, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_bi_loai
from marts.mart_trip_summary
order by pct_bi_loai desc
```

Kết quả, 4 trip bị loại nhiều nhất:

| trip | drive | split | tốc độ TB (km/h) | khung | giữ lại | % bị loại |
|---|---|---|---|---|---|---|
| trip-04622 | A | train | 67.1 | 60 | 26 | 56.7 |
| trip-03904 | A | val | 46.1 | 22 | 11 | 50.0 |
| trip-02086 | A | train | 17.6 | 60 | 32 | 46.7 |
| trip-08888 | A | test | 6.5 | 60 | 36 | 40.0 |

Đọc biểu đồ:
- Cả 8 trip bị loại nhiều nhất đều thuộc drive A (6 trip) hoặc là trip B chạy nhanh.
- Truy ngược bằng `silver_frames_flagged`, gần như toàn bộ khung bị loại ở 4 trip trên là **khung trùng** (dHash), không phải đen hay mờ. Ví dụ trip-04622 có 34 khung trùng, trip-02086 có 28, trip-08888 có 24.
- Với trip chậm (6–18 km/h) thì điều này hợp lý: xe gần như đứng yên.
- Đáng chú ý, ở split test, trip-08888 (6.5 km/h) mất 40 % khung, nên test set của drive A vừa ít vừa thiên về cảnh chạy chậm.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Bất ngờ nhất**: trip-04622 chạy trung bình **67 km/h** mà vẫn mất **34/60** khung vì "trùng". Một phần là đoạn camera đứng hình 6 giây được cấy vào drive A. Phần còn lại cho thấy dHash 64 bit (ảnh thu nhỏ còn 9×8) **không nhạy với chuyển động trên đường cao tốc**, nơi cảnh vật đồng đều (bầu trời, mặt đường). Ngưỡng 2 bit có thể đang loại nhầm khung khác nhau thật.
- **Chưa rõ**: vì sao mỗi drive đều có đúng 7 khung "mất đồng bộ" (> 150 ms). Tôi đoán đó là các khoảng hở telemetry mà test `max_gap_ms` báo WARN, nhưng chưa kiểm chứng từng khung.
- **Lỗi pipeline chưa bắt**:
  - Tốc độ **trong khoảng 0..70 m/s nhưng nhảy phi vật lý** giữa hai mẫu cách nhau 0,1 giây (ví dụ 10 → 60 m/s) vẫn lọt vào silver. Pipeline chưa có kiểm tra gia tốc hay tính liên tục, và cũng không đối chiếu tốc độ với quãng đường GPS.
  - Khung trùng chỉ so với **khung liền trước**, nên camera đứng hình xen kẽ (khung A, B, A, B) sẽ không bị phát hiện.
