# Data Dictionary

## 1. Fact Table: fact_product_view

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| event_id | VARCHAR(64) | Unique event identifier | No |
| user_id_db | VARCHAR(255) | User identifier in warehouse | Yes |
| product_id | VARCHAR(255) | Product identifier | Yes |
| store_id | VARCHAR(255) | Store identifier | Yes |
| time_stamp | TIMESTAMP | Event timestamp | Yes |
| date_sk | BIGINT | Surrogate key for date dimension | Yes |
| customer_sk | VARCHAR(64) | Surrogate key for customer dim | Yes |
| product_sk | VARCHAR(64) | Surrogate key for product dim | Yes |
| store_sk | VARCHAR(64) | Surrogate key for store dim | Yes |
| location_sk | VARCHAR(64) | Surrogate key for location dim | Yes |
| referrer_url | TEXT | Referrer page | Yes |
| current_url | TEXT | Current page URL | Yes |
| domain | VARCHAR(255) | Domain value | Yes |
| country_domain | VARCHAR(100) | Country/domain mapping | Yes |
| browser | VARCHAR(100) | Browser type | Yes |
| os | VARCHAR(100) | Operating system | Yes |

## 2. Dimension Tables

### dim_customer

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| customer_sk | VARCHAR(64) | Surrogate key | No |
| user_id_db | VARCHAR(255) | Customer id | Yes |
| email_address | VARCHAR(255) | Email | Yes |
| device_id | VARCHAR(255) | Device id | Yes |
| user_agent | TEXT | User agent string | Yes |
| resolution | VARCHAR(50) | Screen resolution | Yes |

### dim_product

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| product_sk | VARCHAR(64) | Surrogate key | No |
| product_id | VARCHAR(64) | Product id | Yes |
| collection | VARCHAR(255) | Product collection | Yes |
| alloy | VARCHAR(255) | Alloy metadata | Yes |
| diamond | VARCHAR(255) | Diamond metadata | Yes |

### dim_store

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| store_sk | VARCHAR(64) | Surrogate key | No |
| store_id | VARCHAR(255) | Store id | Yes |

### dim_location

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| location_sk | VARCHAR(64) | Surrogate key | No |
| ip | VARCHAR(64) | IP address | Yes |
| country | VARCHAR(100) | Country | Yes |
| region | VARCHAR(100) | Region | Yes |
| city | VARCHAR(100) | City | Yes |
| latitude | DOUBLE PRECISION | Latitude | Yes |
| longitude | DOUBLE PRECISION | Longitude | Yes |

### dim_date

| Column | Type | Description | Nullable |
| --- | --- | --- | --- |
| date_sk | BIGINT | Date key | No |
| full_date | DATE | Full date value | Yes |
| year | INTEGER | Year | Yes |
| month | INTEGER | Month | Yes |
| day | INTEGER | Day | Yes |
| quarter | INTEGER | Quarter | Yes |

## 3. Operational Tables

### batch_runs

| Column | Type | Description |
| --- | --- | --- |
| batch_id | VARCHAR(64) | Batch identifier |
| status | VARCHAR(20) | STARTED / SUCCEEDED / FAILED |
| rows_in | BIGINT | Rows read for the batch |
| started_at | TIMESTAMP | Batch start time |
| finished_at | TIMESTAMP | Batch finish time |
| error_message | TEXT | Failure reason |
| updated_at | TIMESTAMP | Last modification time |

### quarantine_events

| Column | Type | Description |
| --- | --- | --- |
| batch_id | VARCHAR(64) | Source batch |
| payload | TEXT | Rejected raw payload |
| reason | VARCHAR(64) | Rejection reason |
| created_at | TIMESTAMP | Row inserted time |

## 4. Notes

- `event_id` is the primary dedupe key for fact records.
- Staging tables include `batch_id` for batch-level traceability.
- `quarantine_events` stores invalid payloads rejected before transformation.
- `batch_runs` is the operational control table used for monitoring and freshness checks.
