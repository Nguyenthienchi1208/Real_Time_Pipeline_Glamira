# Real-Time Customer Behaviour Data Pipeline

---

## 1. Project

* Xây dựng **Real-Time Data Pipeline** xử lý dữ liệu hành vi khách hàng
* Ingest dữ liệu từ **Apache Kafka**
* Process và transform streaming data bằng **PySpark**
* Lưu dữ liệu đã xử lý vào **PostgreSQL**
* Thiết kế **Star Schema** phục vụ phân tích dữ liệu
* Xây dựng **near-real-time dashboard** bằng Streamlit
* Cung cấp dữ liệu phục vụ **Marketing, CRM và Customer Behaviour Analysis**

---

## 2. Tổng quan kiến trúc dữ liệu

```text
Customer Events
      ↓
Apache Kafka
      ↓
PySpark Structured Streaming
      ↓
Data Processing & Transformation
      ↓
PostgreSQL
      ↓
Star Schema
      ↓
Streamlit Dashboard
      ↓
Marketing / CRM Analytics
```

---

## 3. Data Pipeline

### 3.1 Data Source

* Customer behaviour events
* Dữ liệu được publish vào **Apache Kafka**
* Kafka đóng vai trò streaming platform để quản lý và truyền dữ liệu real-time

---

### 3.2 Apache Kafka

* Tiếp nhận dữ liệu customer events
* Publish events vào Kafka Topic
* Đảm nhiệm việc quản lý và kiểm soát data flow giữa source và processing layer


### 3.3 PySpark Processing

* Consume streaming data từ Kafka
* Parse dữ liệu từ Kafka message
* Data cleaning và validation
* Transform dữ liệu
* Aggregate dữ liệu phục vụ analytical use cases
* Sử dụng Spark's **parallel processing** và **task partitioning** để xử lý dữ liệu hiệu quả


### 3.4 PostgreSQL Data Warehouse

* Lưu trữ dữ liệu sau khi processing
* Thiết kế theo mô hình **Star Schema**
* Tách dữ liệu thành Fact Table và Dimension Tables
* Phục vụ analytical queries và dashboard


## 4. PostgreSQL Tables

### 4.1 Dimension Tables

* `dim_customer`
* `dim_product`
* `dim_store`
* `dim_referrer`
* `dim_date`
* `dim_location`
* `dim_device`

---

### 4.2 Fact Table

* `fact_customer_view`

Fact table lưu trữ các customer behaviour events và các foreign key liên kết tới dimension tables.

---

## 5. Công nghệ sử dụng

* Apache Kafka
* PySpark
* PostgreSQL
* Docker
* Streamlit
* Python
* SQL
* Git

---

## 6. Dashboard

### Streamlit Dashboard

Dashboard được xây dựng để theo dõi và phân tích customer behaviour theo thời gian gần real-time.

Các nội dung phân tích:

* Product Views
* Views by Country
* Referrer
* Store
* Hourly View Trends
* Browser
* Operating System


