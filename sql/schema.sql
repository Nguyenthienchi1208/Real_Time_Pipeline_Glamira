

DROP TABLE IF EXISTS fact_product_view CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

DROP TABLE IF EXISTS stg_fact_product_view CASCADE;
DROP TABLE IF EXISTS stg_dim_customer CASCADE;
DROP TABLE IF EXISTS stg_dim_product CASCADE;
DROP TABLE IF EXISTS stg_dim_store CASCADE;
DROP TABLE IF EXISTS stg_dim_location CASCADE;
DROP TABLE IF EXISTS stg_dim_date CASCADE;

DROP SEQUENCE IF EXISTS dim_customer_customer_sk_seq;
DROP SEQUENCE IF EXISTS dim_store_store_sk_seq;
DROP SEQUENCE IF EXISTS fact_product_view_event_id_seq;

CREATE TABLE dim_customer (
    customer_sk   VARCHAR(64)  PRIMARY KEY,
    user_id_db    VARCHAR(255),
    email_address VARCHAR(255),
    device_id     VARCHAR(255),
    user_agent    TEXT,
    resolution    VARCHAR(50)
);

CREATE TABLE dim_product (
    product_sk  VARCHAR(64)  PRIMARY KEY,
    product_id  VARCHAR(64),
    collection  VARCHAR(255),
    alloy       VARCHAR(255),
    diamond     VARCHAR(255)
);

CREATE TABLE dim_store (
    store_sk  VARCHAR(64)  PRIMARY KEY,
    store_id  VARCHAR(255) UNIQUE
);

CREATE TABLE dim_location (
    location_sk VARCHAR(64)  PRIMARY KEY,
    ip          VARCHAR(64),
    country     VARCHAR(100),
    region      VARCHAR(100),
    city        VARCHAR(100),
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION
);

CREATE TABLE dim_date (
    date_sk    BIGINT PRIMARY KEY,
    full_date  DATE,
    year       INTEGER,
    month      INTEGER,
    day        INTEGER,
    quarter    INTEGER
);

CREATE TABLE fact_product_view (
    event_id        VARCHAR(64) PRIMARY KEY,
    user_id_db      VARCHAR(255),
    product_id      VARCHAR(255),
    store_id        VARCHAR(255),
    time_stamp      TIMESTAMP,
    date_sk         BIGINT,
    customer_sk     VARCHAR(64),
    product_sk      VARCHAR(64),
    store_sk        VARCHAR(64),
    location_sk     VARCHAR(64),
    referrer_url    TEXT,
    current_url     TEXT,
    domain          VARCHAR(255),
    country_domain  VARCHAR(100),
    browser         VARCHAR(100),
    os              VARCHAR(100)
);

CREATE TABLE quarantine_events (
    batch_id        VARCHAR(64) NOT NULL,
    payload         TEXT NOT NULL,
    reason          VARCHAR(64) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);

ALTER TABLE stg_fact_product_view ADD CONSTRAINT IF NOT EXISTS uq_stg_fact_event_batch UNIQUE (batch_id, event_id);

CREATE TABLE batch_runs (
    batch_id        VARCHAR(64) PRIMARY KEY,
    status          VARCHAR(20) NOT NULL,
    rows_in         BIGINT DEFAULT 0,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    error_message   TEXT,
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_dim_customer (
    batch_id      VARCHAR(64) NOT NULL,
    customer_sk   VARCHAR(64),
    user_id_db    VARCHAR(255),
    email_address VARCHAR(255),
    device_id     VARCHAR(255),
    user_agent    TEXT,
    resolution    VARCHAR(50),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_dim_product (
    batch_id      VARCHAR(64) NOT NULL,
    product_sk    VARCHAR(64),
    product_id    VARCHAR(64),
    collection    VARCHAR(255),
    alloy         VARCHAR(255),
    diamond       VARCHAR(255),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_dim_store (
    batch_id      VARCHAR(64) NOT NULL,
    store_sk      VARCHAR(64),
    store_id      VARCHAR(255),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_dim_location (
    batch_id      VARCHAR(64) NOT NULL,
    location_sk   VARCHAR(64),
    ip            VARCHAR(64),
    country       VARCHAR(100),
    region        VARCHAR(100),
    city          VARCHAR(100),
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_dim_date (
    batch_id      VARCHAR(64) NOT NULL,
    date_sk       BIGINT,
    full_date     DATE,
    year          INTEGER,
    month         INTEGER,
    day           INTEGER,
    quarter       INTEGER,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE stg_fact_product_view (
    batch_id        VARCHAR(64) NOT NULL,
    event_id        VARCHAR(64),
    user_id_db      VARCHAR(255),
    product_id      VARCHAR(255),
    store_id        VARCHAR(255),
    time_stamp      TIMESTAMP,
    date_sk         BIGINT,
    customer_sk     VARCHAR(64),
    product_sk      VARCHAR(64),
    store_sk        VARCHAR(64),
    location_sk     VARCHAR(64),
    referrer_url    TEXT,
    current_url     TEXT,
    domain          VARCHAR(255),
    country_domain  VARCHAR(100),
    browser         VARCHAR(100),
    os              VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_product_view (date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_product_view (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_user ON fact_product_view (user_id_db);
CREATE INDEX IF NOT EXISTS idx_fact_country_domain ON fact_product_view (country_domain);
CREATE INDEX IF NOT EXISTS idx_fact_browser_os ON fact_product_view (browser, os);

CREATE INDEX IF NOT EXISTS idx_stg_fact_batch_id ON stg_fact_product_view (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_dim_customer_batch ON stg_dim_customer (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_dim_product_batch ON stg_dim_product (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_dim_store_batch ON stg_dim_store (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_dim_location_batch ON stg_dim_location (batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_dim_date_batch ON stg_dim_date (batch_id);
