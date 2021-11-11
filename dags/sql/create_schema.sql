CREATE SCHEMA IF NOT EXISTS bootcampdb;
CREATE TABLE IF NOT EXISTS bootcampdb.user_purchase (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(20)
            );