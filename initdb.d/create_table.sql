CREATE DATABASE IF NOT EXISTS sample;
CREATE TABLE IF NOT EXISTS sample.shipment (
    product TEXT,
    ship_date TEXT,
    region TEXT,
    country TEXT
);
INSERT INTO sample.shipment VALUES 
    ("product 1", "2019-01-01", "SG", "Singapore"),
    ("product 2", "2019-01-02", "US", "US"),
    ("product 3", "2019-01-01", "EU", "France"),
    ("product 4", "2019-01-01", "EU", "Germany")