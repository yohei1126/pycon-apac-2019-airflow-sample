#!/bin/bash
psql -U airflow -d sample << "EOSQL"
CREATE TABLE shipment (
    product TEXT,
    ship_date TEXT,
    region TEXT,
    country TEXT
);
INSERT INTO shipment (product, ship_date, region, country) VALUES 
    ('product 1', '2019-02-01', 'sg', 'Singapore'),
    ('product 2', '2019-02-02', 'sg', 'Singapore'),
    ('product 3', '2019-02-03', 'sg', 'Singapore'),
    ('product 4', '2019-02-04', 'sg', 'Singapore'),
    ('product 5', '2019-02-05', 'sg', 'Singapore'),
    ('product 1', '2019-02-01', 'us', 'US'),
    ('product 2', '2019-02-02', 'us', 'US'),
    ('product 3', '2019-02-03', 'us', 'US'),
    ('product 4', '2019-02-04', 'us', 'US'),
    ('product 5', '2019-02-05', 'us', 'US'),
    ('product 1', '2019-02-01', 'eu', 'France'),
    ('product 2', '2019-02-02', 'eu', 'France'),
    ('product 3', '2019-02-03', 'eu', 'Germany'),
    ('product 4', '2019-02-04', 'eu', 'Germany'),
    ('product 5', '2019-02-05', 'eu', 'Germany');
EOSQL