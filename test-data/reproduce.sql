CREATE TABLE iceberg_ctl.nyc.taxis
(
  vendor_id bigint,
  trip_id bigint,
  trip_distance float,
  fare_amount double,
  store_and_fwd_flag string
)
PARTITIONED BY (vendor_id);

-- force the format to be v2
ALTER TABLE iceberg_ctl.nyc.taxis SET TBLPROPERTIES ('format-version' = '2');

-- insert with data
INSERT INTO iceberg_ctl.nyc.taxis
VALUES (1, 1000371, 1.8, 15.32, 'N'), (2, 1000372, 2.5, 22.15, 'N'), (2, 1000373, 0.9, 9.01, 'N'), (1, 1000374, 8.4, 42.13, 'Y');

-- add columns to make a schema evolution
ALTER TABLE iceberg_ctl.nyc.taxis ADD COLUMNS ( comment string );

-- overwrites
INSERT OVERWRITE iceberg_ctl.nyc.taxis
VALUES (1, 1000371, 1.8, 15.32, 'N', '1st vendor'), (2, 1000372, 2.5, 22.15, 'N', '2nd vendor'), (2, 1000373, 0.9, 9.01, 'N', '2nd vendor'), (1, 1000374, 8.4, 42.13, 'Y', '1st vendor');
