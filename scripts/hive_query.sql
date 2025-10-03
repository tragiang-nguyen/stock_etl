-- Drop tables if exist
DROP TABLE IF EXISTS stock_cleaned_external;
DROP TABLE IF EXISTS stock_cleaned;

-- Tạo external table để read HDFS file với schema đúng
CREATE EXTERNAL TABLE stock_cleaned_external (
  tstamp STRING,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data/cleaned_stock';

-- Tạo internal table
CREATE TABLE stock_cleaned (
  tstamp STRING,
  open DOUBLE,
  high DOUBLE,
  low DOUBLE,
  close DOUBLE,
  volume BIGINT
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Insert from external to internal
INSERT OVERWRITE TABLE stock_cleaned
SELECT * FROM stock_cleaned_external;

-- Aggregate
INSERT OVERWRITE DIRECTORY 'hdfs://localhost:9000/data/aggregated_stock'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT tstamp, AVG(close) FROM stock_cleaned GROUP BY tstamp;