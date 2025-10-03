-- Create external table for raw stock data from HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS stock_raw (
    tstamp STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/stock_IBM_*.csv';

-- Create cleaned table from MapReduce output
CREATE EXTERNAL TABLE IF NOT EXISTS stock_cleaned (
    tstamp STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/cleaned_stock';

-- Aggregate table (AVG close GROUP BY tstamp)
CREATE TABLE IF NOT EXISTS aggregated_stock AS
SELECT
    tstamp AS `timestamp`,
    AVG(close) AS avg_close
FROM
    stock_cleaned
GROUP BY
    tstamp
ORDER BY
    tstamp;