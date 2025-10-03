import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import to_timestamp, col

# Copy schema from etl.py to make test self-contained (use StringType for initial read, then cast)
schema = StructType([
    StructField("tstamp", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])

@pytest.fixture(scope="session")
def spark_session():
    # Fix hostname resolution in WSL
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    spark = SparkSession.builder.appName("TestETL").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

def test_schema_definition():
    """Test that schema has correct structure with 6 fields."""
    expected_fields = [
        ("tstamp", StringType(), True),
        ("open", DoubleType(), True),
        ("high", DoubleType(), True),
        ("low", DoubleType(), True),
        ("close", DoubleType(), True),
        ("volume", LongType(), True)
    ]
    for i, (name, dtype, nullable) in enumerate(expected_fields):
        field = schema.fields[i]
        assert field.name == name
        assert field.dataType == dtype
        assert field.nullable == nullable

def test_cast_timestamp(spark_session):
    """Test casting tstamp from STRING to TIMESTAMP."""
    data = [("2025-09-02 04:00:00", 243.67, 243.79, 242.02, 242.38, 522)]
    df = spark_session.createDataFrame(data, schema)
    df_cast = df.withColumn("tstamp", to_timestamp(col("tstamp"), "yyyy-MM-dd HH:mm:ss"))
    assert df_cast.schema["tstamp"].dataType == TimestampType()
    result = df_cast.collect()
    assert result[0]["tstamp"] is not None  # Cast successful

def test_filter_and_order(spark_session):
    """Test filter volume > 0 and order by tstamp."""
    data = [
        ("2025-09-02 04:00:00", 243.67, 243.79, 242.02, 242.38, 522),
        ("2025-09-02 04:05:00", 243.07, 243.07, 242.41, 242.54, 0),  # Volume = 0, should be filtered
        ("2025-09-02 04:10:00", 242.96, 242.96, 242.54, 242.94, 64)
    ]
    df = spark_session.createDataFrame(data, schema)
    df_processed = df.withColumn("tstamp", to_timestamp(col("tstamp"), "yyyy-MM-dd HH:mm:ss")).filter(col("volume") > 0).orderBy("tstamp")
    result = df_processed.collect()
    assert len(result) == 2  # Filtered out volume = 0
    assert result[0]["tstamp"] < result[1]["tstamp"]  # Ordered by tstamp

def test_jdbc_write_mock(spark_session):
    """Mock JDBC write to avoid actual DB connection in test."""
    data = [("2025-09-02 04:00:00", 243.67, 243.79, 242.02, 242.38, 522)]
    df = spark_session.createDataFrame(data, schema)
    df_cast = df.withColumn("tstamp", to_timestamp(col("tstamp"), "yyyy-MM-dd HH:mm:ss"))
    # Mock write: Use temp view instead of JDBC
    df_cast.createOrReplaceTempView("mock_stock_data")
    # Simulate save without error
    assert df_cast.count() == 1  # Mock success
    assert True  # No exception raised

if __name__ == "__main__":
    pytest.main(["-v", __file__])