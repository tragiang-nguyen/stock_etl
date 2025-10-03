from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, LongType
from pyspark.sql.functions import to_timestamp

schema = StructType([
    StructField("tstamp", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True)
])

spark = SparkSession.builder.appName("StockETL").master("local[*]").getOrCreate()
df = spark.read.schema(schema).csv("hdfs://localhost:9000/data/cleaned_stock/*", header=False, sep=",")
df = df.withColumn("tstamp", to_timestamp("tstamp", "yyyy-MM-dd HH:mm:ss"))  # Cast STRING to TIMESTAMP
df = df.filter(df.volume > 0).orderBy("tstamp")  # Clean data

df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/airflow") \
    .option("dbtable", "stock_data").option("user", "datauser") \
    .option("password", "password").option("driver", "org.postgresql.Driver").mode("overwrite").save()
spark.stop()