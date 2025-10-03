from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Khởi tạo SparkSession với kết nối Hive
spark = SparkSession.builder \
    .appName("Stock Clean with PySpark") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .config("spark.eventLog.enabled", "false") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS
df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/data/stock/stock_*.csv")

# Làm sạch dữ liệu: loại bỏ hàng có close NULL
df_clean = df.filter(df.close.isNotNull())  # Sử dụng "close" thay vì "_c4"

# Tính trung bình giá đóng cửa theo timestamp
result_df = df_clean.groupBy("timestamp").agg(avg("close").alias("avg_close"))  # Sử dụng "timestamp" thay vì "_c0"

# Lưu kết quả vào HDFS
result_df.write.csv("hdfs://localhost:9000/data/cleaned_stock", mode="overwrite", sep="\t")

# (Tùy chọn) Lưu vào Hive
result_df.write.mode("overwrite").saveAsTable("stock")

# Dừng SparkSession
spark.stop()