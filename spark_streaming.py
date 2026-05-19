from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os
import time

# Khởi tạo Spark Session với thư viện Kafka 3.5.1
spark = SparkSession.builder \
    .appName("CrimeTrendMonitor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Giảm bớt log hệ thống để dễ nhìn Terminal
spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema cho dữ liệu (khớp với các cột trong CSV)
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Primary Type", StringType(), True),
    StructField("District", StringType(), True),
    StructField("Year", StringType(), True)
])

# 3. Lắng nghe luồng dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crime_topic") \
    .load()

# 4. Giải mã dữ liệu JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Xử lý tính toán: Đếm tổng số vụ án theo loại tội phạm (Primary Type)
crime_counts = parsed_df.groupBy("Primary Type").count()

# 6. Hàm tùy chỉnh để lưu kết quả đang xử lý ra file tĩnh cho Dashboard đọc
def write_to_csv(batch_df, batch_id):
    start_time = time.time() # Bắt đầu bấm giờ
    
    # Lưu file như cũ
    batch_df.toPandas().to_csv("data/crime_counts_realtime.csv", index=False)
    
    end_time = time.time() # Kết thúc bấm giờ
    processing_time = end_time - start_time
    
    print(f"⏱️ Batch {batch_id} | Số dòng: {batch_df.count()} | Thời gian xử lý: {processing_time:.4f} giây")

# Bắt đầu stream và ghi kết quả
print("🚀 Spark Streaming đang chạy và chờ dữ liệu từ Kafka...")
query = crime_counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .start()

query.awaitTermination()