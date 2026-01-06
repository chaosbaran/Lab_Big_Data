from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import base64
import cv2
import numpy as np
import os
from background_remover import remove_background

def process_batch(batch_df, batch_id):
    # Lấy dữ liệu về driver để xử lý
    data_list = batch_df.collect()
    
    if not data_list:
        return

    print(f"Đang xử lý Batch {batch_id}: {len(data_list)} ảnh")
    
    # Tạo thư mục output nếu chưa có
    if not os.path.exists("output"):
        os.makedirs("output")

    for row in data_list:
        try:
            timestamp = row.timestamp
            b64_string = row.image
            
            if b64_string:
                # Giải mã Base64 -> Image Bytes
                img_bytes = base64.b64decode(b64_string)
                
                # Bytes -> Numpy Array -> Ảnh OpenCV
                np_arr = np.frombuffer(img_bytes, dtype=np.uint8)
                frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                
                # Kiểm tra xem frame có giải mã thành công không
                if frame is not None:
                    result_frame = remove_background(frame)
                    
                    # Lưu ảnh kết quả
                    file_name = f"output/frame_{timestamp}.jpg"
                    cv2.imwrite(file_name, result_frame)
                    print(f"Đã lưu: {file_name}")
                else:
                    print(f"Cảnh báo: Không thể decode ảnh tại timestamp {timestamp}")
                
        except Exception as e:
            print(f"Lỗi xử lý frame: {e}")

def main():
    # Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("SparkBackgroundRemover") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Đọc Stream từ Socket
    raw_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 6100) \
        .load()

    # Định nghĩa cấu trúc JSON
    json_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("image", StringType(), True)
    ])

    # Parse cột 'value' (text) thành các cột dữ liệu
    parsed_df = raw_df.select(from_json(col("value"), json_schema).alias("data")).select("data.*")

    # Chạy Stream và xử lý bằng hàm process_batch
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='1 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()