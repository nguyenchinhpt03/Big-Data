import pandas as pd
from kafka import KafkaProducer
import json
import time

# 1. Khởi tạo kết nối tới Kafka Server
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Kết nối Kafka thành công! Bắt đầu phát luồng dữ liệu...")
except Exception as e:
    print(f"❌ Lỗi kết nối Kafka: {e}")
    print("Vui lòng đảm bảo Zookeeper và Kafka Server đang chạy.")
    exit()

# Đường dẫn tương đối đến file data của bạn
file_path = 'data/chicago_crime_data.csv'
topic_name = 'crime_topic'

try:
    # 2. Đọc dữ liệu theo từng khối nhỏ (100 dòng/lần) để mô phỏng Streaming
    for chunk in pd.read_csv(file_path, chunksize=100):
        chunk = chunk.fillna("") # Thay thế các giá trị NaN bằng chuỗi rỗng
        records = chunk.to_dict(orient='records')
        
        for record in records:
            # Gửi dữ liệu vào topic
            producer.send(topic_name, record)
            print(f"Báo cáo mới: {record.get('Date')} - {record.get('Primary Type')} tại Quận {record.get('District')}")
            
            # Tạm dừng 0.1 giây để giả lập thời gian thực
            time.sleep(0.1) 
            
except FileNotFoundError:
    print(f"❌ Không tìm thấy file tại {file_path}. Vui lòng kiểm tra lại tên file.")