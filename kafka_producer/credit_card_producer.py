#!/usr/bin/env python3
"""
Kafka Producer cho giao dịch thẻ tín dụng
Đọc file CSV và gửi từng giao dịch vào Kafka topic
với khoảng thời gian ngẫu nhiên từ 1-5 giây
"""

import csv
import json
import time
import random
import logging
from confluent_kafka import Producer, KafkaException
from datetime import datetime
from pathlib import Path

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load Environment Variables from .env
# find_dotenv() sẽ tự tìm file .env ở thư mục gốc project
import os
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except ImportError:
    logger.warning("Thư viện python-dotenv chưa cài đặt.")

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'credit-card-transactions')
   
# Đường dẫn file dataset
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(os.path.dirname(BASE_DIR), 'dataset')
DEFAULT_CSV_PATH = os.path.join(DATASET_DIR, 'User0_credit_card_transactions.csv')

# Cho phép override file path từ .env
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH', DEFAULT_CSV_PATH)

# Cấu hình Producer tối ưu
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'credit-card-producer',
    # Tối ưu hiệu suất
    'linger.ms': 10,  # Chờ 10ms để batch messages
    'batch.size': 16384,  # Batch size 16KB
    'compression.type': 'snappy',  # Nén dữ liệu
    'acks': 1,  # Đợi leader acknowledge
    # Retry và timeout
    'retries': 3,
    'retry.backoff.ms': 100,
    'request.timeout.ms': 30000,
    # Error handling
    'enable.idempotence': False  # Tắt để đơn giản
}

class CreditCardProducer:
    """Lớp Producer cho giao dịch thẻ tín dụng"""
    
    def __init__(self):
        try:
            self.producer = Producer(conf)
            logger.info("Khởi tạo Kafka Producer thành công")
        except KafkaException as e:
            logger.error(f"Lỗi khởi tạo Kafka Producer: {e}")
            raise
        
        self.success_count = 0
        self.error_count = 0
        self.total_count = 0
    
    def delivery_report(self, err, msg):
        """Callback để log trạng thái gửi message (không đếm ở đây)"""
        if err is not None:
            logger.error(f"Kafka callback: Message thất bại - {err}")
        else:
            logger.debug(f"Kafka callback: Message OK - {msg.topic()}[{msg.partition()}]@{msg.offset()}")
    
    def validate_csv_file(self) -> bool:
        """Kiểm tra file CSV tồn tại và có thể đọc"""
        csv_path = Path(CSV_FILE_PATH)
        
        if not csv_path.exists():
            logger.error(f"File không tồn tại: {CSV_FILE_PATH}")
            return False
        
        if not csv_path.is_file():
            logger.error(f"Đường dẫn không phải file: {CSV_FILE_PATH}")
            return False
        
        try:
            with open(CSV_FILE_PATH, 'r', encoding='utf-8') as f:
                csv.reader(f)
            logger.info(f"File CSV hợp lệ: {CSV_FILE_PATH}")
            return True
        except Exception as e:
            logger.error(f"Không thể đọc file CSV: {e}")
            return False
    
    def create_transaction_message(self, row: dict) -> dict:
        """Tạo message transaction từ dòng CSV"""
        return {
            'User': row['User'],
            'Card': row['Card'],
            'Year': row['Year'],
            'Month': row['Month'],
            'Day': row['Day'],
            'Time': row['Time'],
            'Amount': row['Amount'],
            'Use Chip': row['Use Chip'],
            'Merchant Name': row['Merchant Name'],
            'Merchant City': row['Merchant City'],
            'Merchant State': row['Merchant State'],
            'Zip': row['Zip'],
            'MCC': row['MCC'],
            'Errors?': row['Errors?'],
            'Is Fraud?': row['Is Fraud?']
        }
    
    def send_transaction(self, transaction: dict, key: str) -> bool:
        """Gửi một giao dịch vào Kafka"""
        try:
            message = json.dumps(transaction)
            
            self.producer.produce(
                KAFKA_TOPIC,
                key=key,
                value=message,
                callback=self.delivery_report
            )
            
            # Poll để xử lý callbacks
            self.producer.poll(0)
            return True
            
        except BufferError:
            logger.warning("Buffer đầy, đang đợi...")
            self.producer.flush()
            return False
        except Exception as e:
            logger.error(f"Lỗi gửi transaction: {e}")
            return False
    
    def run(self):
        """Chạy Producer - đọc CSV và gửi vào Kafka"""
        logger.info("="*60)
        logger.info("BẮT ĐẦU KAFKA PRODUCER")
        logger.info("="*60)
        logger.info(f"File CSV: {CSV_FILE_PATH}")
        logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
        logger.info(f"Khoảng thời gian: 1-5 giây (ngẫu nhiên)")
        logger.info("="*60)
        
        # Validate file trước
        if not self.validate_csv_file():
            return
        
        start_time = time.time()
        
        try:
            with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    self.total_count += 1
                    
                    # Tạo transaction message
                    transaction = self.create_transaction_message(row)
                    
                    # Gửi vào Kafka với retry
                    retry_count = 0
                    sent_successfully = False
                    
                    while retry_count < 3:
                        if self.send_transaction(transaction, str(row['Card'])):
                            sent_successfully = True
                            break
                        retry_count += 1
                        time.sleep(0.1)
                    
                    # Đếm ngay sau khi gửi
                    if sent_successfully:
                        self.success_count += 1
                    else:
                        self.error_count += 1
                        logger.error(f"Không thể gửi giao dịch #{self.total_count} sau 3 lần thử")
                    
                    # Log mỗi 10 giao dịch
                    if self.total_count % 10 == 0:
                        logger.info(
                            f"Tiến trình: {self.total_count} giao dịch "
                            f"(Thành công: {self.success_count}, Lỗi: {self.error_count})"
                        )
                    
                    # Sleep ngẫu nhiên 1-5 giây
                    sleep_time = random.uniform(1, 5)
                    time.sleep(sleep_time)
                
                # Đợi tất cả messages được gửi xong
                logger.info("\nĐang đồng bộ messages còn lại...")
                self.producer.flush()
                
        except FileNotFoundError:
            logger.error(f"Không tìm thấy file: {CSV_FILE_PATH}")
        except KeyboardInterrupt:
            logger.warning("\nProducer bị dừng bởi người dùng")
            self.producer.flush()
        except Exception as e:
            logger.error(f"Lỗi không mong muốn: {e}", exc_info=True)
        finally:
            # Thống kê cuối cùng
            elapsed_time = time.time() - start_time
            logger.info("\n" + "="*60)
            logger.info("THỐNG KÊ CUỐI CÙNG")
            logger.info("="*60)
            logger.info(f"Tổng số giao dịch: {self.total_count}")
            logger.info(f"Gửi thành công: {self.success_count}")
            logger.info(f"Gửi thất bại: {self.error_count}")
            logger.info(f"Tỷ lệ thành công: {(self.success_count/self.total_count*100) if self.total_count > 0 else 0:.2f}%")
            logger.info(f"Thời gian chạy: {elapsed_time:.2f} giây")
            
            if elapsed_time > 0:
                logger.info(f"Tốc độ trung bình: {self.total_count/elapsed_time:.2f} giao dịch/giây")
            
            logger.info("="*60)
            
            # Đóng producer
            self.producer.flush()

def main():
    """Hàm main"""
    try:
        producer = CreditCardProducer()
        producer.run()
    except Exception as e:
        logger.error(f"Lỗi khởi tạo Producer: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())