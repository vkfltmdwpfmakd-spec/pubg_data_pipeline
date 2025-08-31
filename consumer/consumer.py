# 테스트 용으로 만든 파일 , 사용 시 docker-compose 파일에서 주석 해제 필요

import os
import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "pubg-matches"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# docker logs -f consumer 실행 해서 확인
def consume_messages():

    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="pubg-consumer-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
        except NoBrokersAvailable:
            logging.warning("Kafka 브로커에 연결할 수 없습니다. 5초 후 재시도합니다.")
            time.sleep(5)


    logging.info(f"Kafka 토픽 구독 시작 : {KAFKA_TOPIC}")

    for message in consumer:
        logging.info(f"받은 메시지 : {json.dumps(message.value, ensure_ascii=False, indent=2)}")

if __name__ == "__main__":
    consume_messages()