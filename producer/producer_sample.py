import os
import json
import requests
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from pubg_python import PUBG, Shard

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()

APIKEY = os.getenv("PUBG_API_KEY")

PLAYER_NAMES = ["ATTACK-SUPERNOVA", "B1ceps-", "6zD666"] # https://op.gg/ko/pubg 에서 가져옴
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "pubg-matches"


producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except NoBrokersAvailable:
        logging.warning("Kafka 브로커에 연결할 수 없습니다. 5초 후 재시도합니다.")
        time.sleep(5)

pubg = PUBG(APIKEY, Shard.STEAM)


def fetch_player_and_matches(player_name):
    player = pubg.players().filter(player_names=[player_name])
    
    if not player:
        logging.warning(f"{player_name} 플레이어를 찾을 수 없습니다.")
        return

    player = player[0]
    logging.info(vars(player))
    
    player_data = {
        "player_name": player_name,
        "account_id": player.id,
        "shard_id": player.shard_id,
        "created_at": str(player.created_at),
        "updated_at": str(player.updated_at),
        "match_ids": [m.id for m in player.matches[:3]]
    }

    matches_data = []
    for match in player.matches[:3]:
        m = pubg.matches().get(match.id)
        matches_data.append({
            "match_id": m.id,
            "game_mode": m.game_mode,
            "map_name": m.map_name,
            "duration": m.duration,
            "created_at": str(m.created_at)
        })

    return {"player": player_data,  "matches": matches_data}


if __name__ == "__main__":
    while True:
        try:
            for PLAYER_NAME in PLAYER_NAMES:
                data = fetch_player_and_matches(PLAYER_NAME)
                producer.send(KAFKA_TOPIC, value=data).get(timeout=10)
                logging.info(f"토픽 저장 성공 : {PLAYER_NAME}")
        except Exception as e:
            logging.error(f"토픽 저장 실패 : {e}")
        time.sleep(60)

