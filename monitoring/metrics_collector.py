import os
import json
import time
import logging
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "pubg-matches"
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "your_token_here")  # 실제로는 InfluxDB에서 생성된 토큰 사용
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pubg-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "pubg-metrics")

def create_influx_client():
    """InfluxDB 클라이언트 생성"""
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logging.info("InfluxDB 클라이언트 연결 성공")
        return client, write_api
    except Exception as e:
        logging.error(f"InfluxDB 연결 실패: {e}")
        return None, None

def create_kafka_consumer():
    """Kafka 컨슈머 생성"""
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",  # 최신 데이터부터 수집
                enable_auto_commit=True,
                group_id="metrics-collector-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            logging.info(f"Kafka 컨슈머 연결 성공: {KAFKA_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            logging.warning("Kafka 브로커에 연결할 수 없습니다. 5초 후 재시도합니다.")
            time.sleep(5)

def extract_business_metrics(data):
    """비즈니스 메트릭 추출"""
    metrics = []
    current_time = datetime.utcnow()
    
    try:
        # 플레이어 기본 정보
        player_info = data.get('player', {})
        player_name = player_info.get('player_name', 'unknown')
        account_id = player_info.get('account_id', 'unknown')
        
        # 플레이어 레벨 메트릭
        point = Point("player_stats") \
            .tag("player_name", player_name) \
            .tag("account_id", account_id) \
            .field("total_matches_count", player_info.get('total_matches_count', 0)) \
            .time(current_time)
        metrics.append(point)
        
        # 매치별 상세 메트릭
        matches = data.get('matches', [])
        for match in matches:
            if not match:
                continue
                
            match_id = match.get('match_id', 'unknown')
            game_mode = match.get('game_mode', 'unknown')
            map_name = match.get('map_name', 'unknown')
            duration = match.get('duration', 0)
            
            # 매치 기본 정보
            match_point = Point("match_info") \
                .tag("player_name", player_name) \
                .tag("match_id", match_id) \
                .tag("game_mode", game_mode) \
                .tag("map_name", map_name) \
                .field("duration_seconds", duration) \
                .field("duration_minutes", duration / 60 if duration else 0) \
                .field("participants_count", match.get('participants_count', 0)) \
                .field("teams_count", match.get('rosters_count', 0)) \
                .time(current_time)
            metrics.append(match_point)
            
            # 플레이어 성과 메트릭
            performance = match.get('player_performance')
            if performance:
                perf_point = Point("player_performance_v2") \
                    .tag("player_name", player_name) \
                    .tag("match_id", match_id) \
                    .tag("game_mode", game_mode) \
                    .tag("map_name", map_name) \
                    .field("kills", int(performance.get('kills', 0))) \
                    .field("assists", int(performance.get('assists', 0))) \
                    .field("headshot_kills", int(performance.get('headshot_kills', 0))) \
                    .field("damage_dealt", float(performance.get('damage_dealt', 0))) \
                    .field("time_survived", float(performance.get('time_survived', 0))) \
                    .field("heals", int(performance.get('heals', 0))) \
                    .field("boosts", int(performance.get('boosts', 0))) \
                    .field("walk_distance", float(performance.get('walk_distance', 0))) \
                    .field("ride_distance", float(performance.get('ride_distance', 0))) \
                    .field("win_place", int(performance.get('win_place', 0))) \
                    .field("longest_kill", float(performance.get('longest_kill', 0))) \
                    .field("weapons_acquired", int(performance.get('weapons_acquired', 0))) \
                    .time(current_time)
                metrics.append(perf_point)
                
                # KDA 계산
                kills = int(performance.get('kills', 0))
                deaths = 1 if performance.get('death_type') != 'alive' else 0
                assists = int(performance.get('assists', 0))
                kda = (kills + assists) / max(deaths, 1)
                
                kda_point = Point("player_kda") \
                    .tag("player_name", player_name) \
                    .tag("match_id", match_id) \
                    .tag("game_mode", game_mode) \
                    .field("kda_ratio", float(kda)) \
                    .field("kills", int(kills)) \
                    .field("deaths", int(deaths)) \
                    .field("assists", int(assists)) \
                    .time(current_time)
                metrics.append(kda_point)
        
        # 실시간 시스템 메트릭
        system_point = Point("system_metrics") \
            .field("messages_processed", 1) \
            .field("players_in_batch", len(matches)) \
            .field("data_freshness_minutes", 0)  # 실시간이므로 0
        metrics.append(system_point)
        
        return metrics
        
    except Exception as e:
        logging.error(f"메트릭 추출 중 오류: {e}")
        return []

def main():
    """메인 실행 함수"""
    logging.info("PUBG 실시간 메트릭 수집기 시작...")
    
    # InfluxDB 연결
    influx_client, write_api = create_influx_client()
    if not influx_client:
        logging.error("InfluxDB 연결 실패로 종료")
        return
    
    # Kafka 컨슈머 생성
    consumer = create_kafka_consumer()
    
    # 메트릭 수집 시작
    messages_processed = 0
    
    try:
        logging.info("실시간 메트릭 수집 시작...")
        
        for message in consumer:
            try:
                # 비즈니스 메트릭 추출
                metrics = extract_business_metrics(message.value)
                
                if metrics:
                    # InfluxDB에 메트릭 저장
                    write_api.write(bucket=INFLUXDB_BUCKET, record=metrics)
                    messages_processed += 1
                    
                    # 처리된 플레이어 정보 로깅
                    player_name = message.value.get('player', {}).get('player_name', 'unknown')
                    match_count = len(message.value.get('matches', []))
                    
                    logging.info(f"✅ 메트릭 저장 완료: {player_name} ({match_count}개 매치, 총 처리: {messages_processed})")
                    
                    # 100개 메시지마다 상태 출력
                    if messages_processed % 100 == 0:
                        logging.info(f"📊 누적 처리량: {messages_processed}개 메시지")
                        
            except Exception as e:
                logging.error(f"메시지 처리 중 오류: {e}")
                continue
                
    except KeyboardInterrupt:
        logging.info("사용자에 의해 중단됨")
    except Exception as e:
        logging.error(f"예상치 못한 오류: {e}")
    finally:
        # 리소스 정리
        if consumer:
            consumer.close()
        if influx_client:
            influx_client.close()
        logging.info(f"메트릭 수집기 종료. 총 처리: {messages_processed}개 메시지")

if __name__ == "__main__":
    main()