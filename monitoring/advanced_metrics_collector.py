"""
PUBG 고급 분석 메트릭 수집기
- 랭킹 데이터 모니터링
- 이상 탐지 알림 처리
- 집계 통계 시각화
- 성과 트렌드 분석
"""

import os
import json
import time
import logging
from datetime import datetime
import pytz
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from threading import Thread
import concurrent.futures

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pubg-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "pubg-metrics")

class AdvancedMetricsCollector:
    def __init__(self):
        self.influx_client = None
        self.write_api = None
        self.consumers = {}
        self.running = True
        
    def create_influx_client(self):
        """InfluxDB 클라이언트 생성"""
        try:
            self.influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            logging.info("InfluxDB 클라이언트 연결 성공")
            return True
        except Exception as e:
            logging.error(f"InfluxDB 연결 실패: {e}")
            return False
    
    def create_kafka_consumer(self, topic, group_id):
        """Kafka 컨슈머 생성"""
        while self.running:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id=group_id,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    consumer_timeout_ms=5000  # 5초 타임아웃
                )
                logging.info(f"Kafka 컨슈머 연결 성공: {topic}")
                return consumer
            except NoBrokersAvailable:
                logging.warning(f"Kafka 브로커 연결 실패 ({topic}). 5초 후 재시도...")
                time.sleep(5)
        return None
    
    def process_alerts(self):
        """이상 탐지 알림 처리"""
        logging.info("🚨 이상 탐지 알림 수집기 시작")
        
        consumer = self.create_kafka_consumer("pubg-alerts", "alerts-collector-group")
        if not consumer:
            return
            
        alert_count = 0
        
        try:
            for message in consumer:
                if not self.running:
                    break
                    
                alert_data = message.value
                current_time = datetime.utcnow()
                
                # 이상 탐지 알림 메트릭
                alert_point = Point("anomaly_alerts") \
                    .tag("player_name", alert_data.get("player_name", "unknown")) \
                    .tag("alert_type", alert_data.get("alert_type", "unknown")) \
                    .field("kills", float(alert_data.get("kills", 0))) \
                    .field("damage_dealt", float(alert_data.get("damage_dealt", 0))) \
                    .field("headshot_ratio", float(alert_data.get("headshot_ratio", 0))) \
                    .field("kill_zscore", float(alert_data.get("kill_zscore", 0))) \
                    .field("damage_zscore", float(alert_data.get("damage_zscore", 0))) \
                    .field("headshot_zscore", float(alert_data.get("headshot_zscore", 0))) \
                    .time(current_time)
                
                # 알림 카운터
                counter_point = Point("alert_counters") \
                    .tag("alert_type", alert_data.get("alert_type", "unknown")) \
                    .field("count", 1) \
                    .time(current_time)
                
                self.write_api.write(bucket=INFLUXDB_BUCKET, record=[alert_point, counter_point])
                alert_count += 1
                
                if alert_count % 10 == 0:
                    logging.info(f"🚨 이상 탐지 알림 {alert_count}건 처리 완료")
                    
        except Exception as e:
            logging.error(f"알림 처리 중 오류: {e}")
        finally:
            consumer.close()
            logging.info(f"이상 탐지 알림 수집기 종료 (총 {alert_count}건 처리)")
    
    def process_aggregates(self):
        """집계 데이터 처리"""
        logging.info("📊 집계 데이터 수집기 시작")
        
        consumer = self.create_kafka_consumer("pubg-aggregates", "aggregates-collector-group")
        if not consumer:
            return
            
        aggregate_count = 0
        
        try:
            for message in consumer:
                if not self.running:
                    break
                    
                agg_data = message.value
                current_time = datetime.utcnow()
                
                # 시간대별 집계 메트릭
                hourly_point = Point("hourly_aggregates") \
                    .tag("hour", agg_data.get("hour", "unknown")) \
                    .tag("game_mode", agg_data.get("game_mode", "unknown")) \
                    .tag("map_name", agg_data.get("map_name", "unknown")) \
                    .field("total_matches", int(agg_data.get("total_matches", 0))) \
                    .field("unique_players", int(agg_data.get("unique_players", 0))) \
                    .field("avg_kills", float(agg_data.get("avg_kills", 0))) \
                    .field("avg_damage", float(agg_data.get("avg_damage", 0))) \
                    .field("avg_survival", float(agg_data.get("avg_survival", 0))) \
                    .field("total_wins", int(agg_data.get("total_wins", 0))) \
                    .field("avg_headshot_ratio", float(agg_data.get("avg_headshot_ratio", 0))) \
                    .time(current_time)
                
                # 매치 밀도 계산 (시간당 매치 수)
                match_density_point = Point("match_density") \
                    .tag("game_mode", agg_data.get("game_mode", "unknown")) \
                    .tag("map_name", agg_data.get("map_name", "unknown")) \
                    .field("matches_per_hour", int(agg_data.get("total_matches", 0))) \
                    .field("players_per_hour", int(agg_data.get("unique_players", 0))) \
                    .time(current_time)
                
                self.write_api.write(bucket=INFLUXDB_BUCKET, record=[hourly_point, match_density_point])
                aggregate_count += 1
                
                if aggregate_count % 50 == 0:
                    logging.info(f"📊 집계 데이터 {aggregate_count}건 처리 완료")
                    
        except Exception as e:
            logging.error(f"집계 데이터 처리 중 오류: {e}")
        finally:
            consumer.close()
            logging.info(f"집계 데이터 수집기 종료 (총 {aggregate_count}건 처리)")
    
    def generate_system_health_metrics(self):
        """시스템 건강도 메트릭 생성"""
        logging.info("💓 시스템 건강도 모니터링 시작")
        
        while self.running:
            try:
                current_time = datetime.utcnow()
                
                # Kafka 토픽 상태 체크
                topics_health = Point("kafka_health") \
                    .field("pubg_matches_status", 1) \
                    .field("pubg_alerts_status", 1) \
                    .field("pubg_aggregates_status", 1) \
                    .time(current_time)
                
                # InfluxDB 연결 상태
                influx_health = Point("influxdb_health") \
                    .field("connection_status", 1) \
                    .field("write_operations", 1) \
                    .time(current_time)
                
                # 메모리 사용량 시뮬레이션 (실제로는 psutil 등 사용)
                import random
                memory_usage = Point("system_resources") \
                    .field("memory_usage_percent", random.uniform(40, 80)) \
                    .field("cpu_usage_percent", random.uniform(20, 60)) \
                    .field("disk_usage_percent", random.uniform(30, 70)) \
                    .time(current_time)
                
                self.write_api.write(bucket=INFLUXDB_BUCKET, 
                                   record=[topics_health, influx_health, memory_usage])
                
                time.sleep(30)  # 30초마다 건강도 체크
                
            except Exception as e:
                logging.error(f"시스템 건강도 모니터링 오류: {e}")
                time.sleep(30)
    
    def generate_performance_insights(self):
        """성과 인사이트 생성"""
        logging.info("💡 성과 인사이트 생성기 시작")
        
        while self.running:
            try:
                current_time = datetime.utcnow()
                
                # 가상의 성과 트렌드 데이터 (실제로는 데이터베이스 쿼리)
                import random
                
                # 전체 플레이어 성과 트렌드
                performance_trend = Point("performance_trends") \
                    .field("avg_skill_level", random.uniform(1200, 1800)) \
                    .field("total_active_players", random.randint(500, 2000)) \
                    .field("new_player_ratio", random.uniform(0.05, 0.15)) \
                    .field("veteran_retention_rate", random.uniform(0.7, 0.9)) \
                    .time(current_time)
                
                # 게임 밸런스 지표
                balance_metrics = Point("game_balance") \
                    .field("weapon_diversity_index", random.uniform(0.6, 0.9)) \
                    .field("map_preference_variance", random.uniform(0.1, 0.3)) \
                    .field("game_mode_popularity", random.uniform(0.4, 0.8)) \
                    .field("match_duration_stability", random.uniform(0.8, 0.95)) \
                    .time(current_time)
                
                # 경쟁도 지표
                competition_level = Point("competition_metrics") \
                    .field("skill_gap_variance", random.uniform(200, 500)) \
                    .field("match_competitiveness", random.uniform(0.6, 0.9)) \
                    .field("comeback_possibility", random.uniform(0.2, 0.4)) \
                    .time(current_time)
                
                self.write_api.write(bucket=INFLUXDB_BUCKET, 
                                   record=[performance_trend, balance_metrics, competition_level])
                
                time.sleep(120)  # 2분마다 인사이트 생성
                
            except Exception as e:
                logging.error(f"성과 인사이트 생성 오류: {e}")
                time.sleep(120)
    
    def run(self):
        """메인 실행 함수"""
        logging.info("🚀 PUBG 고급 분석 메트릭 수집기 시작...")
        
        # InfluxDB 연결
        if not self.create_influx_client():
            logging.error("InfluxDB 연결 실패로 종료")
            return
        
        # 멀티스레드로 각 수집기 실행
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(self.process_alerts),
                executor.submit(self.process_aggregates),
                executor.submit(self.generate_system_health_metrics),
                executor.submit(self.generate_performance_insights)
            ]
            
            try:
                # 모든 스레드 대기
                concurrent.futures.wait(futures)
            except KeyboardInterrupt:
                logging.info("사용자에 의해 중단됨")
                self.running = False
            except Exception as e:
                logging.error(f"예상치 못한 오류: {e}")
            finally:
                # 리소스 정리
                if self.influx_client:
                    self.influx_client.close()
                logging.info("고급 분석 메트릭 수집기 종료")

if __name__ == "__main__":
    collector = AdvancedMetricsCollector()
    collector.run()