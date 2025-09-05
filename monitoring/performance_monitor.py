"""
PUBG 데이터 파이프라인 성능 모니터링 시스템
- Kafka 처리량 및 지연시간 모니터링
- Spark 성능 메트릭 수집
- 시스템 리소스 사용량 추적
- 실시간 성능 대시보드 데이터 제공
"""

import os
import time
import psutil
import logging
import pytz
from datetime import datetime
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
import json
from threading import Thread
import concurrent.futures

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 환경 변수
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pubg-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "pubg-metrics")

class PerformanceMonitor:
    def __init__(self):
        self.influx_client = None
        self.write_api = None
        self.kafka_admin = None
        self.running = True
        self.performance_data = {
            'kafka_lag': {},
            'processing_times': {},
            'system_resources': {}
        }
        
    def setup_clients(self):
        """클라이언트 연결 설정"""
        try:
            # InfluxDB 클라이언트
            self.influx_client = InfluxDBClient(
                url=INFLUXDB_URL,
                token=INFLUXDB_TOKEN,
                org=INFLUXDB_ORG
            )
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
            logger.info("✅ InfluxDB 연결 성공")
            
            # Kafka Admin 클라이언트
            self.kafka_admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id="performance-monitor"
            )
            logger.info("✅ Kafka Admin 연결 성공")
            
        except Exception as e:
            logger.error(f"❌ 클라이언트 연결 실패: {e}")
            
    def collect_kafka_metrics(self):
        """Kafka 성능 메트릭 수집"""
        try:
            topics = ['pubg-matches', 'pubg-alerts', 'pubg-aggregates']
            
            for topic in topics:
                # 컨슈머 그룹별 지연시간 확인
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=f'performance-monitor-{topic}',
                    auto_offset_reset='latest',
                    enable_auto_commit=False,
                    consumer_timeout_ms=5000
                )
                
                # 파티션별 메트릭
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    for partition in partitions:
                        tp = TopicPartition(topic, partition)
                        
                        # 현재 오프셋과 최신 오프셋 비교
                        committed = consumer.committed(tp)
                        end_offsets = consumer.end_offsets([tp])
                        
                        if committed is not None and tp in end_offsets:
                            lag = end_offsets[tp] - committed
                            
                            # InfluxDB에 저장
                            point = Point("kafka_performance") \
                                .tag("topic", topic) \
                                .tag("partition", str(partition)) \
                                .field("consumer_lag", lag) \
                                .field("latest_offset", end_offsets[tp]) \
                                .field("committed_offset", committed) \
                                .time(datetime.now(KST))
                            
                            self.write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                            
                            # 메모리에도 저장
                            key = f"{topic}_p{partition}"
                            self.performance_data['kafka_lag'][key] = {
                                'lag': lag,
                                'latest_offset': end_offsets[tp],
                                'committed_offset': committed,
                                'timestamp': time.time()
                            }
                
                consumer.close()
                
        except Exception as e:
            logger.error(f"❌ Kafka 메트릭 수집 실패: {e}")
    
    def collect_spark_metrics(self):
        """Spark 성능 메트릭 수집"""
        try:
            # Spark Master Web UI에서 메트릭 수집
            spark_master_url = "http://spark-master:8080/json/"
            
            response = requests.get(spark_master_url, timeout=10)
            if response.status_code == 200:
                spark_data = response.json()
                
                # Active 애플리케이션 메트릭
                for app in spark_data.get('activeapps', []):
                    app_id = app.get('id', 'unknown')
                    
                    point = Point("spark_performance") \
                        .tag("application_id", app_id) \
                        .tag("application_name", app.get('name', 'unknown')) \
                        .field("cores_granted", app.get('coresgranted', 0)) \
                        .field("cores_requested", app.get('coresrequested', 0)) \
                        .field("memory_used", app.get('memoryused', 0)) \
                        .field("executors", len(app.get('executorinfos', []))) \
                        .field("duration", app.get('duration', 0)) \
                        .time(datetime.now(KST))
                    
                    self.write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                
                # 워커 메트릭
                for worker in spark_data.get('workers', []):
                    point = Point("spark_workers") \
                        .tag("worker_id", worker.get('id', 'unknown')) \
                        .tag("worker_host", worker.get('host', 'unknown')) \
                        .field("cores_used", worker.get('coresused', 0)) \
                        .field("cores_free", worker.get('coresfree', 0)) \
                        .field("memory_used", worker.get('memoryused', 0)) \
                        .field("memory_free", worker.get('memoryfree', 0)) \
                        .field("active_drivers", worker.get('activedrivers', 0)) \
                        .field("active_executors", worker.get('activeexecutors', 0)) \
                        .time(datetime.now(KST))
                    
                    self.write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                    
        except Exception as e:
            logger.error(f"❌ Spark 메트릭 수집 실패: {e}")
    
    def collect_system_metrics(self):
        """시스템 리소스 메트릭 수집"""
        try:
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            
            # 네트워크 I/O
            net_io = psutil.net_io_counters()
            
            # 시스템 메트릭 저장
            point = Point("system_performance") \
                .field("cpu_percent", cpu_percent) \
                .field("memory_percent", memory.percent) \
                .field("memory_available", memory.available) \
                .field("memory_total", memory.total) \
                .field("disk_percent", (disk.used / disk.total) * 100) \
                .field("disk_free", disk.free) \
                .field("disk_total", disk.total) \
                .field("network_bytes_sent", net_io.bytes_sent) \
                .field("network_bytes_recv", net_io.bytes_recv) \
                .field("network_packets_sent", net_io.packets_sent) \
                .field("network_packets_recv", net_io.packets_recv) \
                .time(datetime.now(KST))
            
            self.write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            
            # 메모리에도 저장
            self.performance_data['system_resources'] = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'disk_percent': (disk.used / disk.total) * 100,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"❌ 시스템 메트릭 수집 실패: {e}")
    
    def collect_processing_latency(self):
        """처리 지연시간 메트릭 수집"""
        try:
            # InfluxDB에서 최근 처리 시간 데이터 가져오기
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -5m)
              |> filter(fn: (r) => r["_measurement"] == "player_performance")
              |> filter(fn: (r) => r["_field"] == "processing_latency")
              |> last()
            '''
            
            tables = self.influx_client.query_api().query(query, org=INFLUXDB_ORG)
            
            total_latency = 0
            count = 0
            
            for table in tables:
                for record in table.records:
                    total_latency += record.get_value()
                    count += 1
            
            if count > 0:
                avg_latency = total_latency / count
                
                point = Point("processing_performance") \
                    .field("avg_processing_latency", avg_latency) \
                    .field("total_processed", count) \
                    .time(datetime.now(KST))
                
                self.write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                
        except Exception as e:
            logger.error(f"❌ 처리 지연시간 메트릭 수집 실패: {e}")
    
    def generate_performance_summary(self):
        """성능 요약 리포트 생성"""
        try:
            current_time = datetime.now(KST)
            
            # 성능 요약 데이터
            kafka_total_lag = sum([data['lag'] for data in self.performance_data['kafka_lag'].values()])
            
            system_data = self.performance_data.get('system_resources', {})
            
            summary_point = Point("performance_summary") \
                .field("kafka_total_lag", kafka_total_lag) \
                .field("kafka_topics_monitored", len(self.performance_data['kafka_lag'])) \
                .field("system_cpu_percent", system_data.get('cpu_percent', 0)) \
                .field("system_memory_percent", system_data.get('memory_percent', 0)) \
                .field("system_disk_percent", system_data.get('disk_percent', 0)) \
                .field("monitoring_health", 1) \
                .time(current_time)
            
            self.write_api.write(bucket=INFLUXDB_BUCKET, record=summary_point)
            
            logger.info(f"📊 성능 요약: Kafka 지연 {kafka_total_lag}건, CPU {system_data.get('cpu_percent', 0):.1f}%, 메모리 {system_data.get('memory_percent', 0):.1f}%")
            
        except Exception as e:
            logger.error(f"❌ 성능 요약 생성 실패: {e}")
    
    def run_monitoring_cycle(self):
        """모니터링 사이클 실행"""
        logger.info("🚀 성능 모니터링 시작")
        
        while self.running:
            try:
                start_time = time.time()
                
                # 병렬로 메트릭 수집
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    futures = [
                        executor.submit(self.collect_kafka_metrics),
                        executor.submit(self.collect_spark_metrics),
                        executor.submit(self.collect_system_metrics),
                        executor.submit(self.collect_processing_latency)
                    ]
                    
                    # 모든 작업 완료 대기
                    concurrent.futures.wait(futures, timeout=30)
                
                # 성능 요약 생성
                self.generate_performance_summary()
                
                cycle_time = time.time() - start_time
                logger.info(f"⏱️ 모니터링 사이클 완료: {cycle_time:.2f}초")
                
                # 30초마다 실행
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ 모니터링 사이클 오류: {e}")
                time.sleep(10)
    
    def shutdown(self):
        """모니터링 종료"""
        self.running = False
        if self.influx_client:
            self.influx_client.close()
        logger.info("🛑 성능 모니터링 종료")

def main():
    monitor = PerformanceMonitor()
    
    try:
        monitor.setup_clients()
        monitor.run_monitoring_cycle()
    except KeyboardInterrupt:
        logger.info("사용자에 의한 종료")
    except Exception as e:
        logger.error(f"❌ 모니터링 실행 실패: {e}")
    finally:
        monitor.shutdown()

if __name__ == "__main__":
    main()