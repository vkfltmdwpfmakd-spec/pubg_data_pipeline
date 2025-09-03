# PUBG Real-time Data Pipeline

실시간 PUBG 게임 데이터를 수집, 처리, 분석하는 엔드투엔드 데이터 파이프라인입니다. Apache Kafka, Spark Streaming, HDFS, InfluxDB, Grafana를 활용하여 실시간 플레이어 성과 분석 및 모니터링 시스템을 구축했습니다.

## 프로젝트 개요

이 프로젝트는 PUBG API를 통해 실시간으로 플레이어 데이터를 수집하고, 스트림 처리를 통해 실시간 분석을 수행합니다. 플레이어 성과 추적, 이상 패턴 탐지, 게임 트렌드 분석 등의 인사이트를 제공하며, Grafana 대시보드를 통해 실시간 시각화를 지원합니다.

### 주요 특징

- **실시간 데이터 처리**: Apache Kafka와 Spark Streaming을 활용한 실시간 데이터 파이프라인
- **고급 분석 기능**: 플레이어 랭킹, 이상 탐지, 윈도우 함수 기반 트렌드 분석
- **확장 가능한 아키텍처**: Docker Compose를 통한 마이크로서비스 구조
- **실시간 모니터링**: InfluxDB + Grafana를 통한 실시간 대시보드
- **지능형 알림**: 성과 이상 패턴 감지 및 알림 시스템
- **한국 시간대 지원**: KST(Asia/Seoul) 기준 데이터 처리

## 시스템 아키텍처

```
PUBG API → Producer → Kafka → Spark Streaming → HDFS
                                    ↓
              InfluxDB ← Metrics Collector ← Advanced Analytics
                 ↓
              Grafana Dashboard
```

### 데이터 흐름

1. **데이터 수집**: PUBG API에서 플레이어 및 매치 데이터 수집
2. **메시지 큐**: Kafka를 통한 실시간 데이터 스트리밍
3. **스트림 처리**: Spark Streaming으로 데이터 변환 및 정제
4. **저장**: HDFS에 구조화된 데이터 저장
5. **고급 분석**: 실시간 랭킹, 이상 탐지, 집계 분석 수행
6. **메트릭 수집**: InfluxDB에 시계열 메트릭 저장
7. **시각화**: Grafana 대시보드를 통한 실시간 모니터링

## 기술 스택

### 핵심 기술
- **Apache Kafka 7.0.1**: 실시간 데이터 스트리밍
- **Apache Spark 3.3.0**: 대용량 데이터 처리 및 스트리밍
- **Hadoop HDFS 3.2.1**: 분산 파일 시스템
- **InfluxDB 2.7**: 시계열 데이터베이스
- **Grafana 10.2.0**: 데이터 시각화 및 모니터링

### 지원 기술
- **Python 3.9**: 애플리케이션 개발 언어
- **Docker & Docker Compose**: 컨테이너 기반 배포
- **Apache Zookeeper**: Kafka 클러스터 관리
- **PUBG API**: 게임 데이터 소스

### 주요 Python 라이브러리
- `kafka-python`: Kafka 클라이언트
- `pyspark`: Spark Python API
- `pubg-python`: PUBG API 클라이언트
- `influxdb-client`: InfluxDB 연동
- `pytz`: 시간대 처리

## 주요 기능

### 1. 실시간 데이터 수집
- PUBG API를 통한 플레이어 및 매치 데이터 실시간 수집
- API 레이트 리미팅 및 에러 핸들링
- 구조화된 데이터 스키마를 통한 일관된 데이터 처리

### 2. 스트림 데이터 처리
- Spark Streaming을 활용한 실시간 데이터 변환
- 플레이어 성과 데이터 추출 및 정제
- 중복 제거 및 데이터 품질 관리

### 3. 고급 분석 시스템
- **실시간 플레이어 랭킹**: 종합 성과 점수 기반 실시간 순위
- **윈도우 함수 분석**: 최근 N게임 이동평균 및 트렌드 분석
- **이상 탐지**: 통계적 Z-Score 기반 이상 패턴 탐지
- **시간대별 집계**: 시간/맵/게임모드별 통계 생성
- **맵 난이도 분석**: 맵별 평균 성과 기반 난이도 계산

### 4. 실시간 모니터링
- 플레이어 KDA, 데미지, 생존시간 등 핵심 성과 지표 추적
- 시스템 헬스 체크 및 리소스 사용량 모니터링
- 커스터마이징 가능한 Grafana 대시보드

### 5. 지능형 알림 시스템
- 이상 성과 패턴 감지 시 실시간 알림
- Slack 웹훅 및 이메일 알림 지원
- 설정 가능한 임계값 및 알림 조건

## 설치 및 실행

### 1. 환경 설정

프로젝트를 클론하고 환경 변수를 설정합니다:

```bash
git clone <repository-url>
cd pubg_data_pipeline
```

`.env` 파일을 생성하고 PUBG API 키를 설정합니다:

```env
PUBG_API_KEY=your_pubg_api_key_here
SLACK_WEBHOOK_URL=your_slack_webhook_url
EMAIL_ENABLED=false
EMAIL_USERNAME=your_email@gmail.com
EMAIL_PASSWORD=your_email_password
EMAIL_FROM=your_email@gmail.com
EMAIL_TO=recipient@gmail.com
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
```

### 2. 시스템 실행

Docker Compose를 사용하여 전체 시스템을 시작합니다:

```bash
docker-compose up -d
```

### 3. 서비스 확인

각 서비스가 정상적으로 시작되었는지 확인합니다:

```bash
docker-compose ps
```

### 4. 서비스 접속

- **Grafana 대시보드**: http://localhost:3000 (admin/admin123)
- **Spark Master UI**: http://localhost:8080
- **HDFS NameNode UI**: http://localhost:9870
- **InfluxDB UI**: http://localhost:8086 (admin/password123)

## 모니터링 및 대시보드

### Grafana 대시보드

시스템에는 두 가지 주요 대시보드가 포함되어 있습니다:

1. **기본 대시보드** (`pubg_basic_dashboard.json`)
   - 실시간 플레이어 성과 메트릭
   - KDA, 데미지, 생존시간 추적
   - 매치별 성과 분석

2. **고급 분석 대시보드** (`advanced_analytics_dashboard.json`)
   - 플레이어 랭킹 시스템
   - 이상 탐지 알림
   - 시간대별 게임 통계
   - 시스템 헬스 모니터링

### 주요 메트릭

- **플레이어 성과**: 킬, 어시스트, 데미지, 헤드샷률
- **생존 지표**: 생존시간, 이동거리, 순위
- **시스템 메트릭**: 처리량, 지연시간, 에러율
- **게임 통계**: 모드별/맵별 평균 성과

## 프로젝트 구조

```
pubg_data_pipeline/
├── docker-compose.yml          # Docker Compose 설정
├── Dockerfile                  # Python 애플리케이션 컨테이너
├── requirements.txt            # Python 의존성
├── .env                       # 환경 변수
├── producer/
│   └── producer.py            # PUBG 데이터 수집기
├── consumer/
│   └── consumer.py            # Kafka 컨슈머
├── spark_apps/
│   ├── pubg_streaming.py      # Spark 스트리밍 애플리케이션
│   └── advanced_analytics.py  # 고급 분석 엔진
├── monitoring/
│   ├── metrics_collector.py         # 기본 메트릭 수집기
│   ├── advanced_metrics_collector.py # 고급 메트릭 수집기
│   ├── alert_manager.py             # 알림 관리자
│   └── grafana/
│       └── provisioning/
│           ├── dashboards/          # 대시보드 설정
│           └── datasources/         # 데이터 소스 설정
└── test/                           # 테스트 파일
```

### 주요 컴포넌트

- **Producer**: PUBG API를 통한 실시간 데이터 수집
- **Spark Streaming**: 실시간 데이터 처리 및 변환
- **Advanced Analytics**: 고급 분석 및 이상 탐지
- **Metrics Collector**: InfluxDB 메트릭 저장
- **Alert Manager**: 실시간 알림 처리

