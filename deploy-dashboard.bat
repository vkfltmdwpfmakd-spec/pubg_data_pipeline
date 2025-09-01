@echo off
echo ===================================
echo PUBG 실시간 데이터 파이프라인 배포
echo ===================================

echo.
echo 🚀 시스템 구성:
echo - Kafka + Spark Streaming 
echo - HDFS 데이터 저장
echo - InfluxDB + Grafana 모니터링
echo - 고급 분석 (랭킹, 이상탐지, 집계)
echo - Slack 알림 시스템

echo.
echo 📊 접속 정보:
echo - Grafana: http://localhost:3000 (admin/admin123)
echo - Spark UI: http://localhost:8080
echo - HDFS: http://localhost:9870
echo - InfluxDB: http://localhost:8086

echo.
echo ⚠️ 주의사항:
echo - Slack 알림: .env 파일에 SLACK_WEBHOOK_URL 설정
echo - 완전 로딩까지 3-5분 소요

echo.
echo 🚀 전체 시스템 시작...
docker-compose down
docker-compose up -d

echo.
echo ⏳ 서비스 준비 대기 중... (대시보드 자동 로딩)
timeout /t 30 /nobreak >nul

echo.
echo 📊 대시보드가 자동으로 프로비저닝됩니다...

echo.
echo ✅ 배포 완료!
echo.
echo 📄 대시보드 확인:
echo 1. 기본 대시보드: http://localhost:3000/d/pubg-dashboard/
echo 2. 고급 분석 대시보드: http://localhost:3000 (메뉴에서 선택)
echo.

set /p choice="실시간 로그를 확인하시겠습니까? (y/n): "
if /i "%choice%"=="y" (
    echo.
    echo 📄 시스템 로그 확인 중... (Ctrl+C로 종료)
    docker-compose logs -f producer spark-streaming spark-advanced-analytics metrics-collector advanced-metrics-collector alert-manager
)

echo.
pause