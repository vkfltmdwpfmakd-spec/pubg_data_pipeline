@echo off
echo 대시보드 배포 중...
curl -X POST http://localhost:3000/api/dashboards/db -u admin:admin123 -H "Content-Type: application/json" --data @monitoring/grafana/import-dashboard.json
echo.
echo 완료! http://localhost:3000/d/pubg-dashboard/ 에서 확인하세요