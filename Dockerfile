FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/producer.py ./producer.py
COPY consumer/consumer.py ./consumer.py
COPY monitoring/metrics_collector.py ./metrics_collector.py
COPY monitoring/advanced_metrics_collector.py ./advanced_metrics_collector.py
COPY monitoring/alert_manager.py ./alert_manager.py