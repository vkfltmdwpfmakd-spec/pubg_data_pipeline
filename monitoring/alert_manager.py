"""
PUBG 실시간 알림 관리자
- 이상 탐지 알림 처리
- Slack 통합
- 이메일 알림 (선택적)
- 알림 중복 제거 및 집계
"""

import os
import json
import time
import logging
import requests
import smtplib
import pytz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from collections import defaultdict, deque

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')
from threading import Lock

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")  # Slack 웹훅 URL
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"

# 이메일 설정
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_USERNAME = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")  # Gmail의 경우 앱 패스워드 사용
EMAIL_FROM = os.getenv("EMAIL_FROM", EMAIL_USERNAME)
EMAIL_TO = os.getenv("EMAIL_TO", "").split(",")  # 쉼표로 구분된 수신자 목록

class AlertManager:
    def __init__(self):
        self.alert_history = defaultdict(deque)  # 플레이어별 알림 히스토리
        self.alert_lock = Lock()
        self.notification_cache = {}  # 중복 알림 방지
        self.alert_stats = {
            'total_alerts': 0,
            'high_severity': 0,
            'medium_severity': 0,
            'last_alert_time': None
        }
        
    def create_kafka_consumer(self):
        """Kafka 컨슈머 생성"""
        while True:
            try:
                consumer = KafkaConsumer(
                    "pubg-alerts",
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    group_id="alert-manager-group",
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    consumer_timeout_ms=-1,    # 무한 대기 (타임아웃 없음)
                    session_timeout_ms=30000,    # 세션 타임아웃 증가
                    heartbeat_interval_ms=10000, # 하트비트 간격 증가
                    fetch_max_wait_ms=1000,      # 최대 대기 시간 줄임
                    max_poll_records=10,         # 한 번에 가져올 레코드 수 제한
                    reconnect_backoff_ms=5000,   # 재연결 백오프
                    retry_backoff_ms=5000        # 재시도 백오프
                )
                logging.info("Kafka 알림 컨슈머 연결 성공")
                return consumer
            except NoBrokersAvailable:
                logging.warning("Kafka 브로커 연결 실패. 10초 후 재시도...")
                time.sleep(10)
            except Exception as e:
                logging.error(f"Kafka 컨슈머 생성 중 오류: {e}. 10초 후 재시도...")
                time.sleep(10)
    
    def is_duplicate_alert(self, player_name, alert_type, threshold_minutes=10):
        """중복 알림 확인 (최근 N분 내 동일한 알림)"""
        cache_key = f"{player_name}_{alert_type}"
        current_time = datetime.utcnow()
        
        if cache_key in self.notification_cache:
            last_alert_time = self.notification_cache[cache_key]
            if (current_time - last_alert_time).total_seconds() < threshold_minutes * 60:
                return True
        
        self.notification_cache[cache_key] = current_time
        return False
    
    def format_slack_message(self, alert_data, severity):
        """Slack 메시지 포맷팅"""
        player_name = alert_data.get('player_name', 'Unknown')
        match_id = alert_data.get('match_id', 'N/A')
        
        # 심각도별 이모지 및 색상
        severity_config = {
            'HIGH': {'emoji': '🚨', 'color': '#FF0000'},
            'MEDIUM': {'emoji': '⚠️', 'color': '#FFA500'},
            'LOW': {'emoji': 'ℹ️', 'color': '#00FF00'}
        }
        
        config = severity_config.get(severity, severity_config['MEDIUM'])
        
        # 성과 지표별 메시지 구성
        performance_info = []
        
        kills = alert_data.get('kills', 0)
        damage = alert_data.get('damage_dealt', 0)
        headshot_ratio = alert_data.get('headshot_ratio', 0)
        
        if kills > 0:
            performance_info.append(f"• 킬 수: {kills}킬")
        if damage > 0:
            performance_info.append(f"• 데미지: {damage:,.0f}")
        if headshot_ratio > 0:
            performance_info.append(f"• 헤드샷 비율: {headshot_ratio:.1%}")
        
        # Z-Score 정보 (통계적 이상도)
        zscore_info = []
        kill_zscore = alert_data.get('kill_zscore', 0)
        damage_zscore = alert_data.get('damage_zscore', 0)
        headshot_zscore = alert_data.get('headshot_zscore', 0)
        
        if abs(kill_zscore) > 2:
            zscore_info.append(f"킬 수 이상도: {kill_zscore:.1f}σ")
        if abs(damage_zscore) > 2:
            zscore_info.append(f"데미지 이상도: {damage_zscore:.1f}σ")
        if abs(headshot_zscore) > 2:
            zscore_info.append(f"헤드샷 이상도: {headshot_zscore:.1f}σ")
        
        # 메시지 구성
        message = {
            "username": "PUBG 이상 탐지 시스템",
            "icon_emoji": ":warning:",
            "attachments": [
                {
                    "color": config['color'],
                    "title": f"{config['emoji']} {severity} 심각도 이상 패턴 탐지",
                    "fields": [
                        {
                            "title": "플레이어",
                            "value": f"`{player_name}`",
                            "short": True
                        },
                        {
                            "title": "매치 ID",
                            "value": f"`{match_id[:8]}...`",
                            "short": True
                        },
                        {
                            "title": "성과 지표",
                            "value": "\n".join(performance_info) if performance_info else "데이터 없음",
                            "short": False
                        },
                        {
                            "title": "통계적 이상도",
                            "value": "\n".join(zscore_info) if zscore_info else "정상 범위",
                            "short": False
                        }
                    ],
                    "footer": "PUBG Analytics",
                    "footer_icon": "https://cdn.iconscout.com/icon/free/png-256/pubg-3627018-3030122.png",
                    "ts": int(datetime.utcnow().timestamp())
                }
            ]
        }
        
        return message
    
    def send_slack_notification(self, alert_data, severity):
        """Slack 알림 전송"""
        if not SLACK_WEBHOOK_URL:
            logging.warning("Slack 웹훅 URL이 설정되지 않음")
            return False
        
        try:
            message = self.format_slack_message(alert_data, severity)
            
            response = requests.post(
                SLACK_WEBHOOK_URL,
                json=message,
                timeout=10
            )
            
            if response.status_code == 200:
                logging.info(f"✅ Slack 알림 전송 성공: {alert_data.get('player_name')}")
                return True
            else:
                logging.error(f"❌ Slack 알림 전송 실패: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Slack 알림 전송 중 오류: {e}")
            return False
    
    def format_email_message(self, alert_data, severity):
        """이메일 메시지 포맷팅"""
        player_name = alert_data.get('player_name', 'Unknown')
        match_id = alert_data.get('match_id', 'N/A')
        
        # 심각도별 제목 설정
        severity_titles = {
            'HIGH': '🚨 긴급: 고위험 이상 패턴 탐지',
            'MEDIUM': '⚠️ 주의: 중위험 이상 패턴 탐지',
            'LOW': 'ℹ️ 정보: 저위험 이상 패턴 탐지'
        }
        
        subject = f"PUBG Analytics - {severity_titles.get(severity, '알림')}"
        
        # HTML 이메일 본문 작성
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: {'#ff4444' if severity == 'HIGH' else '#ffa500' if severity == 'MEDIUM' else '#44aa44'}; 
                           color: white; padding: 15px; text-align: center; border-radius: 5px 5px 0 0; }}
                .content {{ background-color: #f9f9f9; padding: 20px; border: 1px solid #ddd; }}
                .footer {{ background-color: #333; color: white; padding: 10px; text-align: center; 
                          border-radius: 0 0 5px 5px; font-size: 12px; }}
                .metric {{ margin: 10px 0; padding: 8px; background-color: white; border-left: 3px solid #007bff; }}
                .metric-label {{ font-weight: bold; color: #333; }}
                .metric-value {{ color: #007bff; font-size: 1.1em; }}
                .alert-info {{ background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; 
                              border-radius: 5px; margin: 15px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>{severity_titles.get(severity, '알림')}</h2>
                    <p>PUBG 실시간 이상 탐지 시스템</p>
                </div>
                
                <div class="content">
                    <div class="alert-info">
                        <strong>탐지 시간:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}<br>
                        <strong>플레이어:</strong> {player_name}<br>
                        <strong>매치 ID:</strong> {match_id[:16]}...
                    </div>
                    
                    <h3>🎯 성과 지표</h3>
                    <div class="metric">
                        <span class="metric-label">킬 수:</span>
                        <span class="metric-value">{alert_data.get('kills', 0)}킬</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">데미지:</span>
                        <span class="metric-value">{alert_data.get('damage_dealt', 0):,.0f}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">헤드샷 비율:</span>
                        <span class="metric-value">{alert_data.get('headshot_ratio', 0):.1%}</span>
                    </div>
                    
                    <h3>📊 통계적 이상도 (Z-Score)</h3>
                    <div class="metric">
                        <span class="metric-label">킬 수 이상도:</span>
                        <span class="metric-value">{alert_data.get('kill_zscore', 0):.2f}σ</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">데미지 이상도:</span>
                        <span class="metric-value">{alert_data.get('damage_zscore', 0):.2f}σ</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">헤드샷 이상도:</span>
                        <span class="metric-value">{alert_data.get('headshot_zscore', 0):.2f}σ</span>
                    </div>
                    
                    <div class="alert-info">
                        <strong>📝 참고:</strong> Z-Score가 ±2.5 이상인 경우 통계적으로 이상한 패턴으로 간주됩니다.
                        고위험 알림의 경우 추가 조사가 필요할 수 있습니다.
                    </div>
                </div>
                
                <div class="footer">
                    PUBG Real-time Analytics System | 
                    <a href="http://localhost:3000" style="color: #87ceeb;">Grafana 대시보드</a>
                </div>
            </div>
        </body>
        </html>
        """
        
        # 텍스트 버전 (HTML을 지원하지 않는 이메일 클라이언트용)
        text_content = f"""
PUBG 이상 탐지 알림 - {severity} 심각도

탐지 시간: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
플레이어: {player_name}
매치 ID: {match_id}

성과 지표:
- 킬 수: {alert_data.get('kills', 0)}킬
- 데미지: {alert_data.get('damage_dealt', 0):,.0f}
- 헤드샷 비율: {alert_data.get('headshot_ratio', 0):.1%}

통계적 이상도 (Z-Score):
- 킬 수: {alert_data.get('kill_zscore', 0):.2f}σ
- 데미지: {alert_data.get('damage_zscore', 0):.2f}σ
- 헤드샷: {alert_data.get('headshot_zscore', 0):.2f}σ

※ Z-Score ±2.5 이상은 통계적 이상 패턴입니다.

---
PUBG Real-time Analytics System
대시보드: http://localhost:3000
        """
        
        return subject, html_content, text_content
    
    def send_email_notification(self, alert_data, severity):
        """이메일 알림 전송"""
        if not EMAIL_ENABLED or not EMAIL_USERNAME or not EMAIL_PASSWORD or not EMAIL_TO:
            logging.debug("이메일 설정이 완료되지 않음")
            return False
        
        try:
            subject, html_content, text_content = self.format_email_message(alert_data, severity)
            
            # 이메일 메시지 생성
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = EMAIL_FROM
            msg['To'] = ', '.join([email.strip() for email in EMAIL_TO if email.strip()])
            
            # 텍스트 및 HTML 파트 추가
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # SMTP 서버 연결 및 전송
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()  # TLS 암호화 시작
                server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
                
                # 각 수신자에게 전송
                for email_to in EMAIL_TO:
                    if email_to.strip():
                        server.send_message(msg, to_addrs=[email_to.strip()])
            
            logging.info(f"✅ 이메일 알림 전송 성공: {alert_data.get('player_name')} → {len(EMAIL_TO)}명")
            return True
            
        except smtplib.SMTPAuthenticationError:
            logging.error("❌ 이메일 인증 실패: 사용자명/패스워드를 확인하세요")
            return False
        except smtplib.SMTPRecipientsRefused:
            logging.error("❌ 이메일 수신자 주소가 올바르지 않음")
            return False
        except Exception as e:
            logging.error(f"이메일 전송 중 오류: {e}")
            return False
    
    def determine_severity(self, alert_data):
        """알림 심각도 결정"""
        # None 값을 안전하게 처리
        kills = alert_data.get('kills') or 0
        damage = alert_data.get('damage_dealt') or 0
        kill_zscore = abs(alert_data.get('kill_zscore') or 0)
        damage_zscore = abs(alert_data.get('damage_zscore') or 0)
        headshot_zscore = abs(alert_data.get('headshot_zscore') or 0)
        
        # 모든 값이 숫자인지 확인
        try:
            kills = float(kills) if kills is not None else 0
            damage = float(damage) if damage is not None else 0
            kill_zscore = float(kill_zscore) if kill_zscore is not None else 0
            damage_zscore = float(damage_zscore) if damage_zscore is not None else 0
            headshot_zscore = float(headshot_zscore) if headshot_zscore is not None else 0
        except (ValueError, TypeError):
            logging.warning(f"심각도 판정 중 숫자 변환 실패: {alert_data}")
            return 'LOW'
        
        # HIGH 심각도 조건
        if (kills >= 20 or damage >= 4000 or 
            kill_zscore >= 4 or damage_zscore >= 4 or headshot_zscore >= 4):
            return 'HIGH'
        
        # MEDIUM 심각도 조건
        if (kills >= 15 or damage >= 2500 or 
            kill_zscore >= 3 or damage_zscore >= 3 or headshot_zscore >= 3):
            return 'MEDIUM'
        
        return 'LOW'
    
    def update_alert_stats(self, severity):
        """알림 통계 업데이트"""
        with self.alert_lock:
            self.alert_stats['total_alerts'] += 1
            self.alert_stats['last_alert_time'] = datetime.utcnow()
            
            if severity == 'HIGH':
                self.alert_stats['high_severity'] += 1
            elif severity == 'MEDIUM':
                self.alert_stats['medium_severity'] += 1
    
    def send_periodic_summary(self):
        """주기적 요약 전송 (1시간마다)"""
        if not SLACK_WEBHOOK_URL or self.alert_stats['total_alerts'] == 0:
            return
        
        try:
            summary_message = {
                "username": "PUBG Analytics Summary",
                "icon_emoji": ":chart_with_upwards_trend:",
                "text": "📊 *PUBG 이상 탐지 시스템 - 1시간 요약*",
                "attachments": [
                    {
                        "color": "#36a64f",
                        "fields": [
                            {
                                "title": "총 알림 수",
                                "value": f"{self.alert_stats['total_alerts']}건",
                                "short": True
                            },
                            {
                                "title": "고위험 알림",
                                "value": f"{self.alert_stats['high_severity']}건",
                                "short": True
                            },
                            {
                                "title": "중위험 알림",
                                "value": f"{self.alert_stats['medium_severity']}건",
                                "short": True
                            },
                            {
                                "title": "마지막 알림",
                                "value": self.alert_stats['last_alert_time'].strftime('%H:%M:%S') if self.alert_stats.get('last_alert_time') else "없음",
                                "short": True
                            }
                        ]
                    }
                ]
            }
            
            response = requests.post(SLACK_WEBHOOK_URL, json=summary_message, timeout=10)
            if response.status_code == 200:
                logging.info("📊 시간별 요약 전송 완료")
            
            # 통계 초기화
            self.alert_stats = {
                'total_alerts': 0,
                'high_severity': 0,
                'medium_severity': 0,
                'last_alert_time': None
            }
            
        except Exception as e:
            logging.error(f"요약 전송 중 오류: {e}")
    
    def run(self):
        """메인 실행 함수"""
        logging.info("🚨 PUBG 알림 관리자 시작...")
        
        consumer = self.create_kafka_consumer()
        alerts_processed = 0
        last_summary_time = datetime.utcnow()
        
        try:
            logging.info("실시간 알림 처리 시작...")
            logging.info("메시지 처리 루프 시작 중...")
            
            # 메시지 처리 루프
            for message in consumer:
                logging.debug(f"새 메시지 수신: {message}")
                try:
                    alert_data = message.value
                    player_name = alert_data.get('player_name', 'Unknown')
                    alert_type = alert_data.get('alert_type', 'unknown')
                    
                    # 중복 알림 확인
                    if self.is_duplicate_alert(player_name, alert_type, threshold_minutes=15):
                        logging.debug(f"중복 알림 무시: {player_name} - {alert_type}")
                        continue
                    
                    # 심각도 결정
                    severity = self.determine_severity(alert_data)
                    
                    # 알림 통계 업데이트
                    self.update_alert_stats(severity)
                    
                    # HIGH, MEDIUM 심각도만 알림 전송
                    if severity in ['HIGH', 'MEDIUM']:
                        # Slack 알림 전송
                        slack_success = self.send_slack_notification(alert_data, severity)
                        
                        # 이메일 알림 전송 (HIGH 심각도만)
                        email_success = False
                        if severity == 'HIGH' and EMAIL_ENABLED:
                            email_success = self.send_email_notification(alert_data, severity)
                        
                        # 성공 카운트 (둘 중 하나라도 성공하면 카운트)
                        if slack_success or email_success:
                            alerts_processed += 1
                    
                    # 로그 출력
                    kills_val = alert_data.get('kills') or 0
                    damage_val = alert_data.get('damage_dealt') or 0
                    try:
                        damage_val = float(damage_val)
                    except (ValueError, TypeError):
                        damage_val = 0
                    
                    logging.info(f"🚨 {severity} 알림 처리: {player_name} "
                               f"(킬: {kills_val}, "
                               f"데미지: {damage_val:.0f})")
                    
                    # 1시간마다 요약 전송
                    current_time = datetime.utcnow()
                    if last_summary_time and (current_time - last_summary_time).total_seconds() >= 3600:  # 1시간
                        self.send_periodic_summary()
                        last_summary_time = current_time
                    
                except Exception as e:
                    logging.error(f"알림 처리 중 오류: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logging.info("사용자에 의해 중단됨")
        except Exception as e:
            import traceback
            logging.error(f"예상치 못한 오류: {e}")
            logging.error(f"오류 상세: {traceback.format_exc()}")
        finally:
            consumer.close()
            logging.info(f"알림 관리자 종료. 총 처리: {alerts_processed}건")

if __name__ == "__main__":
    # Slack 연결 확인 (메시지 전송 없이 로그만)
    if SLACK_WEBHOOK_URL:
        logging.info("✅ Slack 웹훅 URL 설정됨")
    else:
        logging.warning("⚠️ Slack 웹훅 URL이 설정되지 않음. Slack 알림이 비활성화됩니다.")
    
    alert_manager = AlertManager()
    alert_manager.run()