#!/usr/bin/env python3
"""
직접 알림 테스트 스크립트 (Kafka 우회)
Alert Manager를 거치지 않고 직접 Slack과 이메일로 알림을 전송하는 테스트
"""

import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
import json
import time
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 환경 변수 로드
load_dotenv()

def send_slack_notification(message, title="PUBG 시스템 직접 테스트"):
    """
    Slack으로 직접 알림 전송
    """
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        logger.error("SLACK_WEBHOOK_URL이 설정되지 않았습니다")
        return False
    
    try:
        payload = {
            "text": f"*{title}*\n{message}",
            "username": "PUBG Test Bot",
            "icon_emoji": ":test_tube:"
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            logger.info("Slack 알림 전송 성공")
            return True
        else:
            logger.error(f"Slack 알림 전송 실패: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Slack 알림 전송 중 오류: {e}")
        return False

def send_email_notification(message, title="PUBG 시스템 직접 테스트"):
    """
    이메일로 직접 알림 전송
    """
    email_enabled = os.getenv('EMAIL_ENABLED', 'false').lower() == 'true'
    if not email_enabled:
        logger.info("이메일 알림이 비활성화되어 있습니다")
        return False
    
    try:
        username = os.getenv('EMAIL_USERNAME')
        password = os.getenv('EMAIL_PASSWORD')
        from_email = os.getenv('EMAIL_FROM', username)
        to_emails = os.getenv('EMAIL_TO', '').split(',')
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        
        if not username or not password:
            logger.error("이메일 인증 정보가 설정되지 않았습니다")
            return False
        
        # 이메일 메시지 구성
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = title
        
        # HTML 본문
        html_body = f"""
        <html>
        <body>
            <h2>{title}</h2>
            <div style="background-color: #f0f0f0; padding: 15px; border-radius: 5px;">
                <pre>{message}</pre>
            </div>
            <br>
            <p><strong>전송 시간:</strong> {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><em>이 메시지는 PUBG 데이터 파이프라인 시스템의 직접 테스트입니다.</em></p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        
        # SMTP 서버 연결 및 전송
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(username, password)
            text = msg.as_string()
            server.sendmail(from_email, to_emails, text)
        
        logger.info("이메일 알림 전송 성공")
        return True
        
    except Exception as e:
        logger.error(f"이메일 알림 전송 중 오류: {e}")
        return False

def run_direct_notification_test():
    """
    직접 알림 테스트 실행
    """
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    test_messages = [
        {
            "title": "[직접 테스트] Slack 연결 확인",
            "message": f"""
직접 Slack 알림 테스트입니다!

테스트 시간: {current_time}
테스트 유형: Slack Webhook 직접 호출
목적: Alert Manager 우회 후 Slack 연결 상태 확인

이 메시지가 표시되면 Slack 연결이 정상입니다.
"""
        },
        {
            "title": "[직접 테스트] 이메일 연결 확인", 
            "message": f"""
직접 이메일 알림 테스트입니다!

테스트 시간: {current_time}
테스트 유형: SMTP 직접 연결
목적: Alert Manager 우회 후 이메일 연결 상태 확인

이 메시지가 수신되면 이메일 연결이 정상입니다.
"""
        },
        {
            "title": "[직접 테스트] 통합 알림 시스템 확인",
            "message": f"""
PUBG 데이터 파이프라인 통합 알림 테스트 완료!

확인된 사항:
• Slack 웹훅 연결: 정상
• 이메일 SMTP 연결: 정상  
• 직접 알림 시스템: 정상 작동

테스트 완료 시간: {current_time}
다음 단계: Alert Manager를 통한 Kafka 기반 알림 테스트

모든 직접 알림 채널이 정상적으로 작동하고 있습니다!
"""
        }
    ]
    
    logger.info("=== 직접 알림 시스템 테스트 시작 ===")
    
    success_count = 0
    total_tests = len(test_messages) * 2  # Slack + Email 각각
    
    for i, test_msg in enumerate(test_messages, 1):
        logger.info(f"테스트 {i}/{len(test_messages)}: {test_msg['title']}")
        
        # Slack 테스트
        slack_success = send_slack_notification(test_msg['message'], test_msg['title'])
        if slack_success:
            success_count += 1
            logger.info(f"  Slack 전송 성공")
        else:
            logger.error(f"  Slack 전송 실패")
        
        # Email 테스트
        email_success = send_email_notification(test_msg['message'], test_msg['title'])
        if email_success:
            success_count += 1
            logger.info(f"  이메일 전송 성공")
        else:
            logger.error(f"  이메일 전송 실패")
        
        # 각 테스트 사이에 2초 간격
        if i < len(test_messages):
            logger.info("  다음 테스트까지 2초 대기...")
            time.sleep(2)
    
    logger.info("=== 직접 알림 시스템 테스트 완료 ===")
    logger.info(f"성공률: {success_count}/{total_tests} ({success_count/total_tests*100:.1f}%)")
    
    if success_count == total_tests:
        logger.info("[SUCCESS] 모든 직접 알림 테스트 성공!")
        logger.info("[INFO] Slack과 이메일 모두 정상 작동 중입니다.")
        return True
    else:
        logger.error(f"[ERROR] {total_tests-success_count}개의 알림 테스트 실패!")
        logger.info("[INFO] 실패한 알림 채널을 확인해주세요.")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("[직접 테스트] PUBG 알림 시스템 (Alert Manager 우회)")
    print("=" * 60)
    
    success = run_direct_notification_test()
    
    if success:
        print("\n[SUCCESS] 직접 알림 테스트 완료!")
        print("[INFO] 모든 알림 채널이 정상적으로 작동합니다.")
    else:
        print("\n[ERROR] 일부 알림 테스트 실패!")
        print("[INFO] 설정을 확인해주세요.")