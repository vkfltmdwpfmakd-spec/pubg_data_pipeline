#!/usr/bin/env python3
"""
Kafka를 통한 Alert Manager 테스트
- Kafka에 테스트 메시지 전송
- Alert Manager가 메시지를 수신해서 Slack/이메일로 전송하는 전체 플로우 테스트
"""

import json
import time
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_test_alerts():
    """
    테스트 알림 메시지들을 Kafka에 전송하여 Slack/이메일 알림 시스템 테스트
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # Alert Manager가 인식하는 형식의 테스트 알림 메시지들
        test_alerts = [
            {
                "alert_type": "high_performance_test",
                "player_name": "TestPlayer_HIGH",
                "match_id": "test-match-high-12345678",
                "kills": 25,  # HIGH 심각도를 위한 높은 킬 수
                "damage_dealt": 4500,
                "headshot_ratio": 0.8,
                "kill_zscore": 4.2,
                "damage_zscore": 3.8,
                "headshot_zscore": 4.5,
                "timestamp": int(time.time()),
                "test_message": f"테스트 시간: {current_time} - HIGH 심각도 테스트"
            },
            {
                "alert_type": "medium_performance_test", 
                "player_name": "TestPlayer_MEDIUM",
                "match_id": "test-match-medium-87654321",
                "kills": 16,  # MEDIUM 심각도를 위한 중간 킬 수
                "damage_dealt": 2800,
                "headshot_ratio": 0.6,
                "kill_zscore": 3.2,
                "damage_zscore": 3.1,
                "headshot_zscore": 3.0,
                "timestamp": int(time.time()),
                "test_message": f"테스트 시간: {current_time} - MEDIUM 심각도 테스트"
            },
            {
                "alert_type": "info_test",
                "player_name": "TestPlayer_INFO",
                "match_id": "test-match-info-11223344",
                "kills": 5,  # 낮은 킬 수 (LOW 심각도)
                "damage_dealt": 800,
                "headshot_ratio": 0.3,
                "kill_zscore": 1.2,
                "damage_zscore": 0.8,
                "headshot_zscore": 1.0,
                "timestamp": int(time.time()),
                "test_message": f"테스트 시간: {current_time} - 정상 범위 테스트"
            }
        ]
        
        logger.info("🧪 Alert Manager 호환 알림 테스트 시작...")
        logger.info("📱 HIGH/MEDIUM 심각도 알림은 Slack과 📧 이메일로 전송됩니다.")
        
        for i, alert in enumerate(test_alerts, 1):
            try:
                # 알림을 Kafka pubg-alerts 토픽으로 전송
                producer.send('pubg-alerts', key=f'test-alert-{i}', value=alert)
                logger.info(f"✅ 테스트 알림 {i}/3 전송 완료: {alert['player_name']} (킬: {alert['kills']}, 데미지: {alert['damage_dealt']})")
                
                # 각 알림 사이에 3초 간격
                if i < len(test_alerts):
                    logger.info(f"⏳ 다음 알림까지 3초 대기...")
                    time.sleep(3)
                    
            except Exception as e:
                logger.error(f"❌ 알림 {i} 전송 실패: {e}")
        
        # 모든 메시지 전송 완료 대기
        producer.flush()
        producer.close()
        
        logger.info("🎉 모든 테스트 알림 전송 완료!")
        logger.info("📢 이제 다음을 확인해주세요:")
        logger.info("   1. 📱 Slack에서 HIGH/MEDIUM 심각도 알림 확인 (2개 예상)")
        logger.info("   2. 📧 이메일에서 HIGH 심각도 알림 확인 (1개 예상)")
        logger.info("   3. ⏰ Alert Manager 로그에서 처리 상태 확인")
        logger.info("")
        logger.info("💡 Alert Manager 로그 확인 명령어:")
        logger.info("   docker-compose logs alert-manager --tail=20")
        
    except Exception as e:
        logger.error(f"❌ 테스트 실행 중 오류 발생: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("[TEST] PUBG 알림 시스템 테스트 (Kafka → Alert Manager)")
    print("=" * 60)
    
    success = send_test_alerts()
    
    if success:
        print("\n[SUCCESS] 테스트 전송 완료!")
        print("[INFO] Slack과 이메일을 확인해서 메시지가 도착했는지 확인해주세요.")
    else:
        print("\n[ERROR] 테스트 실행 실패!")
        print("[INFO] Kafka 연결이나 설정을 확인해주세요.")