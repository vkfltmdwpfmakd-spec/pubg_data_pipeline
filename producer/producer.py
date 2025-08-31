import os
import json
import requests
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from pubg_python import PUBG, Shard

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

APIKEY = os.getenv("PUBG_API_KEY")

PLAYER_NAMES = ["ATTACK-SUPERNOVA", "B1ceps-", "6zD666"] # https://op.gg/ko/pubg 에서 가져옴
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "pubg-matches"


# 브로커 시작 확인
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
        )
        logging.info("Kafka 프로듀서가 성공적으로 연결되었습니다.")
    except NoBrokersAvailable:
        logging.warning("Kafka 브로커에 연결할 수 없습니다. 5초 후 재시도합니다.")
        time.sleep(5)

pubg = PUBG(APIKEY, Shard.STEAM)


def fetch_player_and_matches(player_name):
    """단일 플레이어의 정보와 매치 데이터를 가져옵니다."""
    try:
        # 플레이어 검색
        players = pubg.players().filter(player_names=[player_name])
        
        if not players:
            logging.warning(f"{player_name} 플레이어를 찾을 수 없습니다.")
            return None
        
        player = players[0]  # 첫 번째 플레이어 선택
        logging.info(f"{player_name} 플레이어를 찾았습니다. ID: {player.id}")
        
        
        # 원시 데이터에서 추가 정보 추출
        raw_data = getattr(player, '_raw_data', {})
        player_attributes = getattr(player, 'attributes', {})
        
        logging.info(f"[{player_name}] PLAYER raw_data 구조:")
        if raw_data:
            logging.info(f"  - raw_data keys: {list(raw_data.keys())}")
            if 'data' in raw_data:
                logging.info(f"  - raw_data['data'] keys: {list(raw_data['data'].keys())}")
                if 'attributes' in raw_data['data']:
                    logging.info(f"  - raw_data['data']['attributes'] keys: {list(raw_data['data']['attributes'].keys())}")
                if 'relationships' in raw_data['data']:
                    logging.info(f"  - raw_data['data']['relationships'] keys: {list(raw_data['data']['relationships'].keys())}")

        
        # 플레이어 데이터 구성 (모든 계정 정보 포함)
        player_data = {
            # === 기본 식별 정보 ===
            "player_name": player_name,                                        # 플레이어 닉네임
            "account_id": player.id,                                          # 고유 계정 ID
            "shard_id": player.shard_id,                                      # 서버 샤드 (steam, kakao 등)
            
            # === 게임 정보 ===
            "title_id": getattr(player, 'title_id', None),                    # 게임 타이틀 ID (pubg)
            "patch_version": getattr(player, 'patch_version', None),          # 현재 패치 버전
            
            # === 계정 상태 정보 ===
            "ban_type": player_attributes.get('banType', None),               # 밴 상태 (Innocent, Banned 등)
            "clan_id": player_attributes.get('clanId', None),                 # 소속 클랜 ID
            
            # === 통계 및 랭킹 정보 ===
            "stats": getattr(player, 'stats', None),                         # 플레이어 통계 데이터
            "rank": getattr(player, 'rank', None),                           # 현재 랭크 정보
            
            # === 매치 관련 정보 ===
            "total_matches_count": len(player.matches) if player.matches else 0,  # 총 매치 수
            "match_ids": [m.id for m in player.matches[:5]],                      # 최근 5경기 매치 ID
            
            # === 메타데이터 ===
            "data_collected_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), # 데이터 수집 시각
            
            # === 원시 데이터에서 추출한 추가 정보 ===
            "account_created_at": raw_data.get('data', {}).get('attributes', {}).get('createdAt', None),  # 계정 생성일
            "account_updated_at": raw_data.get('data', {}).get('attributes', {}).get('updatedAt', None),  # 계정 최종 업데이트일
            "api_links": raw_data.get('data', {}).get('links', {}),                                      # API 관련 링크들
            
            # === 전체 원시 속성 (분석용) ===
            "all_raw_attributes": player_attributes,                         # 모든 원시 속성 데이터
        }

        # 매치 데이터 수집 (최근 5경기)
        matches_data = []
        for match_ref in player.matches[:5]:
            try:
                match = pubg.matches().get(match_ref.id)
                
                # 매치 원시 데이터 추출
                match_raw_data = getattr(match, '_raw_data', {})
                match_attributes = getattr(match, 'attributes', {})
                match_relationships = getattr(match, 'relationships', {})
                
                match_data = {
                    # === 기본 매치 정보 ===
                    "match_id": match.id,                                           # 매치 고유 ID
                    "type": getattr(match, 'type', None),                          # 객체 타입 (match)
                    
                    # === 게임 설정 정보 ===
                    "game_mode": getattr(match, 'game_mode', None),                # 게임 모드 (solo, duo, squad 등)
                    "map_name": getattr(match, 'map_name', None),                  # 맵 이름 (Erangel, Miramar 등)
                    "duration": getattr(match, 'duration', None),                  # 게임 진행 시간 (초)
                    "title_id": getattr(match, 'title_id', None),                  # 게임 타이틀 ID
                    "shard_id": getattr(match, 'shard_id', None),                  # 서버 샤드
                    
                    # === 매치 상태 및 설정 ===
                    "season_state": getattr(match, 'season_state', None),          # 시즌 상태
                    "is_custom_match": getattr(match, 'is_custom_match', None),    # 커스텀 매치 여부
                    "tags": getattr(match, 'tags', None),                          # 매치 태그
                    
                    # === 시간 정보 ===
                    "created_at": str(getattr(match, 'created_at', None)) if getattr(match, 'created_at', None) else None,  # 매치 생성 시간
                    
                    # === 원시 데이터에서 추출한 상세 정보 ===
                    "patch_version": match_attributes.get('patchVersion', None),   # 매치 시점의 패치 버전
                    "match_type": match_attributes.get('matchType', None),         # 매치 타입 (official, custom 등)
                    "telemetry_url": match_attributes.get('telemetryUrl', None),  # 텔레메트리 데이터 URL
                    
                    # === 참가자 및 로스터 정보 ===
                    "rosters_count": len(match_relationships.get('rosters', {}).get('data', [])) if match_relationships else 0,  # 팀 수
                    "participants_count": sum(len(roster.participants) for roster in match.rosters) if hasattr(match, 'rosters') and match.rosters else 0,  # 실제 참가자 수
                    "assets_count": len(match_relationships.get('assets', {}).get('data', [])) if match_relationships else 0,  # 자산 수
                    
                    # === API 관련 정보 ===
                    "api_links": match_raw_data.get('data', {}).get('links', {}),  # API 관련 링크들
                    
                    # === 전체 원시 속성 (분석용) ===
                    "all_raw_attributes": match_attributes,                        # 모든 원시 매치 속성
                    "relationships_summary": {                                     # 관계 데이터 요약
                        "has_rosters": bool(match_relationships.get('rosters')),
                        "has_participants": bool(match_relationships.get('participants')),
                        "has_assets": bool(match_relationships.get('assets'))
                    }
                }
                
                # === 대상 플레이어의 게임 성과 데이터 추출 ===
                player_performance = None
                if hasattr(match, 'rosters') and match.rosters:
                    for roster in match.rosters:
                        if hasattr(roster, 'participants') and roster.participants:
                            for participant in roster.participants:
                                if hasattr(participant, 'player_id') and participant.player_id == player.id:
                                    player_performance = {
                                        # === 킬 관련 성과 ===
                                        "kills": getattr(participant, 'kills', 0),
                                        "assists": getattr(participant, 'assists', 0),
                                        "headshot_kills": getattr(participant, 'headshot_kills', 0),
                                        "longest_kill": getattr(participant, 'longest_kill', 0),
                                        "road_kills": getattr(participant, 'road_kills', 0),
                                        "team_kills": getattr(participant, 'team_kills', 0),
                                        
                                        # === 데미지/생존 관련 ===
                                        "damage_dealt": getattr(participant, 'damage_dealt', 0),
                                        "time_survived": getattr(participant, 'time_survived', 0),
                                        "dbnos": getattr(participant, 'dbnos', 0),  # 다운시킨 적 수
                                        "death_type": getattr(participant, 'death_type', None),
                                        
                                        # === 힐/부스터 ===
                                        "heals": getattr(participant, 'heals', 0),
                                        "boosts": getattr(participant, 'boosts', 0),
                                        "revives": getattr(participant, 'revives', 0),
                                        
                                        # === 이동/활동 ===
                                        "walk_distance": getattr(participant, 'walk_distance', 0),
                                        "ride_distance": getattr(participant, 'ride_distance', 0),
                                        "swim_distance": getattr(participant, 'swim_distance', 0),
                                        
                                        # === 순위/기타 ===
                                        "win_place": getattr(participant, 'win_place', 0),
                                        "kill_place": getattr(participant, 'kill_place', 0),
                                        "weapons_acquired": getattr(participant, 'weapons_acquired', 0),
                                        "vehicle_destroys": getattr(participant, 'vehicle_destroys', 0),
                                        
                                        # === 메타 정보 ===
                                        "participant_name": getattr(participant, 'name', None),
                                        "participant_id": getattr(participant, 'id', None)
                                    }
                                    break
                        if player_performance:
                            break
                
                # 매치 데이터에 플레이어 성과 추가
                match_data["player_performance"] = player_performance
                matches_data.append(match_data)
                
                logging.info(f"매치 데이터 수집 완료: {match.id} (참가자: {match_data['participants_count']}명, 팀: {match_data['rosters_count']}개)")
            except Exception as e:
                logging.error(f"매치 {match_ref.id} 데이터 수집 실패: {e}")
                continue

        return {"player": player_data, "matches": matches_data}
        
    except Exception as e:
        logging.error(f"{player_name} 플레이어 데이터 수집 중 오류 발생: {e}")
        return None


def send_all_players_data():
    """모든 플레이어의 데이터를 수집하고 Kafka에 전송합니다."""
    all_data = []
    
    for player_name in PLAYER_NAMES:
        logging.info(f"{player_name} 데이터 수집 시작...")
        data = fetch_player_and_matches(player_name)
        
        if data:
            all_data.append(data)
            logging.info(f"{player_name} 데이터 수집 완료")
        else:
            logging.warning(f"{player_name} 데이터 수집 실패")
        
        # API 레이트 리미트을 피하기 위한 딜레이
        time.sleep(2)
    
    # 수집된 데이터를 Kafka에 전송
    if all_data:
        try:
            for data in all_data:
                producer.send(KAFKA_TOPIC, value=data)
            producer.flush()  # 즉시 전송 보장
            logging.info(f"총 {len(all_data)}개의 플레이어 데이터를 Kafka에 전송 완료")
        except Exception as e:
            logging.error(f"Kafka 전송 실패: {e}")
    else:
        logging.warning("전송할 데이터가 없습니다.")


if __name__ == "__main__":
    logging.info("PUBG 데이터 프로듀서 시작...")
    
    while True:
        try:
            send_all_players_data()
            logging.info("다음 수집까지 60초 대기...")
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("프로그램이 사용자에 의해 중단되었습니다.")
            break
        except Exception as e:
            logging.error(f"예상치 못한 오류 발생: {e}")
            time.sleep(10)  # 오류 발생시 잠시 대기 후 재시도
    
    # 프로듀서 종료
    if producer:
        producer.close()
        logging.info("Kafka 프로듀서가 종료되었습니다.")