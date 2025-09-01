from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# 스파크 세션 생성
spark = SparkSession.builder \
    .appName("PUBG_Streaming_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.session.timeZone", "Asia/Seoul") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 한국 시간대 설정
spark.sql("SET time_zone = 'Asia/Seoul'")

# 카프카 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "pubg-matches") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 스키마 정의
player_schema = StructType([
    # 기본 식별 정보
    StructField("player_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("shard_id", StringType(), True),
    
    # 게임 정보
    StructField("title_id", StringType(), True),
    StructField("patch_version", StringType(), True),
    
    # 계정 상태 정보
    StructField("ban_type", StringType(), True),
    StructField("clan_id", StringType(), True),
    
    # 통계 정보
    StructField("stats", StringType(), True),
    StructField("rank", StringType(), True),
    
    # 매치 관련 정보
    StructField("total_matches_count", IntegerType(), True),
    StructField("match_ids", ArrayType(StringType()), True),
    
    # 메타데이터
    StructField("data_collected_at", StringType(), True),
    StructField("account_created_at", StringType(), True),
    StructField("account_updated_at", StringType(), True),
    
    # API 링크
    StructField("api_links", StringType(), True),
    
    # 원시 속성
    StructField("all_raw_attributes", StringType(), True)
])

match_schema = ArrayType(StructType([
    # 기본 매치 정보
    StructField("match_id", StringType(), True),
    StructField("type", StringType(), True),
    
    # 게임 설정 정보
    StructField("game_mode", StringType(), True),
    StructField("map_name", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("title_id", StringType(), True),
    StructField("shard_id", StringType(), True),
    
    # 매치 상태 및 설정
    StructField("season_state", StringType(), True),
    StructField("is_custom_match", BooleanType(), True),
    StructField("tags", StringType(), True),
    
    # 시간 정보
    StructField("created_at", StringType(), True),
    
    # 원시 데이터에서 추출한 상세 정보
    StructField("patch_version", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("telemetry_url", StringType(), True),
    
    # 참가자 및 로스터 정보
    StructField("rosters_count", IntegerType(), True),
    StructField("participants_count", IntegerType(), True),
    StructField("assets_count", IntegerType(), True),
    
    # API 관련 정보
    StructField("api_links", StringType(), True),
    
    # 전체 원시 속성
    StructField("all_raw_attributes", StringType(), True),
    StructField("relationships_summary", StringType(), True),
    
    # 플레이어 게임 성과 데이터
    StructField("player_performance", StructType([
        # 킬 관련 성과
        StructField("kills", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("headshot_kills", IntegerType(), True),
        StructField("longest_kill", DoubleType(), True),
        StructField("road_kills", IntegerType(), True),
        StructField("team_kills", IntegerType(), True),
        
        # 데미지/생존 관련
        StructField("damage_dealt", DoubleType(), True),
        StructField("time_survived", DoubleType(), True),
        StructField("dbnos", IntegerType(), True),
        StructField("death_type", StringType(), True),
        
        # 힐/부스터
        StructField("heals", IntegerType(), True),
        StructField("boosts", IntegerType(), True),
        StructField("revives", IntegerType(), True),
        
        # 이동/활동
        StructField("walk_distance", DoubleType(), True),
        StructField("ride_distance", DoubleType(), True),
        StructField("swim_distance", DoubleType(), True),
        
        # 순위/기타
        StructField("win_place", IntegerType(), True),
        StructField("kill_place", IntegerType(), True),
        StructField("weapons_acquired", IntegerType(), True),
        StructField("vehicle_destroys", IntegerType(), True),
        
        # 메타 정보
        StructField("participant_name", StringType(), True),
        StructField("participant_id", StringType(), True)
    ]), True)
]))

# 데이터 파싱 및 변환
parsed_df = df.select(
    from_json(col("value").cast("string"),
        StructType([
            StructField("player", player_schema, True),
            StructField("matches", match_schema, True)
        ])
    ).alias("data")
).select("data.*")

# 플레이어 데이터 추출
player_df = parsed_df.select(
    # 기본 정보
    col("player.player_name").alias("player_name"),
    col("player.account_id").alias("account_id"),
    col("player.shard_id").alias("shard_id"),
    
    # 게임 정보
    col("player.title_id").alias("title_id"),
    col("player.patch_version").alias("patch_version"),
    
    # 계정 상태
    col("player.ban_type").alias("ban_type"),
    col("player.clan_id").alias("clan_id"),
    
    # 통계 정보
    col("player.stats").alias("stats_json"),
    col("player.rank").alias("rank_json"),
    
    # 매치 관련
    col("player.total_matches_count").alias("total_matches_count"),
    size(col("player.match_ids")).alias("recent_matches_count"),
    
    # 메타데이터
    col("player.data_collected_at").alias("data_collected_at"),
    col("player.account_created_at").alias("account_created_at"),
    col("player.account_updated_at").alias("account_updated_at"),
    
    # 처리 시간
    current_timestamp().alias("processed_at")
)

# 매치 데이터 추출
match_df = parsed_df.select(
    col("player.player_name").alias("player_name"),
    col("player.account_id").alias("account_id"),
    explode(col("matches")).alias("match")
).select(
    # 플레이어 정보
    col("player_name"),
    col("account_id"),
    
    # 기본 매치 정보
    col("match.match_id").alias("match_id"),
    col("match.type").alias("match_type"),
    
    # 게임 설정
    col("match.game_mode").alias("game_mode"),
    col("match.map_name").alias("map_name"),
    col("match.duration").alias("duration_seconds"),
    (col("match.duration") / 60.0).alias("duration_minutes"),
    
    # 매치 상태
    col("match.season_state").alias("season_state"),
    col("match.is_custom_match").alias("is_custom_match"),
    
    # 참가자 정보
    col("match.rosters_count").alias("teams_count"),
    col("match.participants_count").alias("players_count"),
    col("match.assets_count").alias("assets_count"),
    
    # 시간 정보
    col("match.created_at").alias("match_created_at"),
    
    # 추가 정보
    col("match.patch_version").alias("patch_version"),
    col("match.telemetry_url").alias("telemetry_url"),
    
    # 플레이어 게임 성과 데이터
    col("match.player_performance.kills").alias("player_kills"),
    col("match.player_performance.assists").alias("player_assists"),
    col("match.player_performance.headshot_kills").alias("player_headshot_kills"),
    col("match.player_performance.damage_dealt").alias("player_damage_dealt"),
    col("match.player_performance.time_survived").alias("player_time_survived"),
    col("match.player_performance.heals").alias("player_heals"),
    col("match.player_performance.boosts").alias("player_boosts"),
    col("match.player_performance.walk_distance").alias("player_walk_distance"),
    col("match.player_performance.ride_distance").alias("player_ride_distance"),
    col("match.player_performance.win_place").alias("player_win_place"),
    col("match.player_performance.kill_place").alias("player_kill_place"),
    col("match.player_performance.longest_kill").alias("player_longest_kill"),
    col("match.player_performance.weapons_acquired").alias("player_weapons_acquired"),
    col("match.player_performance.death_type").alias("player_death_type"),
    
    # 처리 시간
    current_timestamp().alias("processed_at")
)

# 중복 제거 후 HDFS에 저장
def write_to_hdfs_deduplicated(df, output_path, checkpoint_path):
    return df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="60 seconds") \
        .start()

# 플레이어 데이터: account_id 기준 중복 제거
player_deduplicated = player_df.dropDuplicates(["account_id", "data_collected_at"])

# 매치 데이터: match_id + account_id 기준 중복 제거  
match_deduplicated = match_df.dropDuplicates(["match_id", "account_id"])

player_query = write_to_hdfs_deduplicated(
    player_deduplicated,
    "hdfs://namenode:8020/pubg/players",
    "hdfs://namenode:8020/pubg/checkpoints/players"
)

match_query = write_to_hdfs_deduplicated(
    match_deduplicated, 
    "hdfs://namenode:8020/pubg/matches",
    "hdfs://namenode:8020/pubg/checkpoints/matches"
)

# 콘솔 출력
console_query = match_df.writeStream\
    .outputMode("append")\
    .format("console")\
    .trigger(processingTime="60 seconds")\
    .start()

# 모든 스트림 종료 까지 대기
spark.streams.awaitAnyTermination()
