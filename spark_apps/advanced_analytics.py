"""
PUBG 고급 데이터 분석 시스템
1. 실시간 플레이어 랭킹 시스템
2. 윈도우 함수 기반 통계 계산
3. 이상 탐지 알고리즘
4. 집계 테이블 생성
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("PUBG_Advanced_Analytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.session.timeZone", "Asia/Seoul") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 한국 시간대 설정
spark.sql("SET time_zone = 'Asia/Seoul'")

# 카프카에서 데이터 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "pubg-matches") \
    .option("startingOffsets", "latest") \
    .load()

# 스키마 정의
schema = StructType([
    StructField("player", StructType([
        StructField("player_name", StringType(), True),
        StructField("account_id", StringType(), True)
    ]), True),
    StructField("matches", ArrayType(StructType([
        StructField("match_id", StringType(), True),
        StructField("game_mode", StringType(), True),
        StructField("map_name", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("player_performance", StructType([
            StructField("kills", IntegerType(), True),
            StructField("assists", IntegerType(), True),
            StructField("headshot_kills", IntegerType(), True),
            StructField("damage_dealt", DoubleType(), True),
            StructField("time_survived", DoubleType(), True),
            StructField("walk_distance", DoubleType(), True),
            StructField("ride_distance", DoubleType(), True),
            StructField("win_place", IntegerType(), True),
            StructField("longest_kill", DoubleType(), True),
            StructField("weapons_acquired", IntegerType(), True),
            StructField("death_type", StringType(), True),
            StructField("heals", IntegerType(), True),
            StructField("boosts", IntegerType(), True)
        ]), True)
    ])), True)
])

# 데이터 파싱 및 평면화
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 성과 데이터 평면화
performance_df = parsed_df.select(
    col("player.player_name").alias("player_name"),
    col("player.account_id").alias("account_id"),
    explode(col("matches")).alias("match")
).select(
    col("player_name"),
    col("account_id"),
    col("match.match_id").alias("match_id"),
    col("match.game_mode").alias("game_mode"),
    col("match.map_name").alias("map_name"),
    col("match.duration").alias("duration"),
    col("match.player_performance.*"),
    current_timestamp().alias("processed_at"),
    # 파생 메트릭 계산
    (col("match.player_performance.kills") + col("match.player_performance.assists")).alias("ka_score"),
    (col("match.player_performance.damage_dealt") / greatest(col("match.player_performance.time_survived"), lit(1))).alias("dps"),
    (col("match.player_performance.headshot_kills").cast("double") / greatest(col("match.player_performance.kills"), lit(1))).alias("headshot_ratio"),
    when(col("match.player_performance.win_place") == 1, 1).otherwise(0).alias("is_winner"),
    when(col("match.player_performance.win_place") <= 10, 1).otherwise(0).alias("top10_finish")
).filter(col("kills").isNotNull() & col("damage_dealt").isNotNull())

def process_advanced_analytics(batch_df, epoch_id):
    """고급 분석 처리 함수"""
    
    if batch_df.count() == 0:
        return
        
    print(f"=== 고급 분석 배치 {epoch_id} 처리 시작 ===")
    batch_count = batch_df.count()
    print(f"처리할 레코드 수: {batch_count}")
    
    # 1. 실시간 플레이어 랭킹 시스템
    print("\n🏆 실시간 플레이어 랭킹 계산 중...")
    
    # 플레이어별 종합 성과 점수 계산
    player_stats = batch_df.groupBy("player_name", "account_id") \
        .agg(
            count("*").alias("matches_played"),
            avg("kills").alias("avg_kills"),
            avg("assists").alias("avg_assists"),
            avg("damage_dealt").alias("avg_damage"),
            avg("time_survived").alias("avg_survival"),
            avg("headshot_ratio").alias("avg_headshot_ratio"),
            sum("is_winner").alias("wins"),
            sum("top10_finish").alias("top10_finishes"),
            avg("ka_score").alias("avg_ka_score"),
            avg("dps").alias("avg_dps")
        ).withColumn("win_rate", col("wins") / col("matches_played")) \
         .withColumn("top10_rate", col("top10_finishes") / col("matches_played"))
    
    # 종합 랭킹 점수 계산 (가중 평균)
    ranking_df = player_stats.withColumn("ranking_score",
        col("avg_kills") * 10 +           # 킬 점수 (10점/킬)
        col("avg_assists") * 5 +          # 어시스트 점수 (5점/어시스트)
        col("avg_damage") * 0.01 +        # 데미지 점수 (100데미지당 1점)
        col("avg_survival") * 0.005 +     # 생존 점수 (200초당 1점)
        col("win_rate") * 100 +           # 승률 점수 (100점/승률)
        col("top10_rate") * 50 +          # 상위 10위 점수 (50점/top10률)
        col("avg_headshot_ratio") * 30    # 헤드샷 점수 (30점/헤드샷률)
    ).withColumn("rank", row_number().over(Window.orderBy(desc("ranking_score"))))
    
    print("상위 10명 플레이어 랭킹:")
    ranking_df.select("rank", "player_name", "ranking_score", "avg_kills", 
                     "win_rate", "matches_played") \
              .filter(col("rank") <= 10) \
              .show(10, truncate=False)
    
    # 2. 윈도우 함수 기반 최근 N게임 평균 계산
    print("\n📊 윈도우 함수 기반 최근 게임 분석...")
    
    # 플레이어별 최근 5게임 이동평균
    player_window = Window.partitionBy("player_name") \
                          .orderBy("processed_at") \
                          .rowsBetween(-4, 0)  # 현재 + 이전 4게임
    
    windowed_stats = batch_df.withColumn("recent_5_avg_kills", 
                                       avg("kills").over(player_window)) \
                           .withColumn("recent_5_avg_damage", 
                                     avg("damage_dealt").over(player_window)) \
                           .withColumn("recent_5_avg_survival", 
                                     avg("time_survived").over(player_window)) \
                           .withColumn("game_number", 
                                     row_number().over(Window.partitionBy("player_name")
                                                            .orderBy("processed_at")))
    
    # 최근 성과 개선/악화 플레이어 감지
    performance_trend = windowed_stats.filter(col("game_number") >= 5) \
        .withColumn("kill_trend", 
                   col("kills") - col("recent_5_avg_kills")) \
        .withColumn("damage_trend", 
                   col("damage_dealt") - col("recent_5_avg_damage")) \
        .filter((abs(col("kill_trend")) > 2) | (abs(col("damage_trend")) > 500))
    
    if performance_trend.count() > 0:
        print("📈 성과 변화 감지된 플레이어:")
        performance_trend.select("player_name", "kills", "recent_5_avg_kills", 
                                "kill_trend", "damage_dealt", "recent_5_avg_damage", 
                                "damage_trend") \
                        .show(10, truncate=False)
    
    # 3. 이상 탐지 알고리즘
    print("\n🚨 이상 패턴 탐지 중...")
    
    # 통계적 이상 탐지 (Z-Score 기반)
    stats = batch_df.select(
        avg("kills").alias("avg_kills"),
        stddev("kills").alias("std_kills"),
        avg("damage_dealt").alias("avg_damage"),
        stddev("damage_dealt").alias("std_damage"),
        avg("headshot_ratio").alias("avg_headshot_ratio"),
        stddev("headshot_ratio").alias("std_headshot_ratio")
    ).collect()[0]
    
    # 이상치 임계값 설정 (Z-Score > 2.5)
    anomaly_threshold = 2.5
    
    anomalies = batch_df.withColumn("kill_zscore", 
                                  (col("kills") - stats["avg_kills"]) / stats["std_kills"]) \
                      .withColumn("damage_zscore", 
                                (col("damage_dealt") - stats["avg_damage"]) / stats["std_damage"]) \
                      .withColumn("headshot_zscore", 
                                (col("headshot_ratio") - stats["avg_headshot_ratio"]) / stats["std_headshot_ratio"]) \
                      .filter(
                          (abs(col("kill_zscore")) > anomaly_threshold) |
                          (abs(col("damage_zscore")) > anomaly_threshold) |
                          (abs(col("headshot_zscore")) > anomaly_threshold)
                      )
    
    anomaly_count = anomalies.count()
    if anomaly_count > 0:
        print(f"🔥 {anomaly_count}개의 이상 패턴 발견:")
        anomalies.select("player_name", "kills", "kill_zscore", 
                        "damage_dealt", "damage_zscore", 
                        "headshot_ratio", "headshot_zscore") \
                .show(10, truncate=False)
        
        # 이상 데이터를 별도 토픽으로 전송
        anomaly_alerts = anomalies.select(
            to_json(struct(
                col("player_name"),
                col("match_id"),
                col("kills"),
                col("damage_dealt"), 
                col("headshot_ratio"),
                col("kill_zscore"),
                col("damage_zscore"),
                col("headshot_zscore"),
                lit("statistical_anomaly").alias("alert_type"),
                col("processed_at")
            )).alias("value")
        )
        
        # 카프카로 알림 전송
        anomaly_alerts.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "pubg-alerts") \
            .mode("append") \
            .save()
    else:
        print("✅ 이상 패턴 없음")
    
    # 4. 집계 테이블 생성 (시간대별)
    print("\n📈 시간대별 집계 데이터 생성...")
    
    # 현재 시간 기준으로 시간대별 집계
    current_hour = date_format(current_timestamp(), "yyyy-MM-dd HH")
    
    hourly_aggregates = batch_df.withColumn("hour", date_format(col("processed_at"), "yyyy-MM-dd HH")) \
        .groupBy("hour", "game_mode", "map_name") \
        .agg(
            count("*").alias("total_matches"),
            countDistinct("player_name").alias("unique_players"),
            avg("kills").alias("avg_kills"),
            avg("damage_dealt").alias("avg_damage"),
            avg("time_survived").alias("avg_survival"),
            sum("is_winner").alias("total_wins"),
            avg("headshot_ratio").alias("avg_headshot_ratio")
        )
    
    print("시간대별 게임 통계:")
    hourly_aggregates.show(10, truncate=False)
    
    # 맵별 난이도 분석
    map_difficulty = batch_df.groupBy("map_name") \
        .agg(
            avg("kills").alias("avg_kills"),
            avg("time_survived").alias("avg_survival"),
            avg("damage_dealt").alias("avg_damage"),
            count("*").alias("total_matches")
        ).withColumn("difficulty_score", 
                   (lit(1) / (col("avg_kills") + lit(1))) * 
                   (lit(1) / (col("avg_survival") + lit(1))) * 100) \
         .orderBy(desc("difficulty_score"))
    
    print("\n🗺️ 맵별 난이도 분석 (높을수록 어려움):")
    map_difficulty.show(10, truncate=False)
    
    # 집계 결과를 카프카로 전송
    aggregates_json = hourly_aggregates.select(
        to_json(struct("*")).alias("value")
    )
    
    aggregates_json.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "pubg-aggregates") \
        .mode("append") \
        .save()
    
    print(f"✅ 집계 데이터를 pubg-aggregates 토픽으로 전송")
    print(f"=== 배치 {epoch_id} 고급 분석 완료 ===\n")

# 스트리밍 쿼리 실행
analytics_query = performance_df.writeStream \
    .foreachBatch(process_advanced_analytics) \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

print("🚀 PUBG 고급 데이터 분석 시스템 시작")
print("분석 기능:")
print("- 🏆 실시간 플레이어 랭킹")
print("- 📊 윈도우 함수 기반 이동평균")
print("- 🚨 통계적 이상 탐지")
print("- 📈 시간대별 집계 분석")
print("- 🗺️ 맵별 난이도 분석")
print("\n결과는 'pubg-alerts', 'pubg-aggregates' 토픽으로 전송됩니다.")

# 스트림 대기
analytics_query.awaitTermination()