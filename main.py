# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Display schemas
#logs_df.printSchema()
#songs_df.printSchema()

# Task 1: User Favorite Genres
# Join logs with songs metadata
logs_with_genre = logs_df.join(songs_df, on="song_id", how="inner")
fav_genre = logs_with_genre.groupBy("user_id", "genre") \
	.count() \
	.withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("count"))))
fav_genre = fav_genre.filter(col("rank") == 1).select("user_id", "genre", "count")
fav_genre = fav_genre.orderBy(desc("count")) 
fav_genre.show(10) #show top 10 

# Task 2: Average Listen Time
avg_listen_time = logs_df.groupBy("user_id") \
	.agg(avg("duration_sec").alias("avg_duration_sec"))
avg_listen_time = avg_listen_time.orderBy(desc("avg_duration_sec"))
avg_listen_time.show(10) 

# Task 3: Genre Loyalty Scores (total listen time per genre per user, ranked)
genre_loyalty = logs_with_genre.groupBy("user_id", "genre") \
	.agg(sum("duration_sec").alias("loyalty_score"))
top_loyal_users = genre_loyalty.orderBy(desc("loyalty_score")).limit(10)
top_loyal_users.show()

# Task 4: Identify users who listen between 12 AM and 5 AM
from pyspark.sql.types import TimestampType
logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
night_logs = logs_df.filter((hour(col("timestamp")) >= 0) & (hour(col("timestamp")) < 5))
night_users = night_logs.select("user_id").distinct()
night_users.show()
