import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select([
        'song_id', 
        'title', 
        'artist_id', 
        'year'
    ]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode("overwrite")\
        .parquet(os.path.join(output_data, "songs"))

    # extract columns to create artists table
    artists_table = df.select([
        'artist_id', 
        'artist_location', 
        'artist_latitude', 
        'artist_longitude'
    ]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
        .parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/2018/11/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr([
        'userId as user_id', 
        'firstName as first_name', 
        'lastName as last_name', 
        'gender', 
        'level'
    ]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").\
        parquet(os.path.join(output_data, "user"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000)\
        .strftime('%Y-%m-%d %H:%M:%S'))

    df = df.withColumn('timestamp', get_timestamp(df.ts)) 
    
    # extract columns to create time table
    time_table = df.selectExpr('timestamp AS start_time')\
        .withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time'))\
        .withColumn('week', weekofyear('start_time'))\
        .withColumn('month', month('start_time'))\
        .withColumn('year', year('start_time'))\
        .withColumn('weekday', dayofweek('start_time'))\
        .dropDuplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode("overwrite").\
        parquet(os.path.join(output_data, "time"))

    # read in song data to use for songplays table
    song_df = spark.read\
        .json(os.path.join(input_data, "song_data/*/*/*/*.json"))\
        .select("artist_id", "artist_name", "song_id", "title")

    # Join the two daaframes
    df_joined = df.join(
        song_df, 
        (df.song == song_df.title) & (df.artist == song_df.artist_name), 
        "left"
    ).join(
        time_table,
        df.timestamp == time_table.start_time,
        "left"
)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_joined.selectExpr(
        "start_time",
        "userId AS user_id",
        "level",
        "song_id",
        "artist_id",
        "sessionId AS session_id",
        "location",
        "userAgent AS user_agent",
        "year",
        "month"
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month")\
        .parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-project4-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()