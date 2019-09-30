import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Initializes global spark session object with relevant packages and AWS configs
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' 
    Extracts log data from S3 into separate songs and artists
    dataframes, transforms, and loads back into a different S3 bucket
    Args:
        spark : the current spark session
        input_data : input S3 bucket path containing .json files
        output_data : output S3 bucket path containing .parquet files
    '''
    song_data = input_data + "song_data/"
    df = spark.read.json(song_data)

    # Songs table
    song_table = (df.dropna(how = "any", subset = ["song_id", "artist_id"])
                    .where(df["song_id"].isNotNull())
                    .select(["song_id", "title", "artist_id", "year", "duration"])
                    .dropDuplicates())
    (song_table.write.mode("overwrite")
                     .partitionBy("year", "artist_id")
                     .parquet(output_data + "songs.parquet"))

    # Arists table
    artist_table = (df.dropna(how = "any", subset = ["artist_id"])
                      .where(df["artist_id"].isNotNull())
                      .selectExpr("artist_id", "artist_name as name", "artist_location as location",
                              "artist_latitude as latitude", "artist_longitude as longitude")
                      .dropDuplicates())
    artist_table.write.mode("overwrite").parquet(output_data + "artists.parquet")

def process_log_data(spark, input_data, output_data):
    ''' 
    Extracts log data from S3 into separate users, time, and songplays
    dataframes, transforms, and loads back into a different S3 bucket
    Args:
        spark : the current spark session
        input_data : input S3 bucket path containing .json files
        output_data : output S3 bucket path containing .parquet files
    '''
    log_data = input_data + "log-data/"
    df = spark.read.json(log_data)
    df = df.filter(df.page == "NextSong")

    # Users table   
    user_table = (df.where(df["userId"].isNotNull())
                    .selectExpr("userId as user_id", "firstName as first_name",
                               "lastName as last_name", "gender", "level")
                    .dropDuplicates())
    user_table.write.mode("overwrite").parquet(output_data + "users.parquet")
    
    # Time table
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000.0))
    time_table = (df.withColumn("hour", hour(df.start_time))
                    .withColumn("day", dayofmonth(df.start_time))
                    .withColumn("week", weekofyear(df.start_time))
                    .withColumn("month", month(df.start_time))
                    .withColumn("year", year(df.start_time))
                    .withColumn("weekday", dayofweek(df.start_time))
                    .select(["start_time", "hour", "day", "week", "month", "year", "weekday"])
                    .dropDuplicates())
    (time_table.write.mode("overwrite")
                     .partitionBy("year", "month")
                     .parquet(output_data + "time.parquet"))
    
    # Songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet") 
    songplays_table = (df.join(song_df, df.song == song_df.title, how="left")
                         .withColumn("songplay_id", monotonically_increasing_id())
                         .withColumn("year", year(df.start_time))
                         .withColumn("month", month(df.start_time))
                         .selectExpr("songplay_id", "start_time", "year", "month", "userId as user_id", "level", 
                                     "song_id", "artist_id", "sessionId as session_id", "location", 
                                     "userAgent as user_agent"))
    (songplays_table.write.mode("overwrite")
                          .partitionBy("year", "month")
                          .parquet(output_data + "songplays.parquet"))

def main():
    '''
    Creates a new spark session and loads data from an S3 source
    and further transforms it into fact and dimensional tables.
    The data warehouse is fully populated in a different S3 bucket 
    after sucessful execution.
    '''
    spark = create_spark_session()
    input_data = config.get('S3','INPUT_DATA')
    output_data = config.get('S3','OUTPUT_DATA')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
