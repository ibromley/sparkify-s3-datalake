import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format, to_timestamp


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    
    song_data = input_data + 'song_data/A/A/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_table = df.dropna(how = "any", subset = ["song_id", "artist_id"])\
                .where(df["song_id"].isNotNull())\
                .select(["song_id", "title", "artist_id", "year", "duration"])\
                .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.parquet(output_data + "song_table.parquet")

    # extract columns to create artists table
    artist_table = df.dropna(how = "any", subset = ["artist_id"])\
                  .where(df["artist_id"].isNotNull())\
                  .select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
                  .dropDuplicates()
    
    # write artists table to parquet files
    artist_table.write.parquet(output_data + "artist_table.parquet")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/2018/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    #df = 

    # extract columns for users table    
    user_table = df.filter(df.page == "NextSong")\
                   .where(df["userId"].isNotNull())\
                   .select(["userId", "firstName", "lastName", "gender", "level"])\
                   .dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "user_table.parquet")
    
    # create timestamp column from original timestamp column
    #get_timestamp = udf(lambda x: to_timestamp(x / 1000.0)
    #df = df.withColumn("tstmp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn("tstmp", to_timestamp(df.ts / 1000.0))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour(df.tstmp))\
             .withColumn("day", dayofmonth(df.tstmp))\
             .withColumn("week_of_year", weekofyear(df.tstmp))\
             .withColumn("month", month(df.tstmp))\
             .withColumn("year", year(df.tstmp))\
             .withColumn("dow", dayofweek(df.tstmp))\
             .select(["tstmp", "hour", "day", "week_of_year", "month", "year", "dow"])\
             .dropDuplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table.parquet")
'''
    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table
'''
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-ib/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
