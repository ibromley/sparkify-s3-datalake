import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    '''
    # read song data file
    df = 

    # extract columns to create songs table
    songs_table = 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table
    '''

def process_log_data(spark, input_data, output_data):
    
    # get filepath to log data file
    log_data = input_data + 'log-data/2018/11/2018-11-30-events.json'

    # read log data file
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    #df.filter(df.page == "NextSong")
    # extract columns for users table    
    user_table = df.filter(df.page == "NextSong").select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates().show(10)

    # write users table to parquet files
    #artists_table
    #out_path = "data/sparkify_log_small.csv"
    #user_table.write.save(out_path, format="parquet", header=True)
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    #df = df.withColumn('tsCol', df['ts'].cast('date')).show(10)
    testing = df.select(from_unixtime(col('ts')/1000).alias('ts')).show(10)
    
    # extract columns to create time table
    time_table = testing.select('ts',
                           hour('ts').alias('hour'),
                            year('ts').alias('year')).show(10)
    '''
    # write time table to parquet files partitioned by year and month
    time_table

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
    output_data = ""
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()
