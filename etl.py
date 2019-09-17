import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


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
    '''
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table
    '''

def process_data(spark, filepath, func):
    '''
    Given directory, apply function across each file found
    Args:
        cur : sql execution against database
        conn : connection to Postgres instance
        filepath (str): path of the root directory
        func : function applied to each file given
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    for i, datafile in enumerate(all_files, 1):
        func(spark, datafile, None)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "/data/song-data.zip"
    output_data = ""
    
    process_data(spark, filepath='data/song_data', func=process_song_data)

    #process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()
