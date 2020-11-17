#import important packages
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
import  pyspark.sql.functions as F

#read dl.cfg
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function will get or create spark session
    
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function will read and extract data from the songs dataset and load it into Songs and Artists tables.
    
    parameters:
    - spark: The spark session
    - input_data: S3 bucket path of songs dataset.
    - output_data: S3 bucket path of loaded songs and artists tables
    '''
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create a view to use with SQL queries
    df.createOrReplaceTempView("v_song_data")

    # create songs table
    songs_table = spark.sql("""
    
        SELECT DISTINCT song_id, title, artist_id, year,duration
        FROM v_song_data 
        
        """)
    
    # save songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data+'songs_table/')


    # create artists table
    artists_table = spark.sql("""select distinct artist_id, artist_name,   artist_location, artist_latitude, artist_longitude from 
    v_song_data""")
    
    # save artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')

def process_log_data(spark, input_data, output_data):
    '''
    This function will read and extract data from the logs dataset and load it into Users, Time and Songplays tables.
    
    parameters:
    - spark: The spark session
    - input_data: S3 bucket path of songs dataset
    - output_data: S3 bucket path of users, time and songplays tables
    '''
    
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*/*.json"
    

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
     #create a view to use with SQL queries
    df.createOrReplaceTempView("v_log_data")

    # create users table
    users_table = spark.sql("""
        
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM v_log_data
        
        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp("ts"))

    
    # create time table
    time_table = df.withColumn("hour",F.hour("start_time"))\
                    .withColumn("day",F.dayofmonth("start_time"))\
                    .withColumn("week",F.weekofyear("start_time"))\
                    .withColumn("month",F.month("start_time"))\
                    .withColumn("year",F.year("start_time"))\
                    .withColumn("weekday",F.dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday")
    time_table = time_table.dropDuplicates(['start_time'])


    
    # save time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+'time_table/')



    # read in song data to use for songplays table
    #song_df = spark.read.json(song_data)
    song_df = spark.read.parquet(output_data+'song/')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" 
    
        SELECT  DISTINCT monotonically_increasing_id() as songplay_id,
        to_timestamp(log.ts/1000) as start_time,
        log.userId, song.song_id, song.artist_id, log.sessionId, log.location, log.userAgent
        FROM v_log_data log
        JOIN
        v_song_data song
        ON  log.song  = song.title
        AND log.artist = song.artist_name
        AND log.length = song.duration
                                 
        """)
    songplays_table = songplays_table.withColumn("month",F.month("start_time")).withColumn("year",F.year("start_time"))


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+'songplays_table/')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
