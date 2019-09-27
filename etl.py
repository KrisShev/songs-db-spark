import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Starts or retrieves spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Given s3 bucket to songs json, it creates parquet files for songs and artists 
    """
    # get filepath to song data file
    song_data = input_data['json']
    
    # read song data file
    df = spark.read.json(song_data)

    # removing duplicates
    df = df.drop_duplicates()
    
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].repartition('year', 'artist_id')
   
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data['songs'])

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data['artists'])


def process_log_data(spark, input_data, output_data):
    """ Given s3 bucket to logs json, it filters only recrods with attribute 'page' being 'NextSong'. Then it creates parquet files for users and time. 
    Songs and artists parquet from input_data are joined to the logs dataframe to produce songplay.
    """

    # get filepath to log data file
    log_data = input_data['json']
    
    # read log data file
    df = spark.read.json(log_data)
      
    # filter by actions for song plays
    df.where('page="NextSong"') 

    # removing duplicates
    df = df.drop_duplicates()
    
    # extract columns for users table    
    users_table = df['userid', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files
    users_table.write.parquet(output_data['users'])

    # create timestamp column from original timestamp column
    @udf
    def get_timestamp(timestamp):
        return int(timestamp/1000)
    
    df = df.withColumn('start_time', get_timestamp('ts').cast('timestamp')) 
         
    # create datetime column from original timestamp column
    @udf
    def get_datetime(timestamp):
        return str(datetime.fromtimestamp(timestamp/1000))
    
    # extract columns to create time table
    
    df = df.withColumn('start_date', get_datetime('ts')) 
    
    df = df.withColumn('year', year('start_time'))
    df = df.withColumn('month', month('start_time'))
    df = df.withColumn('day', dayofmonth('start_time'))
    df = df.withColumn('hour', hour('start_time'))
    df = df.withColumn('week', weekofyear('start_time'))
    df = df.withColumn('weekday', date_format('start_time', 'EEEE'))
    df = df.repartition("year", "month")
    
    #time_table 
    time_table = df['start_date', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data['time'])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(input_data['songs'])
    artist_df = spark.read.parquet(input_data['artists'])
    artist_df = artist_df.select(col('artist_id').alias('artistid'))
    

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, df.song == song_df.title, how="left")
    df = df.join(artist_df, df.artist_id == artist_df.artistid, how="left")
    
    # adding songplay_id
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    
    songplays_table = df["songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data['songplay'])


def main():
    spark = create_spark_session()
    
    song_input = {'json':'s3a://udacity-dend/song_data/*/*/*/*.json'}
    song_output = {'songs':'songs.parquet', 'artists':'artists.parquet'}
    log_input = {'json':'s3a://udacity-dend/log_data/2018/11/*.json', 'songs':'songs.parquet', 'artists':'artists.parquet'}
    log_output = {'users':'users.parquet', 'time':'time.parquet', 'songplay':'songplays.parquet'}
    process_song_data(spark, song_input, song_output)    
    process_log_data(spark, log_input, log_output)


if __name__ == "__main__":
    main()
