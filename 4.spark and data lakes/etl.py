import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config.get('AWS_DATA','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWS_DATA','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """Creates or retrives a Spark Session

    Returns:
        [SparkSession]: [The SparkSession object]
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Creates the ETL process for the song files, loading the JSON files and generating
        the correspoding dimensional tables.

    Args:
        spark (SparkSession): [A valid SparkSessionObject]
        input_data ([str]): [The song input dir]
        output_data ([str]): [The song output dir]
    """    
    
    # get filepath to song data file
    song_data = "{}/*/*/*/*.json".format(input_data)
    
    # define the data schema, notice this is an ETL approach instead of ELT
    # this approach speeds up the process by avoiding per-file schema inference
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", LongType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])
    
    # read the JSON files, loading them, removing duplicates and caching the dataframe
    songs_df = spark.read.json(song_data, schema = song_schema).dropDuplicates().cache()

    # eextract attributes to create the songs dimension
    songs_table = songs_df.select(['song_id', 'artist_id', 'title', 'year', 'duration']).distinct()

    # Dump the Songs dimension table to a parquet file partition by year and artist_id
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet("{}songs/songs_table.parquet".format(output_data), mode="overwrite")
    
    # extract attributes to create the artists dimension
    artists_table = songs_df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude', 'year']).distinct()

    # Dump the Artists dimension table to a parquet file partition by year
    artists_table.write.partitionBy('year') \
        .parquet("{}artists/artists_table.parquet".format(output_data), mode="overwrite")
    
    # Create a temporary songs view which can then be queried
    songs_table.createOrReplaceTempView("song_df_table")


def process_log_data(spark, input_data, output_data):
    """Creates the ETL process for the log files, loading the JSON files and generating
        the correspoding dimensional tables.

    Args:
       spark (SparkSession): [A valid SparkSessionObject]
        input_data ([str]): [The log input dir]
        output_data ([str]): [The log output dir]
    """    
    # get filepath to log data file
    log_data = "{}/*/*/*.json".format(input_data)
    
    log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionID", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    
    # read log data file
    log_df = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong').dropDuplicates().cache()
    
    # Users table
    users_output_format = ['user_id','first_name','last_name', 'gender', 'level']
    users = log_df.select(['userID', 'firstName', 'lastName', 'gender', 'level']).distinct()
    
    # Dump the User dimension table to a parquet file
    users.write.parquet("{}users/user_table.parquet".format(output_data), mode="overwrite")

    # Rename the users table
    users = users.toDF(*users_output_format)

    # convert to timestamp
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp(col("ts")))

    # Time table
    # extract columns to create time table
    time_df = log_df.withColumn("hour", hour("start_time"))
    time_df = time_df.withColumn("day", dayofmonth("start_time"))
    time_df = time_df.withColumn("month", month("start_time"))
    time_df = time_df.withColumn("year", year("start_time"))
    time_df = time_df.withColumn("week", weekofyear("start_time"))
    time_df = time_df.withColumn("weekday", dayofweek("start_time"))

    time_table = time_df.select(["start_time","hour","day","week", "month", "year", "weekday"]).distinct()

    # Dump the Time dimension table to a parquet file partition by year
    time_table.write.partitionBy('year', 'month') \
        .parquet("{}time/time_table.parquet".format(output_data), mode="overwrite")

    
    # Songplays
    songplays_output_format = ['year', 'month', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', \
                               'session_id', 'location', 'user_agent', 'songplay_id']

    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table")
    
    songplays_table = log_df.join(songs_df, (log_df.song == songs_df.title) & (log_df.artist == songs_df.artist_name), 'inner') \
        .withColumn("month", month("start_time")) \
        .select(['year','month','start_time', 'userID', 'level', 'song_id', 'artist_id', 'sessionID', 'location', 'userAgent']) \
        .distinct() \
        .withColumn("songplay_id", monotonically_increasing_id())

    # Rename the songplays table
    songplays_table = songplays_table.toDF(*songplays_output_format)

    # Dump the Songplays Fact table to a parquet file partition by year
    songplays_table.write.partitionBy('year', 'month') \
        .parquet("{}songplays/songplays_table.parquet".format(output_data), mode="overwrite")

def main():
    spark = create_spark_session()
    
    # Credentials are set via Environment Variables, skip per Context config
    #spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    #spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

    input_data_song = "s3a://udacity-dend/song-data/"
    input_data_log = "s3://udacity-dend/log-data/"
    output_data = "s3a://output-udacity-spark/"
    
    process_song_data(spark, input_data_song, output_data)    
    process_log_data(spark, input_data_log, output_data)

main()