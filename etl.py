import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.functions import from_unixtime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get("default", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("default", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data files from S3 to S3 parquet format
    
            Parameters:
                    spark (SparkSession): an spark session
                    input_data (string): input data base directory
                    output_data (string): output data base directory

            Returns:
                    None
    """
    
    # get filepath to song data file
    song_data =  "{}song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id",
                            "title",
                            "artist_id",
                            "year",
                            "duration")
    
    songs_table = songs_table.dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.\
        partitionBy("year","artist_id").\
        parquet("{}song/songs.parquet".format(output_data), mode="overwrite")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id",
                              "artist_name",
                              "artist_location",
                              "artist_latitude",
                              "artist_longitude")
    
    #rename columns
    artists_table = artists_table.toDF("artist_id",
                                       "name",
                                       "location",
                                       "lattitude",
                                       "longitude")
    
    artists_table = artists_table.dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.\
        parquet("{}artist/artists.parquet".format(output_data), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process log data files from S3 partitioned by year and month to S3 parquet format
    
            Parameters:
                    spark (SparkSession): an spark session
                    input_data (string): input data base directory
                    output_data (string): output data base directory

            Returns:
                    None
    """
    
    # get filepath to log data file
    log_data = '{}log_data/*/*/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")
    
    # extract columns for user table
    users_table =  df.select("userId", "firstName", "lastName", "gender", "level")
    users_table = users_table.dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.\
        parquet("{}user/user.parquet".format(output_data), mode="overwrite")

    # convert long in timestamp
    df = df.withColumn("ts",from_unixtime((df.ts.cast('bigint')/1000)).cast('timestamp'))
    
    # extract columns to create time table
    time_table = df.select(\
                  df.ts.alias('start_time'),    
                  hour(df.ts).alias('hour'), \
                  dayofmonth(df.ts).alias('day'),\
                  weekofyear(df.ts).alias('week'),\
                  month(df.ts).alias('month'),\
                  year(df.ts).alias('year'),\
                  dayofweek(df.ts).alias('weekday'),\
                )
    
    time_table = time_table.dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.\
        partitionBy("year","month").\
        parquet("{}time/time.parquet".format(output_data), mode="overwrite")

    # create columns year and month (partition)
    df = df.withColumn("year",year(df.ts).alias('year'))
    df = df.withColumn("month",month(df.ts).alias('month'))
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}song/songs.parquet".format(output_data))
    
    # extract columns from joined song, artist and log datasets to create songplays table 
    songplays_table = df.join(
                            song_df.select("song_id",
                                           "title",
                                           "artist_id",
                                           "duration").\
                                        alias("songs")
                        ).where(df["song"] == song_df["title"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.\
        partitionBy("year","month").\
        parquet("{}songplays/songplays.parquet".format, mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-udacity-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
