from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, ArrayType, StructField, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, from_json
from pyspark.sql import DataFrame

def main():
    spark = SparkSession.builder.appName("YoutubeDataStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

        
    #Log level
    spark.sparkContext.setLogLevel("WARN")
    
    #Youtube data schema
    youtubeSchema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("published_date", StringType(), True),
        StructField("channel_id", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("view_count", LongType(), True),
        StructField("like_count", LongType(), True),
        StructField("comment_count", LongType(), True),
        StructField("category_id", StringType(), True),
        StructField("topic_categories", ArrayType(StringType()), True),
        StructField("country", StringType(), True)
    ])
    
    #Music data schema
    musicSchema = StructType([
        StructField("id", StringType(), True),  
        StructField("Name", StringType(), True),  
        StructField("Artist", StringType(), True), 
        StructField("Album", StringType(), True), 
        StructField("Release Date", StringType(), True),  
        StructField("Genres", ArrayType(StringType()), True),  
        StructField("Popularity", IntegerType(), True), 
        StructField("Duration (minutes)", FloatType(), True),  
        StructField("Track URL", StringType(), True), 
        StructField("Available Markets", IntegerType(), True)  
    ])
    
    #Weather data schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),  
        StructField("City", StringType(), True),  
        StructField("Country", StringType(), True),  
        StructField("Temperature (°C)", FloatType(), True),  
        StructField("Feels Like (°C)", FloatType(), True),  
        StructField("Weather Description", StringType(), True), 
        StructField("Humidity (%)", IntegerType(), True), 
        StructField("Wind Speed (m/s)", FloatType(), True)  
    ])
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)", "timestamp")  
                .select(from_json(col('value'), schema).alias("data"), "timestamp")  
                .select("data.*", "timestamp")
                .withWatermark('timestamp', '2 minutes') 
        )

    
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream\
            .format("parquet")\
            .option("checkpointLocation", checkpointFolder)\
            .option("path", output)\
            .outputMode("append")\
            .start())
            
    youtubeDF = read_kafka_topic('youtube_data', youtubeSchema).alias('youtube')
    musicDF = read_kafka_topic('music_data', musicSchema).alias('music')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    
    query1 = streamWriter(youtubeDF, "s3a://youtube-streaming-data/checkpoints/youtube_data",
                 "s3a://youtube-streaming-data/data/youtube_data")
    query2 = streamWriter(musicDF, "s3a://youtube-streaming-data/checkpoints/music_data",
                 "s3a://youtube-streaming-data/data/music_data")
    query3 = streamWriter(weatherDF, "s3a://youtube-streaming-data/checkpoints/weather_data",
                 "s3a://youtube-streaming-data/data/weather_data")
    
    query3.awaitTermination()
    
if __name__ == "__main__":
    main()