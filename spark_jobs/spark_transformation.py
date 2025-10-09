import os
import load_dotenv

load_dotenv()

aiven_user = os.getenv("aiven_user")
aivern_url = os.getenv("aivern_url")
aiven_password = os.getenv("aiven_password")


def transform_data():
    # importing libraries

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Youtube_data_cleaning").config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()

    channel_df = spark.read.csv("/opt/airflow/channel_data.csv", header=True, inferSchema=True)
    video_df = spark.read.csv("/opt/airflow/new_video_data.csv",  header=True, inferSchema=True)

    # reporting the data
    channel_df.show()
    video_df.show()

    channel_df.printSchema()
    video_df.printSchema()

    # fillinf null values 
    # use the previous values for the numeric data indicating no significant change
    from pyspark.sql import Window
    import pyspark.sql.functions as F

    channel_window = Window.partitionBy("channel_id").orderBy("extraction_time")

    # the function coalesce picks the first non zero value in the columns provided in its arguments and returns it to the with column funtion 
    # lag in combination with the window function essentially creates a brand new column of view count just shited forward by one value 
    # together they replace nulls with the previous values if a null is found 

    channel_df = (
        channel_df 
        .withColumn('view_count', F.coalesce("view_count", F.lag("view_count").over(channel_window)))
        .withColumn("subscriber_count", F.coalesce("subscriber_count", F.lag("subscriber_count").over(channel_window)))   
        .withColumn('video_count', F.coalesce("video_count", F.lag("video_count").over(channel_window))) 
        .withColumn('extraction_time', F.coalesce("extraction_time", F.lag("extraction_time").over(channel_window)))
        .withColumn("channel_id", F.coalesce("channel_id", F.lag("channel_id").over(channel_window)))   
        .withColumn('published_at', F.coalesce("published_at", F.lag("published_at").over(channel_window)))
        .withColumn('channel_name', F.coalesce("channel_name", F.lag("channel_name").over(channel_window)))
        .withColumn("description", F.coalesce("description", F.lag("description").over(channel_window)))   
        


        )

    channel_df = channel_df.fillna({
        # integer values are replaced with in case not caught above 
        "view_count": 0,
        "subscriber_count": 0,
        "video_count": 0,
        # replace string values with unknown
        "channel_name": "unknown",
        "description": "unknown",

    })

    video_window = Window.partitionBy("video_id").orderBy("extraction_time")

    video_df = (
        video_df 
        .withColumn('view_count', F.coalesce("view_count", F.lag("view_count").over(video_window)))
        .withColumn("like_count", F.coalesce("like_count", F.lag("like_count").over(video_window)))   
        .withColumn('comment_count', F.coalesce("comment_count", F.lag("comment_count").over(video_window))) 
        .withColumn('extraction_time', F.coalesce("extraction_time", F.lag("extraction_time").over(video_window)))
        .withColumn("video_id", F.coalesce("video_id", F.lag("video_id").over(video_window)))   
        .withColumn('title', F.coalesce("title", F.lag("title").over(video_window)))
        .withColumn('publish_date', F.coalesce("publish_date", F.lag("publish_date").over(video_window)))
        # adding the delta columns here to avoid redundancy
        .withColumn("view_change", F.col("view_count") - F.lag("view_count", 1, 0).over(video_window))
        .withColumn("like_change", F.col("like_count") - F.lag("like_count", 1, 0).over(video_window))
        .withColumn("comment_change", F.col("comment_count") - F.lag("comment_count", 1, 0).over(video_window))
        )

    video_df = video_df.fillna({
        # integer values are replaced with in case not caught above 
        "view_count": 0,
        "like_count": 0,
        "comment_count": 0,
        # replace string values with unknown
        "title": "unknown",
    })

    channel_df.show()
    video_df.show()

    # creatign the derived metrics
    from pyspark.sql.functions import col


    video_df = (
        video_df
        #rates
        .withColumn("like_rate", col("like_count") / col("view_count")) 
        .withColumn("comment_rate", col("comment_count") / col("view_count")) 
    .withColumn("engagement_rate", (col("like_count") + col("comment_count")) / col("view_count"))
    
    )
    video_df.show()

    # posting to postgres
    db_url = aivern_url
    db_properties = {
        "user": aiven_user,
        "password": aiven_password,
        "driver": "org.postgresql.Driver"
    }

    video_df.write.jdbc(url=db_url, table="video_stats", mode="append", properties=db_properties)


transform_data()