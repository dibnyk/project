from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
import s3fs

def transform_and_save(input_path, output_path):
    # Initialize a Spark session
    spark = SparkSession.builder.appName("YouTubeETL").getOrCreate()

    # Read CSV into DataFrame using s3fs
    s3 = s3fs.S3FileSystem(anon=False)
    with s3.open(input_path, "rb") as file:
        df_inbound = spark.read.csv(file, header=True, inferSchema=True)

    # Perform transformations (example: adding a new column)

    df_date = df_inbound.select(df_inbound.title,
                                df_inbound.viewers,
                                df_inbound.subscribers,
                                to_date(df_inbound.publishedAt,"yyyy-mm-dd")
                                .alias('published_date'))

    w = Window.partitionBy('title').orderBy('published_date')
    df_row = df_date.withColumn('Order_of_post', dense_rank().over(w))

    #getting the increase in subscribers for each video
    wspec = Window.orderBy('published_date')
    df_transformed = df_row.withColumn('subs_increase',df_date.subsribers - lag(df_date.subscribers).over(wspec))


    # Write transformed DataFrame to a new CSV file using s3fs
    with s3.open(output_path, "wb") as file:
        df_transformed.write.csv(file, header=True, mode="overwrite")

if __name__ == "__main__":
    input_path = "s3://your-input-bucket/inbound/youtube/youtube_load.csv"
    output_path = "s3://your-output-bucket/outbound/youtube_transformed/youtube_transformed_data"

    transform_and_save(input_path, output_path)
