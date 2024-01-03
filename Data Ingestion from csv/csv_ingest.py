import sys
import logging
import traceback
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import dense_rank
from pyspark.sql.types import StructType, StructField, StringType

logger = logging.getLogger(__name__)


class AnimeReport():

    def __init__(self):
        self.start_run_time = None
        self.spark = None
        self.df = None  # Added df as an instance variable

    def start(self, argv):

        self.start_run_time = time.time()
        start_run_time = self.start_run_time
        self.spark = (SparkSession.builder.appName("ETLJob")
                      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
                      .enableHiveSupport()
                      .getOrCreate())

        try:
            self.read_input_file()
            logger.info("Reading done from csv....!!!")

            self.write_output_table()
            logger.info("Writing done....!!!")

        except Exception as e:
            logger.error("Error when executing flow: {}".format(e))
            traceback.print_exc()
            exit(1)

        finally:
            end_run_time = time.time()
            logger.info("Total Time Taken= {}".format(end_run_time - start_run_time))

        logger.info("Successful flow execution")
        exit(0)

    def read_input_file(self):

        try:
            spark = self.spark

            # Define the schema based on your CSV column names and types
            # Assuming all columns are of StringType for simplicity; adjust as needed
            csv_schema = StructType([StructField("Name", StringType(), True),
                                     StructField("Japanese_name", StringType(), True),
                                     StructField("Type", StringType(), True),
                                     StructField("Episodes", StringType(), True),
                                     StructField("Studio", StringType(), True),
                                     StructField("Release_season", StringType(), True),
                                     StructField("Tags", StringType(), True),
                                     StructField("Rating", StringType(), True),
                                     StructField("Release_year", StringType(), True),
                                     StructField("End_year", StringType(), True),
                                     StructField("Description", StringType(), True),
                                     StructField("Related_anime", StringType(), True)
                                     ])
            # csv file path
            csv_file = (r"C:\Users\dibya\Downloads\archive\Anime.csv")

            # Reading the csv file
            self.df = spark.read.csv(csv_file, schema=csv_schema, header=True)

            # show the dataframe
            self.df.show()

        except Exception as error:
            logger.error("Error when executing flow: {}".format(error))
            raise

    def write_output_table(self):
        try:
            spark = self.spark
            anime_report_df = self.df

            # filtering data based on rating and Type as we want shows which only shows on TV
            filter_df = anime_report_df.filter((anime_report_df.Rating > 4.00) & (anime_report_df.Type == 'TV'))

            # filtering the year the show aired
            release_df = filter_df.filter(filter_df.Release_year.isin('2019', '2020', '2021', '2022')) \
                    .orderBy(filter_df.Name, filter_df.Release_year)

            w = Window.partitionBy("Name").orderBy("Release_year")

                # getting the sequels order
            final_df = release_df.withColumn("part", dense_rank().over(w))

                # final_db is tha database
                # anime_report is the final table name after transformation
            final_df.write.mode("overwrite").saveAsTable("final_db.anime_report")

        except Exception as error:
                logger.error("Error when executing flow: {}".format(error))
                raise


def main():
    anime = AnimeReport()
    anime.start(sys.argv)  # Changed from anime.run() to anime.start(sys.argv)


if __name__ == '__main__':
    main()

