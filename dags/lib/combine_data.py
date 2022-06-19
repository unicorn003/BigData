import os
from pyspark.sql import SQLContext
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def combine_data_imdb(current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieTop10/" + current_day + "/"
   NAME_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieName/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS_NAME = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatisticsName/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS_NAME):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS_NAME)
   from pyspark import SparkContext
   sc = SparkContext(appName="CombineData")
   sqlContext = SQLContext(sc)
   sqlContext2 = SQLContext(sc)
   df_ratings = sqlContext.read.parquet(RATING_PATH)
   df_ratings.registerTempTable("ratings")
   df_names = sqlContext2.read.parquet(NAME_PATH)
   df_names.registerTempTable("names")

   stats_df = sqlContext.sql("SELECT AVG(averageRating) AS avg_rating,"
                             "       MAX(averageRating) AS max_rating,"
                             "       MIN(averageRating) AS min_rating,"
                             "       COUNT(averageRating) AS count_rating"
                             "    FROM ratings LIMIT 10")
   top10_df = sqlContext.sql("SELECT tconst, averageRating"
                             "    FROM ratings"
                             "    WHERE numVotes > 50000 "
                             "    ORDER BY averageRating DESC"
                             "    LIMIT 10")
   names_df = sqlContext2.sql("SELECT COUNT(*) AS count_names_with_A"
                              "  FROM names"
                              "  WHERE primaryName LIKE 'A%' ")

   stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")

   stats_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet", mode="overwrite")

   names_df.write.save(USAGE_OUTPUT_FOLDER_STATS_NAME + "res.snappy.parquet", mode="overwrite")