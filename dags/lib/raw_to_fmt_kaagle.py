import os
import pyarrow.parquet as pq
import fastparquet
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_kaagle(file_name, current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/kaagle/MoviePercent/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/kaagle/MoviePercent/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   df = pd.read_csv(RATING_PATH, sep='\t')
   parquet_file_name = file_name.replace(".csv", ".snappy.parquet")
   df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
