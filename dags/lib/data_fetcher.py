import os
import requests
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import date

api_my = KaggleApi()
api_my.authenticate()
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_kaagle(url, data_entity_name, file_name):
   api_my.dataset_download_file(url, file_name=file_name)
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/kaagle/" + "/" + data_entity_name + "/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)


   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + file_name, 'wb').write(file_name)