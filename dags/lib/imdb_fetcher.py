import os
import requests

from datetime import date
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_imdb(url, data_entity_name, file_name):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/" + "/" + data_entity_name + "/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)


   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + file_name, 'wb').write(r.content)