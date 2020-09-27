import argparse
import json
import logging
import os
import requests
import sys
import time
from google.cloud import bigquery

logger = logging.getLogger("jikan-logger")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - ' + os.path.basename(__file__) + ' - %(message)s ')
ch.setFormatter(formatter)
logger.addHandler(ch)

parser = argparse.ArgumentParser(description='Parse params for Jikan REST API')
parser.add_argument('--tts', type=int, help='time to sleep (seconds)', default=1)
parser.add_argument('--start', type=int, help='MAL ID to start', default=1)
parser.add_argument('--end', type=int, help='MAL ID to end', default=100)
parser.add_argument('--inc', type=int, help='Amount to increment', default=50)
args = parser.parse_args()

tts = args.tts
start = args.start
end = args.end
increment = args.inc
jikan_url_anime = 'http://34.82.195.17:8080/v3/anime/'

logger.info("Starting Jikan with TTS: {0}, Start: {1}, End: {2}".format(tts, start, end))

client = bigquery.Client()
dataset_id = 'jikan'
table_id = 'anime'
table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)
schema_def = []
with open('jikan-bq-schema.json') as json_file:
    schema_def = json.load(json_file)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.schema = schema_def

anime_counter = 1

def date_format(string_date):
    return_string = None
    if string_date is not None:
        return_string = string_date[0:10]
    return return_string

def get_bulk_anime(time_to_sleep, bulk_start, bulk_end):
    global anime_counter
    fname = 'data_{}.txt'.format(bulk_start)
    with open(fname, 'w') as outfile:
        for i in range(bulk_start, bulk_end):
            r = requests.get(jikan_url_anime + str(i))
            if r.status_code == 200:
                ani_dict = {}
                anison = r.json()
                ani_dict['aired_from'] = date_format(anison['aired']['from'])
                ani_dict['aired_to'] = date_format(anison['aired']['to'])
                ani_dict['episodes'] = anison['episodes']
                ani_dict['favorites'] = anison['favorites']
                ani_dict['genres'] = [{'genre_id': d['mal_id'], 'genre_name': d['name']} for d in anison['genres']]
                ani_dict['image_url'] = anison['image_url']
                ani_dict['mal_id'] = anison['mal_id']
                ani_dict['members'] = anison['members']
                ani_dict['popularity'] = anison['popularity']
                ani_dict['rank'] = anison['rank']
                ani_dict['score'] = anison['score']
                ani_dict['scored_by'] = anison['scored_by']
                ani_dict['studios'] = [{'studio_id': d['mal_id'], 'studio_name': d['name']} for d in anison['studios']]
                ani_dict['source'] = anison['source']
                ani_dict['synopsis'] = anison['synopsis']
                ani_dict['title'] = anison['title']
                ani_dict['title_english'] = anison['title_english']
                ani_dict['title_japanese'] = anison['title_japanese']
                ani_dict['type'] = anison['type']
                ani_dict['url'] = anison['url']
                outfile.write(json.dumps(ani_dict))
                outfile.write('\n')
                anime_counter = anime_counter + 1
                logger.info(str(ani_dict['title']))
            time.sleep(time_to_sleep)
    return fname

def etl_controller(time_to_sleep, start_loop, end_loop, bulk_increment):
    for i in range(start_loop, end_loop, bulk_increment):
        ani_file = get_bulk_anime(time_to_sleep, i, i+bulk_increment)
        with open(ani_file, "rb") as source_file:
            load_job = client.load_table_from_file(source_file, table, job_config=job_config)
        os.remove(ani_file)
        logger.info("Staring job {}".format(load_job.job_id))
        load_job.result()
        logger.info("Finished.")

logger.info("Starting the ETL for Jikan.")
etl_controller(tts, start, end, increment)
logger.info("Finished the ETL for Jikan with {} anime titles.".format(anime_counter))