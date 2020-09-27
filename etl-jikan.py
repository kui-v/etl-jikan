import argparse
import logging
import requests
import time
from google.cloud import bigquery

logger = logging.getLogger("jikan-logger")

parser = argparse.ArgumentParser(description='Parse params for Jikan REST API')
parser.add_argument('--tts', type=int, help='time to sleep (seconds)', default=1)
parser.add_argument('--start', type=int, help='MAL ID to start', default=1)
parser.add_argument('--end', type=int, help='MAL ID to end', default=100)
args = parser.parse_args()

tts = args.tts
start = args.start
end = args.end
jikan_url_anime = 'http://34.82.195.17:8080/v3/anime/'

logger.info("Starting Jikan with TTS: {0}, Start: {1}, End: {2}".format(tts, start, end))

def get_bulk_anime(time_to_sleep, bulk_start, bulk_end):
    loop_count = 1
    anime_list = []
    for i in range(bulk_start, bulk_end):
        r = requests.get(jikan_url_anime + str(i))
        if r.status_code == 200:
            ani_dict = {}
            anison = r.json()
            ani_dict['aired_from'] = anison['aired']['from']
            ani_dict['aired_to'] = anison['aired']['to']
            ani_dict['episodes'] = anison['episodes']
            ani_dict['favorites'] = anison['favorites']
            ani_dict['genres'] = [[{'genre_id': d['mal_id']}, {'genre_name': d['name']}] for d in anison['genres']]
            ani_dict['image_url'] = anison['image_url']
            ani_dict['mal_id'] = anison['mal_id']
            ani_dict['members'] = anison['members']
            ani_dict['popularity'] = anison['popularity']
            ani_dict['rank'] = anison['rank']
            ani_dict['score'] = anison['score']
            ani_dict['scored_by'] = anison['scored_by']
            ani_dict['studios'] = [[{'studio_id': d['mal_id']}, {'studio_name': d['name']}] for d in anison['studios']]
            ani_dict['source'] = anison['source']
            ani_dict['synopsis'] = anison['synopsis']
            ani_dict['title'] = anison['title']
            ani_dict['title_english'] = anison['title_english']
            ani_dict['title_japanese'] = anison['title_japanese']
            ani_dict['type'] = anison['type']
            ani_dict['url'] = anison['url']
            anime_list.append(ani_dict)
        loop_count = loop_count + 1
        time.sleep(time_to_sleep)
    return anime_list

# def etl_controller(time_to_sleep=tts, start_loop=start, end_loop=end)
    # for i in range(start_loop, end_loop):  
    # counter = 1
    # for i in range(start_loop, end_loop)
    #     if counter % 50 == 0:


ani = get_jikan_anime()
client = bigquery.Client()
