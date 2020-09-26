import argparse
import logging
import requests
import time
from google.cloud import bigquery

logger = logging.getLogger("jikan-logger")

parser = argparse.ArgumentParser(description='Parse params for Jikan REST API')
parser.add_argument('--tts', type=int, help='time to sleep (seconds)', default=1)
parser.add_argument('--start', type=int, help='MAL ID to start', default=1)
parser.add_argument('--end', type=int, help='MAL ID to end', default=100000)
args = parser.parse_args()

tts = args.tts
start = args.start
end = args.end
jikan_url_anime = 'http://34.82.195.17:8080/v3/anime/'

logger.info("Starting Jikan with TTS: {0}, Start: {1}, End: {2}".format(tts, start, end))

