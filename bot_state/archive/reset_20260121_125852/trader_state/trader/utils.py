import logging, requests
from settings import SLACK_WEBHOOK

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def log(msg):
    logging.info(msg)

def send_slack(text):
    requests.post(SLACK_WEBHOOK, json={"text": text})
