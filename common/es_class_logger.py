import logging
import requests
from elasticsearch import Elasticsearch
from requests_aws4auth import AWS4Auth
from datetime import datetime

class ElasticsearchHandler(logging.Handler):
    def __init__(self, access_key, secret_key, region,token, es_endpoint, index_name):
        super().__init__()
        self.awsauth = AWS4Auth(access_key, secret_key, region, 'es', session_token=token)
        self.es_endpoint = es_endpoint
        self.index_name = index_name

    def emit(self, record):
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'message': self.format(record)
        }
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(f'{self.es_endpoint}/{self.index_name}/_doc/', auth=self.awsauth, json=log_entry, headers=headers)
            print(print(response.text))
        except Exception as e:
            print(f"Failed to send log to OpenSearch: {e}")
