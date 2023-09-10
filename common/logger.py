import logging
import boto3
from es_class_logger import ElasticsearchHandler
from requests_aws4auth import AWS4Auth

session = boto3.Session()
credentials = session.get_credentials()

ENDPOINT= "https://search-logs-elastic-tqjeumkt2tcmbg2v6wpcdigesm.us-east-1.es.amazonaws.com"
INDEX_NAME = 'logs_py'
REGION = 'us-east-1'  # Change to your region
TYPE = 'log'
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, REGION, 'es', session_token=credentials.token)
def setup_logger():
    logger=logging.getLogger(__name__)
    es_handler = ElasticsearchHandler(credentials.access_key, credentials.secret_key, REGION, credentials.token, ENDPOINT, INDEX_NAME)
    formatter = logging.Formatter(' %(name)s- [%(levelname)s] - %(filename)s - %(message)s - %(asctime)s ')
    es_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[es_handler])
    return logger
