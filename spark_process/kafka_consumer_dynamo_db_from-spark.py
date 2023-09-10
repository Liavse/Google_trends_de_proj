from datetime import datetime

from kafka import KafkaConsumer
import boto3
import json
from common.config_class import Config
from common.logger import setup_logger
logger = setup_logger()
config=Config()

kafka_spark_topic = config['KAFKA']['kafka_spark_topic']
brokers = config['KAFKA']['brokers']
consumer = KafkaConsumer(kafka_spark_topic, bootstrap_servers=brokers)

# DynamoDB client with explicit credentials
dynamodb = boto3.client(
    'dynamodb',
    aws_access_key_id=config['AWS']['aws_access_key_id'],
    aws_secret_access_key=config['AWS']['aws_secret_access_key'],
    region_name=config['AWS']['region_name']
)
table_name = 'marketing_analysis'

try:
    for message in consumer:
        print(str(message))
        json_data = json.loads(message.value.decode('utf-8'))
        item_data = {
            'location_code': {'N': str(json_data['location_code'])},
            'language_code': {'S': json_data['language_code']},
            'main_keyword': {'S': json_data['main_keyword']},
            'secondary_keyword': {'S': json_data['secondary_keyword']},
            'related_keywords': {'S': json_data['related_keywords']},
            'year' : {'N': str(json_data['year'])},
            'month': {'N': str(json_data['month'])},
            'search_volume': {'N': str(json_data['search_volume'])},
            'location_name': {'S': json_data['location_name']},
            'language_name': {'S': json_data['language_name']},
            'CK': {'S': json_data['CK']},
            'hashed_key': {'N': str(json_data['hashed_key'])},
            'insert_time_stamp': {'S': datetime.fromtimestamp(message.timestamp/1000).isoformat()}
        }
        # Write data to DynamoDB
        dynamodb.put_item(TableName=table_name, Item=item_data)

except Exception as e:
    logger.error(f"Kafka error {e}")
    print(e)

