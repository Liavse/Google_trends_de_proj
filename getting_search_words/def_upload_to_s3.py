import boto3
from botocore.exceptions import NoCredentialsError
from common.config_class import Config
from common.logger import setup_logger

config = Config()
logger = setup_logger()

def upload_to_s3(local_file, bucket, prefix , s3_file):
    """
    Uploads a file to AWS S3.
    :param local_file: Local path to file
    :param bucket: S3 bucket name
    :param s3_file: Destination file name in S3
    :return: True if file was uploaded, else False
    """
    # Initialize S3 client with credentials
    session = boto3.Session(
        aws_access_key_id = config['AWS']['aws_access_key_id'],
        aws_secret_access_key = config['AWS']['aws_secret_access_key']
    )

    s3 = session.client('s3')
    try:
        s3.upload_file(local_file, bucket, f'{prefix}{s3_file}')
        print(f'Successfully uploaded {local_file} to {bucket}{prefix}/s3/{s3_file}')
        logger.info(f'Successfully uploaded {local_file} to {bucket}{prefix}/s3/{s3_file}')
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
        logger.error(f"The file {local_file} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        logger.error("S3 Credentials not available")
        return False