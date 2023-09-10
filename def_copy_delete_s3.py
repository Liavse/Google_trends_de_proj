import boto3
import botocore

from common.logger import setup_logger

logger = setup_logger()

def copy_delete_s3(src_bucket, src_key, dest_bucket, dest_key):
    s3 = boto3.client("s3")
    try:
        s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={'Bucket': src_bucket, 'Key': src_key})
        s3.delete_object(Bucket=src_bucket, Key=src_key)
    except botocore.exceptions.NoCredentialsError:
        logger.error("No credentials available.")
    except botocore.exceptions.PartialCredentialsError:
        logger.error("Incomplete credentials provided.")
    except botocore.exceptions.ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logger.error("Source object does not exist.")
        elif error_code == 403:
            logger.error("Access denied.")
        else:
            logger.error(f"An error occurred: {e}")
