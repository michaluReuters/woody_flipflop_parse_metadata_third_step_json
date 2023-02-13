import json
import os
import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

logger = Logger()
appconfig = boto3.client('appconfig', region_name='eu-west-2')
s3_bucket = boto3.resource("s3")
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")


def file_in_s3_bucket(file_name_sns, prefix) -> bool:
    """
    Checks if specified file exists in s3 bucket

    :param:
        file_name_sns: file that needs to be checked

    :return:
        bool: status
    """

    try:
        logger.info(f"Looking for a file in bucket: {file_name_sns} and prefix: {prefix}")
        s3_bucket.Object(s3_bucket_name, f"{prefix}/{file_name_sns}.json").load()
        logger.info(f"File found in bucket!")
    except ClientError:
        logger.error(f"File not found in bucket:{prefix}/{file_name_sns}")
        return False
    return True


def call_for_required_fields(prefix):
    configuration_prefixes = appconfig.get_hosted_configuration_version(
        ApplicationId=os.environ.get('APP_CONFIG_APP_ID'),
        ConfigurationProfileId=os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_ID'),
        VersionNumber=int(os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_VERSION'))
    )['Content'].read().decode('utf-8')

    data = json.loads(configuration_prefixes)
    required_fields = {config["source-field"]: config["destination-field"] for config in data}
    logger.info(f"Required fields gathered: {required_fields}")
    return required_fields
