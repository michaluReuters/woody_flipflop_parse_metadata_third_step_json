import json
import os
import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

logger = Logger()
appconfig = boto3.client('appconfigdata')
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
    configuration_prefixes = get_latest_configuration(prefix)

    data = json.loads(configuration_prefixes)
    logger.info(f"Required fields gathered: {data}")
    return data


def get_latest_configuration(prefix):
    """
    This function gathers latest configuration in AWS App config

    :return:
        decoded configuration
    """

    token = appconfig.start_configuration_session(
        ApplicationIdentifier=os.environ.get('APP_CONFIG_APP_ID'),
        EnvironmentIdentifier=os.environ.get('APP_ENVIRONMENT'),
        ConfigurationProfileIdentifier=os.environ.get(f'APP_CONFIG_{prefix.replace("-", "_").upper()}_ID'),
        RequiredMinimumPollIntervalInSeconds=20
    )['InitialConfigurationToken']

    response = appconfig.get_latest_configuration(
        ConfigurationToken=token
    )
    return response['Configuration'].read().decode('utf-8')
