import json
import os
import boto3
from aws_lambda_powertools import Logger

logger = Logger()
appconfig = boto3.client('appconfig')


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
