import json
from utils import call_for_required_fields
from aws_lambda_powertools import Logger

logger = Logger()


def json_handler(json_string, prefix):
    required_data = call_for_required_fields(prefix)

    directories_dict = {i["destination-field"]: i["source-field"].split("/") for i in required_data}
    end_result = get_inner_data(json_string, directories_dict)
    logger.info(f"Prepared data: {end_result}")
    return end_result


def get_inner_data(json_string, directories_dict):
    end_result = {}
    dictionary = json.loads(json_string)
    for k, v in directories_dict.items():
        temp_dict = dictionary
        for i in v:
            temp_dict = temp_dict[i]
        end_result[k] = temp_dict
    return end_result
