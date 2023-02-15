import json
from domain.utils import call_for_required_fields
from aws_lambda_powertools import Logger

logger = Logger()


def json_handler(json_string: str, prefix: str) -> dict:
    """
    Extracts specific fields from a JSON string based on a given prefix.

    :param:
        json_string (str): A JSON-formatted string containing the data to be processed.
        prefix (str): A string indicating the prefix for the desired fields.

    :return:
        A dictionary containing the extracted data with keys corresponding to the destination
        fields specified in the `required_data` parameter of `call_for_required_fields()`.
    """
    required_data = call_for_required_fields(prefix)
    directories_dict = {i["destination-field"]: i["source-field"].split(".") for i in required_data}
    end_result = get_inner_data(json_string, directories_dict)
    logger.info(f"Prepared data: {end_result}")
    return end_result


def get_inner_data(json_string, directories_dict):
    """
    This function extracts values from a JSON string based on a provided dictionary.

    :param:
        json_string (str): The JSON string to extract values from.
        directories_dict (dict): A dictionary where each key represents a desired output key, and each value is a list
        of keys or indices to traverse in the JSON string to reach the desired value.

    :return:
        dict: A dictionary containing the extracted values, where each key is the key from directories_dict and each
        value is the extracted value from the JSON string.

    :raises:
        KeyError: If a key in the directories_dict value list does not exist in the JSON string.
        TypeError: If an item in the directories_dict value list is not a key in the JSON string.
    """
    end_result = {}
    dictionary = json.loads(json_string)
    for k, v in directories_dict.items():
        temp_dict = dictionary
        for i in v:
            temp_dict = temp_dict[i]
        end_result[k] = temp_dict
    return end_result
