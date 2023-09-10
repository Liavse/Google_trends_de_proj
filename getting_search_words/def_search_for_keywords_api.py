import json
import requests
from def_upload_to_s3 import upload_to_s3
from common.config_class import Config
from common.logger import setup_logger

config = Config()
logger = setup_logger()

def search_for_keywords(keyword, lng_code, loc_code, login_datetime):
    """
    Search for related keywords using the DataForSEO API.

    :param keyword: The keyword to search for.
    :param lng_code: The language code to use in the API request.
    :param loc_code: The location code to use in the API request.
    :param login_datetime: The login datetime to use in the file name.
    :return: JSON files which have been uploaded to the S3 bucket.
    """

    if len(keyword.split()) > 1:
        keyword = '[', keyword, ']'
    url = config['API']['url']
    payload = [{
        "keyword": keyword,
        "location_code": int(loc_code),
        "language_code": lng_code,
        "depth": 3,
        "include_seed_keyword": False,
        "include_serp_info": False,
        "limit": 100,
        "offset": 0
    }]
    headers = {
        'Authorization': config['API']['authorization'],
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
    logger.info (f"The API params are :  {json.dumps(payload)} ")
    # Replace spaces in the keyword with underscores for the JSON file's name
    keyword_filename = keyword.replace(" ", "_")
    keyword_date = login_datetime.date()
    file_path = rf'C:\Users\Eternity\PycharmProjects\Final_Project\{keyword_filename}_{keyword_date}.json'
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(response.json(), file, indent=4, ensure_ascii=False)
    upload_to_s3(file_path, 'marketing-analysis-project', 'Json-files-api/', f's3_{keyword_filename}_{loc_code}.json')
