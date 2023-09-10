import mysql.connector
from common.config_class import Config
from common.logger import setup_logger
config = Config()
logger = setup_logger()
def get_the_language_code(location_code):
    """
    This Function gets a language name and returns its code for the API search
    :param language: language name - string
    :return: language code - string
    """
    connection = mysql.connector.connect(
        host=config['MYSQL']['host'],
        user=config['MYSQL']['user'],
        password=config['MYSQL']['password']
    )
    # Create a cursor object
    cursor = connection.cursor()
    # SQL query
    lang_query = f"SELECT language_code FROM marketing_analysis.countries_languages where location_code  = '{location_code}' GROUP  by language_code ORDER BY keywords DESC LIMIT 1;"

    # Execute the query
    cursor.execute(lang_query)
    # Fetch the result
    lang_result = cursor.fetchone()
    # Close the cursor and connection
    cursor.close()
    connection.close()
    try:
        language_code = lang_result[0]
        if language_code is not None:
            return language_code
        else:
            print("language error")
    except Exception as e:
        logger (f"Cannot get the language_code because of {e}")