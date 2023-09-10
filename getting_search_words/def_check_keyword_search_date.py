import mysql.connector
from common.config_class import Config
from common.logger import setup_logger
config = Config()
logger = setup_logger()
def check_keyword_search_date(word, language, location):
    """
    Check whether a keyword has been searched and return the date it was searched.

    :param word: The keyword to check.
    :param language: The language of the keyword.
    :param location: The location of the keyword.
    :return: The date the keyword was searched, or None if the keyword has not been searched.
    """
    # Connect to the database
    connection = mysql.connector.connect(
        host = config['MYSQL']['host'],
        user = config['MYSQL']['user'],
        password = config['MYSQL']['password'],
        charset='utf8mb4'
    )
    # Create a cursor object
    cursor = connection.cursor()
    # SQL query to get the date the keyword was searched
    query = f"SELECT MAX(insertdatetime) as max_search_date FROM marketing_analysis.keywords WHERE keyword = '{word}' AND language = '{language}' AND location = '{location}';"

    # Execute the query
    cursor.execute(query)
    # Fetch the result
    query_result = cursor.fetchone()
    # Close the cursor and connection
    cursor.close()
    connection.close()
    # Return the date the keyword was searched, or None if the keyword has not been searched
    return query_result[0] if query_result[0] is not None else None
