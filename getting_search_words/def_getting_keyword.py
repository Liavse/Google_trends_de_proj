from datetime import datetime

from def_get_the_location_code import get_the_location_code
from def_get_the_language_code import get_the_language_code
from def_check_keyword_search_date import check_keyword_search_date
from def_search_for_keywords_api import search_for_keywords
from def_insert_data_to_RDS import insert_data_to_RDS
from common.logger import setup_logger
logger = setup_logger()

def getting_keywords(user):
    """
    This function prompts the user for keywords, languages, and countries to search for. It then checks if each keyword
    has been searched during the current month. If it has, it informs the user. Otherwise, it re-searches the keyword
    and inserts the data into the RDS database.

    Parameters:
    - user (str): The username of the user who is performing the search.

    Returns:
    - None: The function prints messages to the console and calls the `search_for_keywords` function for each keyword
      that needs to be re-searched. It also inserts the data into the RDS database.

    Side Effects:
    - The function may call the `search_for_keywords` function, which may have its own side effects.
    - The function inserts data into the RDS database by calling the `insert_data_to_RDS` function.

    Notes:
    - The function prompts the user for keywords, languages, and countries until the user enters '#' as the keyword.
    - The function uses the `get_the_language_code` and `get_the_location_code` functions to convert the language and
      location names entered by the user into corresponding codes.
    - The function uses the `check_keyword_search_date` function to check if a keyword has been searched during the
      current month.
    - The function calls the `search_for_keywords` function to re-search keywords that have not been searched during
      the current month.
    - The function calls the `insert_data_to_RDS` function to insert the data into the RDS database.
    """
    login_datetime = datetime.now()
    # Initialize an empty dictionary to store the keywords and their corresponding language and location codes.
    words = {}

    # Prompt the user for the first keyword.
    print(f"Hi, {user}, please write the first keyword that you want to seek for: - write '#' to end. ")

    # Loop until the user enters '#'.
    while True:
        # Get the keyword, language, and location from the user.
        keyword = input("Keyword: ").capitalize()
        if keyword == '#':
            break
        location = input("Country: ").capitalize()
        location_code = get_the_location_code(location)
        language_code = get_the_language_code(location_code)

        # Update the dictionary with the keyword and its corresponding language and location codes.
        words[keyword] = {'language_code': language_code, 'location_code': location_code}

    # Get the current month.
    current_month = datetime.now().month

    # Loop through the keywords and their corresponding language and location codes.
    for keyword, codes in words.items():
        # Check if the keyword has been searched before.
        last_searched_month = check_keyword_search_date(keyword, codes['language_code'], codes['location_code'])
        if last_searched_month is not None and last_searched_month.month == current_month:
            print(f"The keyword '{keyword}' has already been searched for this country during this month.")
            logger.info(f"The keyword '{keyword}' has already been searched for this country during this month.")

        else:
            print(f"The keyword '{keyword}' has not been searched for this country this month- starting the process.")
            logger.info(f"The keyword '{keyword}' has not been searched for this country this month- starting the process.")
            search_for_keywords(keyword, codes['language_code'], codes['location_code'], login_datetime)
            insert_data_to_RDS(keyword, codes['language_code'], codes['location_code'], login_datetime, user)

    logger.info(f"User {user} has chosen {keyword} for search in country {location}" )
