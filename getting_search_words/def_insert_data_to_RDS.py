import mysql.connector

from common.config_class import Config
from common.logger import setup_logger
config = Config()
logger = setup_logger()

def insert_data_to_RDS(keyword, language, country, datetime, user):
    """
    Inserts data into the 'keywords' table in a MySQL RDS instance.

    Parameters:
    - keyword (str): The keyword to be inserted.
    - language (str): The language associated with the keyword.
    - country (str): The country associated with the keyword.
    - datetime (datetime): The datetime when the keyword was inserted.
    - user (str): The user who inserted the keyword.

    Returns:
    - None: The function prints messages to the console indicating the success or failure of the insertion.

    Raises:
    - mysql.connector.Error: If there's an error related to the MySQL operations.
    - Exception: For any other unexpected errors.

    Notes:
    - The function establishes a connection to the specified RDS instance and database.
    - It uses parameterized queries to prevent SQL injection attacks.
    - After executing the insertion query, it commits the transaction to the database.
    - The function closes the cursor and the connection to the database after the operation.
    """

    try:
        # Establish a connection to the RDS instance and specify the database
        connection = mysql.connector.connect(
            host=config['MYSQL']['host'],
            user=config['MYSQL']['user'],
            password=config['MYSQL']['password'],
            database='marketing_analysis'  # Ensure you specify the correct database name
        )

        # Create a cursor object
        cursor = connection.cursor()

        # SQL query with parameterized inputs
        inserting = ("INSERT INTO marketing_analysis.keywords (Keyword, language, location, username, insertDatetime) "
                     "VALUES (%s, %s, %s, %s, %s);")

        # Data to be inserted
        data = (keyword, language, country, user, datetime)

        # Execute the query
        cursor.execute(inserting, data)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()
        logger.info(f"Data has been inserted into MYSQL in RDS {data}")

        print("Data has been successfully inserted into the database.")
    except mysql.connector.Error as err:
        print(f"An error occurred: {err}")
        logger.error(f"An error occurred while trying to connect to MySQL in RDS: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logger.error(f"An error occurred while trying to insert data into MySQL in RDS: {e}")
