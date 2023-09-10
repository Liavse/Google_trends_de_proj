import mysql.connector

def get_the_location_code(location):
    """
    This Function gets a location name and returns its code for the API search
    :param location: location name - string
    :return: location code - int
    """
    connection = mysql.connector.connect(
        host='marketing-analysis-db.cqhvkcawiuxn.us-east-1.rds.amazonaws.com',
        user='admin',
        password='liavnatan'
    )
    # Create a cursor object
    cursor = connection.cursor()

    location = location.capitalize()
    # SQL query
    loc_query = f"SELECT DISTINCT location_code FROM marketing_analysis.countries_languages WHERE location_name = '{location}';"

    # Execute the query
    cursor.execute(loc_query)

    # Fetch the result
    loc_result = cursor.fetchone()
    # Close the cursor and connection
    cursor.close()
    connection.close()
    if loc_result is not None:
        location_code = loc_result[0]
        return int(location_code)
    else:
        return None