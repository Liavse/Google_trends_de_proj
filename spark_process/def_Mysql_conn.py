import mysql.connector
import pandas as pd
import re

from common.config_class import Config

# config = configparser.ConfigParser()
# config.read('config.ini')
config = Config()
cnx = mysql.connector.connect(
    user=config['MYSQL']['user'],
    password=config['MYSQL']['password'],
    host=config['MYSQL']['host'],
    database='marketing_analysis',
    port=config['MYSQL']['port']
)
def create_df_mysql(table_name):

    cursor=cnx.cursor()
    cursor.execute(f"select * from {table_name}")
    countries_languages = cursor.fetchall()
    cursor.execute(f"describe {table_name}")
    metadata = cursor.fetchall()
    columns = [col[0] for col in metadata]
    types=[re.sub(r"varchar\(\d+\)","str",type[1].decode("utf-8")) for type in metadata]

    data_type_mapping = {
        'int': int,
        'str': str
    }

    schema= {key: data_type_mapping[value] for key, value in zip(columns, types)}

    df_countries_languages=pd.DataFrame(data=countries_languages,columns=columns).dropna().astype(schema)

    cursor.close()

    #Building the schema for spark in order to convert pandas df to spark df
    Struct=''
    for col in metadata:
        Struct=Struct+(f'StructField("{col[0]}", {col[1]}),')
    Struct=re.sub(r"b'varchar\(\d+\)'", "StringType()",Struct[:-1]).replace("b'int'",'IntegerType()')
    Struct=f'StructType([{Struct}])'
    return df_countries_languages,Struct
