''''

This code creates spark structure streaming:
reading json file from s3 (build its schema)
create a dataframe for the relevant parts of the json (explodes arrays to rows in the right order)
and then:
1. print the df to the console
2. save the df as a parquet file in another s3 prefix
3. add another column that presents a json for each row and send it to kafka topic

####### Need to bring another df from Mysql and join add country column to the main df.  V
####### Need to create partitions to s3 prefix open another prefix by country.  V

'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, expr, hash, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark import SparkConf
from def_Mysql_conn import create_df_mysql
from Spark_Process.def_copy_delete_s3 import files_names_for_move
import sys
sys.path.append('/tmp/pycharm_project_155/common') #this script will recognize common folder
from config_class import Config
from logger import setup_logger

def main_spark(file_name):
    config = Config()
    logger = setup_logger()

    logger.info("Begin spark in python")

    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", config['AWS']['aws_access_key_id'])
    conf.set("spark.hadoop.fs.s3a.secret.key", config['AWS']['aws_secret_access_key'])

    kafka_spark_topic = config['KAFKA']['kafka_spark_topic']
    brokers = config['KAFKA']['brokers']

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Spark Streaming S3") \
        .config(conf=conf) \
        .getOrCreate()

    # Define the schema for the JSON data
    json_schema = StructType([
        StructField("tasks", ArrayType(
            StructType([
                StructField("data", StructType([
                    StructField("keyword", StringType(), True),
                    StructField("location_code", IntegerType(), True),
                    StructField("language_code", StringType(), True)
                ]), True),
                StructField("result", ArrayType(
                    StructType([
                        StructField("items", ArrayType(
                            StructType([
                                StructField("keyword_data", StructType([
                                    StructField("keyword", StringType(), True),
                                    StructField("language_code", StringType(), True),
                                    StructField("keyword_info", StructType([
                                        StructField("monthly_searches", ArrayType(
                                            StructType([
                                                StructField("year", IntegerType(), True),
                                                StructField("month", IntegerType(), True),
                                                StructField("search_volume", IntegerType(), True)
                                            ])
                                        ), True)
                                    ]))
                                ]), True),
                                StructField("related_keywords", ArrayType(StringType(), True))
                            ])

                        ), True)
                    ])
                ), True)
            ])
        ), True)
    ])

    # Define the path to the directory containing JSON files

    json_directory = config['SPARK']['json_directory']
    pq_s3_directory = config['SPARK']['pq_s3_directory']
    checkpoint_location = config['SPARK']['checkpoint_location']

    stream_df = spark.read.option("multiline", "true").schema(json_schema).json(f'{json_directory}/{file_name}')

    basic_df = stream_df.select((col("tasks.result").getItem(0).items.getItem(0)).alias("all_items_first_level"),
                                col("tasks.data.keyword").getItem(0).alias("main_keyword"),
                                col("tasks.data.location_code").getItem(0).alias("location_code"),
                                col("tasks.data.language_code").getItem(0).alias("language_code")
                                )

    exploded_items_df = basic_df.select(col("main_keyword"), col("location_code"), col("language_code"),
                                        explode(col("all_items_first_level")).alias("sec_level")
                                        )

    exploded_searches = exploded_items_df.select(col("main_keyword"), col("location_code"), col("language_code"),
                                                 col("sec_level.keyword_data.keyword").alias("secondary_keyword"),
                                                 col("sec_level.related_keywords").alias("related_keywords"),
                                                 explode(col("sec_level.keyword_data.keyword_info.monthly_searches")).alias(
                                                     "exploded_monthly_search")
                                                 )

    final_df = exploded_searches.select(col("main_keyword"),
                                        col("location_code"),
                                        col("language_code"),
                                        col("secondary_keyword"),
                                        concat_ws(", ", col("related_keywords")).alias("related_keywords"),
                                        col("exploded_monthly_search.year"),
                                        col("exploded_monthly_search.month"),
                                        col("exploded_monthly_search.search_volume"),
                                        input_file_name().alias("file_name")
                                        )

    # creating Mysql DF (regular python - small and static table)

    mysql_df, schema = create_df_mysql('countries_languages')
    spark_schema = eval(schema)

    # must convert to spark df in order to join it with spark df
    mysql_spark_df = spark.createDataFrame(data=mysql_df, schema=spark_schema)

    # join between two df by location_code and lang_code
    final_join_df = final_df.join(mysql_spark_df, on=['location_code', 'language_code'], how="left")

    # def write_to_console_and_kafka(df, id):
    # #used to write to few sinks (console, kafka, s3)

    final_df = final_join_df.withColumn('CK', concat_ws('~', 'location_code', 'language_code', 'main_keyword',
                                                        'secondary_keyword', 'year', 'month')) \
                            .withColumn('hashed_key', hash('CK')
                                                                )
    # every row in the df as a json format for NOSQL DB
    final_enrich_df = final_df.withColumn("value", expr("to_json(struct(*))"))

    final_df.write \
        .format("console") \
        .save()

    pandas_df_table = final_enrich_df.toPandas().head(10).to_string()
    logger.info("DataFrame contents sample:\n%s", pandas_df_table)

    try:
        final_df.write \
        .format("parquet") \
        .option("header", "true")\
        .option("path", pq_s3_directory) \
        .option("checkpointLocation", checkpoint_location) \
        .partitionBy("location_name")\
        .mode("append")\
        .save()
    except Exception as e:
        logger.error(f'Cannot write to S3 {e}')

    """    
    # This was when spark read from directory and not specific file
    # files_df=final_df.select(col('file_name')).distinct().toPandas()
    # files_name=files_df['file_name'].str.split('/').str[-1]
    """

    logger.info(f"Done writing files {file_name} to S3 to path {pq_s3_directory}{final_enrich_df.toPandas().head(1)['location_name'].values[0]}")

    try:
        logger.info(f"Writing file {file_name} content into KAFKA's topic {kafka_spark_topic}")
        final_enrich_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", kafka_spark_topic)\
        .option("checkpointLocation", checkpoint_location) \
        .save()
    except Exception as e:
        logger.error(f'Cannot write to Kafaka {e}')

    logger.info(f"Begin move file: {file_name} from source to dest location in S3")
    files_names_for_move(file_name,json_directory)
