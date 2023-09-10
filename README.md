# Google_Trends_DE 
## Project Architecture 
In this project we use AWS echosystem with python following this architecure 
![](https://github.com/Liavse/Google_trends_de_proj/blob/main/GoogleTrends_Aws_Architecture.png)

## API - getting search words
The first part is extract Json data from the API.
The api gets paramters such as: wanted keyword, language and country to search for and gives back
wide range of data such as different similar keywords (related_keywords), amont of searches per year per month for each keyword, search_engine type, etc...
The function asks the user for keyword, language and country. If the user already searched the same keyword, language and country in the same month, he will get a message that he had already searched it the there is a data for him. 
If the user search something else, the api will issue a new json file, and the record with the search parameters will bw stored in Mysql on RDS.
The same (raw) json file will be uploaded to S3.

## Spark Processing
When there is a new file in S3, lambda function will be triggered the run a python script on EC2.
The scripts get arguments with the file name from s3.
f'sudo -u ubuntu /usr/bin/python3 /tmp/pycharm_project_155/main.py --file_name {key}'
**When the lambda invokes you can see the logs in CloudWatch under Logs Group in "s3_spark_trigger".
The script operation you can see on AWS Systems Manager -> Documents -> AWS-RunShellScript -> Run commands.**
The python script runs Spark DataFrame that reads the specific file from S3 prefix.
Spark writes to S3 prefix, partitioned by location_name (country) and also to Kafka topic (kafka-df_spark_ss).

## Kafka 
Spark DF writes to Kafka to 'kafka-df_spark_ss' topic. 

The kafka consumer writes to DynamoDB in Key-Value format, including hash key composites of location_code', 'language_code', 'main_keyword',
'secondary_keyword', 'year', 'month' fields from the Dataframe.

## Data Analysis
All spark DF transformed files are stored in S3 and there is Athena above S3.
External Table partitioned by location_name for free hand sql analysis.

## Logs
For every step there is a log written to Aws OpenSearch with Kibana dashboard into logs_py index.
The logs written by logger class created by us, that stores the log into OpenSearch (Aws elasticsearch)
Eeach log contain timestamp, level (Info/Warning/Error) and the message with the formatter:
Formatter(' %(name)s- [%(levelname)s] - %(filename)s - %(message)s - %(asctime)s ').
Here is a taste of the kibana logs dashboard:

![](https://github.com/Liavse/Google_trends_de_proj/blob/main/logs_py_kibana.png)






