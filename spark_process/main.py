import argparse

from SparkStreaming import main_spark

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File Name for Spark function')
    parser.add_argument('--file_name', type=str, help='File name from lambda s3 trigger')

    args = parser.parse_args()
    main_spark(args.file_name)