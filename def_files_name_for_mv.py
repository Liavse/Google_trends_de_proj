
'''
This function locate all of the Files name in Spark DF and move it to another location
when i use lambda with s3 trigger (the main stream) i process only one by one files
and this function is actually redundant (not harmful)
this function is relevat when spark reads from directory and not specific file
'''
from def_copy_delete_s3 import copy_delete_s3
from common.logger import setup_logger

logger = setup_logger()

def files_names_for_move(files_df,json_directory):
    files_df['file_name'] = files_df['file_name'].str.split('/').str[-1]
    files_name = files_df['file_name'].to_list()
    src_bucket = json_directory.split('/')[2]
    dest_bucket = src_bucket
    dest_prefix = 'processed_files'
    src_prefix = json_directory.split('/')[3]
    for idx, file in enumerate(files_name):
        src_key = f"{src_prefix}/{files_name[idx]}"
        dest_key = f"{dest_prefix}/{files_name[idx]}"
        copy_delete_s3(src_bucket, src_key, dest_bucket, dest_key)

        logger.info(f"File {files_name[idx]} has been moved to {dest_bucket}\{dest_prefix}")
