import pytest
from src.utils.aws_utils.copy_to_landing import S3_landing

# config = Config(
#         retries = {
#             'max_attempts' : 3 } # default = 20
#     )

# session = boto3.Session(region_name=region_name)
# s3_client = session.client('s3', config=config)
# s3_resource = session.resource('s3')

FILE_NAME = r"D:\repositories\Entertainment-Industry-Retailer-DataWarehouse\test_data\sample_14mb_file.txt"
BUCKET_NAME = "bckt-entertainment-master"
KEY = "multi_part_file.txt"

s3_lan = S3_landing()

def test_upload_file_to_bucket_multipart_ok():
    
    assert (s3_lan.upload_file_to_bucket_multipart(bucket_name=BUCKET_NAME, file_name=FILE_NAME, key=KEY
        , multipart_threshold=5, multipart_chunksize=10) in [0,1])
    
