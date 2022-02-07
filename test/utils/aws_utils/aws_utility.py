from unittest import TestCase as tc
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, WaiterError
import pytest

from src.utils.aws_utils.aws_utility import verify_multipart_uploaded_fl 


def get_s3(region_name="us-east-1"):
    config = Config(
        retries = {
            'max_attempts' : 3 } # default = 20
    )

    session = boto3.Session(region_name=region_name)
    s3_client = session.client('s3', config=config)
    s3_resource = session.resource('s3')

    return s3_client, s3_resource

def test_verify_multipart_file_ok():
    
    expected_eTag = 'ea7528d0be3572e7cfeefe1cb4a77ac8-3'
    # key = 'checksum_text.txt'
    bucket_name = "bckt-entertainment-master"
    key = "multi_part_file.txt"
    s3_client, s3_resource = get_s3()

    tc().assertTrue(verify_multipart_uploaded_fl(s3_client, s3_resource, expected_eTag, bucket_name, key))

def test_verify_multipart_file_fail_wrong_key():
    '''
    Effect would be same for both wrong key or bucket name or the combination of these two.
    '''
    expected_eTag = 'ea7528d0be3572e7cfeefe1cb4a77ac8-3"'
    bucket_name = "bckt-entertainment-master"
    key = "wrong_key_cantexist_765443099877665544_not_AT_aLl.txt"
    s3_client, s3_resource = get_s3()
    
    # does not work
    # dummy_object = tc()
    # tc.assertRaises(dummy_object, WaiterError
    #         , verify_multipart_uploaded_fl(s3_client, s3_resource, expected_eTag, bucket_name, key))
    
    # does not work
    # tc().assertRaises( WaiterError
    #         , verify_multipart_uploaded_fl(s3_client, s3_resource, expected_eTag, bucket_name, key))
    
    # works
    # # tc().assertRaises( WaiterError
    #         , verify_multipart_uploaded_fl,s3_client, s3_resource, expected_eTag, bucket_name, key)
    
    with pytest.raises(WaiterError):
        verify_multipart_uploaded_fl(s3_client, s3_resource, expected_eTag, bucket_name, key)
