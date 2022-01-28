import hashlib
from botocore.exceptions import ClientError, WaiterError
from botocore.exceptions import BotoCoreError # base exception class for all Boto3 error types


def calculate_s3_etag(file_path, chunk_size=5 * 1024 ** 2):
    '''
        chunk_size: type: int, default: 5MB
    '''
    
    md5s = []

    with open(file_path, 'rb') as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))

    if len(md5s) < 1:
        return '{}'.format(hashlib.md5().hexdigest())

    if len(md5s) == 1:
        return '{}'.format(md5s[0].hexdigest())

    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))


def verify_multipart_uploaded_fl(s3_client, s3_resource, expected_eTag, bucket_name, key):
    '''
    Checks if a multipart upload file has been corrupted during Network Traversal.
    
    Refer:
         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Parameters:
    ---------------------
    s3_client: S3 Client Object
    expected_eTag: expected_ETag that must be compared to 

    Returns:
    --------------------
    True On success
    Raises Error otherwise
    '''
    
    SUCCESS_FLAG = False
    
    try:
        # wait for object to exist; polls for 20 times every 5 seconds
        # then throws exception 
        s3_resource.Object(bucket_name, key).wait_until_exists()
        
        res = s3_client.head_object(Bucket=bucket_name, Key=key)
        # example ETag returned by S3 client ==> 'ETag': '"ea7528d0be3572e7cfeefe1cb4a77ac8-3"'
        
        # compare the eTags
        if res['ETag'].strip('"').strip("'").strip("'''") == expected_eTag.strip('"').strip("'").strip("'''"):
            SUCCESS_FLAG = True
        else:
            raise ClientError
        return SUCCESS_FLAG
    
    except WaiterError as e:
        print(f"Produced WaiterError waiting for: S3://{bucket_name}/{key}")
        # raise WaiterError("Object could not be located in bucket") from e
        raise e
        
    except ClientError as e:
        print(f"Produced ClientError comparing ETag for: S3://{bucket_name}/{key}")
        raise e

    except BotoCoreError as e:
        print(f"Produced BotoCoreError verifying: S3://{bucket_name}/{key}")
        raise e
    
    except Exception as e:
        print(f"Produced Unexpected Exception handling: S3://{bucket_name}/{key}")
        raise e
