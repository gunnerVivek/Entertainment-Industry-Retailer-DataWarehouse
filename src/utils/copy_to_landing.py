import os
# from typing import Any
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from botocore.exceptions import BotoCoreError # base exception class for all Boto3 error types
from dataclasses import dataclass

from src.utils import date_utility
from src.utils.common_utils import checksum_utility

# TODO: Create 
# TODO: There is no BoTo3 copy directory to s3 because there is no directory concept in object storage. 
#       Thus copy all files one by one implement using Multithreading
# https://stackoverflow.com/questions/37178584/recursively-copying-content-from-one-path-to-another-of-s3-buckets-using-boto-in
# https://medium.com/analytics-vidhya/aws-s3-multipart-upload-download-using-boto3-python-sdk-2dedb0945f11
# 
# TODO: look at how to make sure if file succesfully uploaded
    #   https://stackoverflow.com/questions/44002090/how-to-check-if-boto3-s3-client-upload-fileobj-succeeded
    #   https://stackoverflow.com/questions/52880311/howto-put-object-to-s3-with-content-md5
    #   https://stackoverflow.com/questions/36179310/an-exception-the-content-md5-you-specified-did-not-match-what-we-received
# PREFIX = os.getenv("PREFIX")


@dataclass
class ErrorTypeClass:
    '''
        This class serves as blue print for Error, Error Message structured
        container. 

        The class needs to initialised as Object(Error, ErrorMessage) and
        returns the same structured Object. Attributes of which are dot accessible.
        Returns:
        ----------
        dictionary of the format {'error': error_type_name, 'error_msg': error_msg}
        
        Ex Code block:
            try:
                ...
            except Exception as e:    
                err = ErrorTypeClass(type(e).__name__, e)
                return err
    '''

    # error_type: Any # dynamically generated s,ome ExceptionType
    error_type_name: str
    error_msg: str

    # will be returned when class object is initialized
    def __new__(cls, name, msg):
        return {'error': name, 'error_msg': msg}


class S3_landing:
    '''
        For each session a seperate Object will be created.
    '''


    def __init__(self, region_name='us-east-1', max_attempts=2, mode='standard'):
        self.S3_config = Config(
            retries= {
                'max_attempts': max_attempts
                , 'mode': mode
            }
        )

        self.session = boto3.Session(profile_name='prfl-entertainment-retailer'
                            , region_name=self.region_name)
        self.s3_client = self.session.client('s3', config=self.S3_config)
        self.s3_resource = self.session.resource('s3')


    @staticmethod
    def _produce_key(full_file_name, prefix=None):
        '''
            This method produces the key for the object to be uploaded.
            It combines the date and the file name (last part of the full file name).

            Parameters:
            ----------------
            full_file_name: Fully qualified name of the file that will be uploaded.
        '''
        return date_utility.get_current_date() + (full_file_name.split('/')[-1] or full_file_name)


    def _check_upload_ok(check_key):
        '''
            This functions checks if the file was uploaded un-corrupted.

        '''


    def upload_file_to_bucket(self, file_name=None, bucket_name=None, key=None
            , check_transfered_data=False):#, is_object=False):
        # 14.
        '''
        Uploads a Single file to the specifed S3 location. Multipart upload is
        not supported. Max file size cannot be greater than 5 GB.

        Amazon S3 never adds partial objects; if you receive a success response,
        Amazon S3 added the entire object to the bucket.
        
        File to be uploaded can either be a compressed or uncompressed file.

        Parameter:
        --------------
        bucket_name: Name of the bucket S3 
        file_name  : Name of the file to be uploaded. 
        key        : Name of the oject in S3, ie. what will the name with which
                     the file will be stored in S3.

        Returns:
        ----------------
        Tuple(True|False, Dict type Response from S3)
        Format:
        ( success[= 0 | 1 | -1 ]
          , {
                'ResponseMetadata': 
                    {
                        'RequestId': 'VLxxxxxxxxxxxxxxxx6'
                        , 'HostId': 'JpKxxxxxxxxxxxx51s='
                        , 'HTTPStatusCode': 200
                        , 'HTTPHeaders': 
                            {   'x-amz-id-2': 'Jpxxxxxxxxxxxxxxxxxxxxxxxxxxx51s='
                                , 'x-amz-request-id': 'VLxxxxxxxxxxxxxxxx6', 'date': 'Wed, 26 Jan 2022 11:46:21 GMT'
                                , 'etag': '"5460b52943a915d16f119f3589aea281"', 'server': 'AmazonS3', 'content-length': '0'
                            }
                        , 'RetryAttempts': 0
                    }
                    , 'ETag': '"5460b52943a915d16f119f3589aea281"'
            }
        )
        '''
        
        if not key:
            key = file_name
        
        # this will be set to 
        #   : 0 on successful upload and file verifed
        #   : 1 on successfull upload operation completion
        #   : -1 (default); unsuccessful
        SUCCESS_CODE = -1 

        # calulate MD5 Hash only if data is to be checked for corruption during Network traversal
        if check_transfered_data:
            # pass md5 hash for data to be checked for corruption during Network traversal
            try:
                # base64 encoded - needed for providing 
                file_hash = checksum_utility.get_md5_checksum(file_name=file_name, is_file=True, base64_encode=True)
                # string type - will be compared with eTag
                file_hash_string = checksum_utility.get_md5_checksum(file_name=file_name, is_file=True)
            except Exception as e:
                print("Checksum utitlity produced error")
                res = ErrorTypeClass(type(e).__name__, e)


            # Actual File Upload Code block
            try:  
                if check_transfered_data: # need to use MD5 Hash
                    # provide file contents as binary
                    with open(file_name, 'rb') as f:
                        # print(">> Within Open")
                        res = self.s3_client.put_object(Body=f.read(), Bucket=bucket_name
                                    , Key=key, ContentMD5=file_hash)
                        
                        # file upload operation completion successful, still need 
                        # to check for corruption during Network traversal
                        SUCCESS_CODE = 1 
                    
                    # check if corrupted during Network trversal
                    if res['ETag'] == file_hash_string: 
                        SUCCESS_CODE = 0 # not corrupted
                
                else: # confirmation check for successful uploaded not required
                    res = self.s3_client.put_object(Body=file_name, Bucket=bucket_name, Key=key)
                    
                    # file upload operation completion successful 
                    # no guarantees for whether file corrupted or not
                    SUCCESS_CODE = 1
        
            except ClientError as e:
                print("MD5 error")
                res = ErrorTypeClass(type(e).__name__, e)

            except BotoCoreError as e:
                print("Boto core error")
                res = ErrorTypeClass(type(e).__name__, e)
            
            except FileNotFoundError as e:
                res = ErrorTypeClass(type(e).__name__, e)

            except OSError as e:
                res = ErrorTypeClass(type(e).__name__, e)
        
        return (SUCCESS_CODE, res)


    def upload_file_to_bucket_multipart(self, bucket_name=None, file_name=None
            , key = None, binary_object=False, multipart_threshold=25, max_concurrency=10
            , multipart_chunksize=25):
        '''
        uploads a file or binary in multiple parts. The destination file name will be same as
        The expected file name 

        Parameters:
        -------------------
        multipart_threshold: provided in Mb. 
        max_concurrency    :
        file_name          : Fully qualified name of the file to be uploaded.
        key                : Fully qualified name of the object in S3 
        '''

        config = TransferConfig(multipart_threshold=multipart_threshold * 1024
                    , max_concurrency=max_concurrency
                    , multipart_chunksize=multipart_chunksize * 1024, use_threads=True)

        try:
            if binary_object: # generally for zipped files
                with open(file_name, 'rb') as f:
                    self.s3_client.upload_fileobj(f, bucket_name, key, Config=config)
            else:
                self.s3_client.upload_file(file_name, bucket_name, key, Config=config)

        except ClientError as botoerror:
            #TODO: LOG
            print("Boto3 error")
        

    # No need will be covered as pload_file with compression --> is_object=True
    # def upload_directory_to_bucket(self, directory_path=None, bucket_name=None):
    #     pass


