
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from botocore.exceptions import BotoCoreError # base exception class for all Boto3 error types
from pathlib import Path
from dataclasses import dataclass

from src.utils.common_utils import checksum_utility
from src.utils.file_utility import get_file_size
from src.utils.aws_utils.aws_utility import verify_multipart_uploaded_fl, calculate_s3_etag
from config.definitions import MB


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

    # will be returned when class object is instantiated
    def __new__(cls, name, msg):
        return {'error': name, 'error_msg': msg}


class S3_landing:
    '''
        For each session a seperate Object will be created.
    '''


    def __init__(self, region_name='us-east-1', max_attempts=3, mode='standard', profile='default'):
        self.S3_config = Config(
            retries= {
                'max_attempts': max_attempts
                , 'mode': mode
            }
        )

        # self.session = boto3.Session(profile_name='prfl-entertainment-retailer'
        #                     , region_name=region_name)
        self.session = boto3.Session(region_name=region_name, profile_name=profile)
        self.s3_client = self.session.client('s3', config=self.S3_config)
        self.s3_resource = self.session.resource('s3')


    def upload_file_to_bucket(self, file_name=None, bucket_name=None, key=None
            , check_transfered_data=False):#, is_object=False):
        '''
        Uploads a Single file to the specifed S3 location. Multipart upload is
        not supported. Max file size cannot be greater than 5 GB.

        Amazon S3 never adds partial objects; if you receive a success response,
        Amazon S3 added the entire object to the bucket.
        
        File to be uploaded can either be a compressed or uncompressed file.

        Parameter:
        --------------
        bucket_name: Name of the bucket S3 
        file_name  : Fully Qualified Name of the file to be uploaded. 
        key        : Name of the oject in S3, ie. what will the name with which
                     the file will be stored in S3.

        Returns:
        ----------------
        In case verification is not required or verification fails (but object upload successful)
        will still return SUCCESS_CODE = 1. This is to allow for flexibility to acomodate any
        future change in ETag algorithm calculation for AWS.
        
        Format:
        ( success[= 0 | 1 | -1 ]
          , ErrorTypeClass(type(e).__name__, e)  
        )
        '''
        
        if not key:
            key = Path(file_name).name

        
        # this will be set to 
        #   : 0 on successful upload and file verifed
        #   : 1 on successfull upload operation completion
        #   : -1 (default); unsuccessful
        SUCCESS_CODE = -1 

        # will hold error type if Error, empty Otherwise
        res = {}

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

            except Exception as e:
                res = ErrorTypeClass(type(e).__name__, e)

        return (SUCCESS_CODE, res)


    def upload_file_to_bucket_multipart(self, bucket_name=None, file_name=None
            , key = None, binary_object=False, multipart_threshold=25, max_concurrency=10
            , threshold_unit=MB, multipart_chunksize=25, chunk_size_unit=MB):
        '''
        uploads a file or binary in multiple parts. The destination file name will be same as
        The expected file name 

        Parameters:
        -------------------
        multipart_threshold: provided in Mb. 
        max_concurrency    :
        file_name          : Fully qualified name of the file to be uploaded.
        key                : Fully qualified name of the object in S3


        Returns:
        ---------------------
        -1: Upload failed
        1: Means No Boto3 error ocurred or client Access error occured while uploadig the object.
           ie. upload operation attempted succesfully without Network or Access error. 
           Does not gurantee correct file upload
        0: File upload succesfull and file contents verified.
        
        In case verification is not required or verification fails (but object upload successful)
        will still return SUCCESS_CODE = 1. This is to allow for flexibility to acomodate any
        future change in ETag algorithm calculation for AWS.
        '''
        # set name of the file in S3 bucket
        if not key:
            key = Path(file_name).name    

        # this will be set to 
        #   : 0 on successful upload and file verifed
        #   : 1 on successfull upload operation completion
        #   : -1 (default); unsuccessful
        SUCCESS_CODE = -1 

        config = TransferConfig(multipart_threshold=multipart_threshold * threshold_unit
                    , max_concurrency=max_concurrency
                    , multipart_chunksize=multipart_chunksize * chunk_size_unit, use_threads=True)
        
        # Upload the file 
        try:
            if binary_object: # generally for zipped files
                with open(file_name, 'rb') as f:
                    self.s3_client.upload_fileobj(f, bucket_name, key, Config=config)
            else:
                self.s3_client.upload_file(file_name, bucket_name, key, Config=config)

            # upload operation attempted succesfully without Network or Access error
            SUCCESS_CODE = 1

        except ClientError as botoerror:
            #TODO: LOG
            print(">>Boto3 error", botoerror, sep="\n")
        #TODO: all other exceptions
        except Exception as e:
            #  Handle it here
            ''
        else:
            print("Multipart Upload attempted")
        ### verify file not corrupted during Network Traversal    #####
        
        # calculate the ETag
        try:
            eTag = calculate_s3_etag(file_name, config.multipart_chunksize)
        
        except FileNotFoundError as e:
            print(f"FileNotFoundError occured while calculating expected ETag, while reading file: {file_name}")

        except OSError as e:
            print(f"OSError occured while calculating expected ETag, while reading file: {file_name}")
            
        # check for non - corrupt upload
        else:
            try:
                if verify_multipart_uploaded_fl(self.s3_client, self.s3_resource, eTag, bucket_name, key):
                    SUCCESS_CODE = 0

            except BotoCoreError as e:
                print(f"Produced BotoCoreError while verifying Multipart Upload of: S3://{bucket_name}/{key}")
                
            except Exception as e:
                print(f"Unknown Exception occured while verifying Multipart Upload of: S3://{bucket_name}/{key}")

        # SUCCESS_CODE = -1: Failed upload
        return SUCCESS_CODE
