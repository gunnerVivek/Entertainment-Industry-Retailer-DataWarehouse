import hashlib
import base64


def get_md5_checksum(string_value=None, file_name=None, is_file=False
        , hash_format_byte=False, base64_encode=False
        , read_file_in_chunks=False, chunk_size=4096):
    '''
        This function creates and returns a md5 - checksum value for a given
        file object or String value.

        Parameters:
        ---------------
        string_value    : type: str, default:  None
                   The value to be hashed. If :is_file is True then it will be ignored.
        
        file_name: type: str or FilePath, default: None
                   Name of the file to be hashed. If :is_file is False then it will be ignored.

        is_file  : type: Bool, default: False
                   If set to `True` uses :file_name to create checksum for the file object.
                   If set to `False` uses :value to create checksum for the string value.

        hash_format_byte: type: Bool, default: False
                          If set to True the hash will be generated in byte type
                          Else hash is generated in String type.

        base64_encoded  : type: Bool, default: False
                          If set to true returns a base64 encoded md5 hash.

        read_file_in_chunks: type: Bool, default: False
                          If set to True reads the file chunkwise, with each chunk
                             equal to as set by :chunk_size
                          Else reads the whole file as a single chunk.

        chunk_size: The size of each chunk if the file is being read chunk wise as 
                   opposed to being read in full.

        Returns:
        ------------
        md5 checksum either in String or Byte type as indicated in :hash_format_byte.

    '''

    if is_file:  # read the file and create checksum for it.
        
        hash_md5 = None
        if read_file_in_chunks:  # if file is too big to fit in memory
            
            hash_md5 = hashlib.md5()
            with open(file_name, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
        
        else: # read the file as whole
            with open(file_name, 'rb') as f:
                hash_md5 = hashlib.md5(f.read())

    else:   # create hash for String value provided
        hash_md5 = hashlib.md5(string_value.encode('utf-8'))
    

    # ########## return the MD5 hash ######################### #
    if base64_encode and hash_format_byte: # if base64 encoding is needed along with byte type
        return base64.b64encode(hash_md5.digest()) # base64 encoding works only with byte type

    elif base64_encode: # if base64 encoding is needed, but string type
        # base64 encoding works only with byte type, thus to convert to
        # string decode() is needed
        return base64.b64encode(hash_md5.digest()).decode("utf-8")

    elif hash_format_byte: # create hash in byte format
        return hash_md5.digest()

    else: # create hash in String format
        return hash_md5.hexdigest()
