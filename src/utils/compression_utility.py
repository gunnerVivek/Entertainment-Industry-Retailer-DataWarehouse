import os
from zipfile import ZipFile
from zipfile import BadZipFile, LargeZipFile
import logging

from utils.common_utils import common_utility


def _compress_file(input_file, out_file_name=None):
    '''
    Compresses a single file to a zip file
    '''
    if not out_file_name:
        out_file_name = input_file+'.zip'

    try:
        with ZipFile(out_file_name, 'w') as zip:
            zip.write(input_file)
    except BadZipFile:
        print("Bad zip")
    except LargeZipFile:
        print("require Zip64 functionality")
    except FileNotFoundError as e:
        # Log
        raise e
    except OSError as e:
        # Log
        raise e


def _compress_directory(input_files, out_file_name=None):
    '''
        files        : A list of fully qualified file names
        out_file_name: Name of the output zipped file
    '''
    if not common_utility.is_iterable(input_files, str_ok=False):
        raise TypeError("`input_files` should be iterable, ex: list type.")

    try:
        with ZipFile(out_file_name, 'w') as zip:
            for file in input_files:
                zip.write(file)
    except BadZipFile as e:
        print("Bad zip")
        raise e
    except LargeZipFile as e:
        print("require Zip64 functionality")
        raise e
    except FileNotFoundError as e:
        # Log
        raise e
    except OSError as e:
        # Log
        raise e


def _get_all_files(directory):
    '''
    Get list of all files present in the directory and sub-directories. 

    Parameters:
    ------------
    directory: Fully Qualified directory name of the diretory to be zipped.
               All the files and files in the subdirectories will be traversed
               and included in te zip. Same structure as the underlying file structure
               is maintained.
    '''
    full_file_path = []
    print(directory)
    try:
        for root_dir, sub_dirs, files in os.walk(directory):
            print(root_dir, sub_dirs, files)
            for f_name in files:
                full_file_path.append(os.path.join(root_dir, f_name))
        
        return full_file_path
    
    except OSError as e:
        # log
        # log.warn(f"Error ocuured while traversing directory: {directory}")
        ''


# def dump_zipped_directory(directory):
    
#     for 

def dump_zipped_file(input_files, output_file_name, is_directory=True):
    '''
    input_files: Can be a list or a single file. The values are expected
    '''

    if is_directory:
        # input_files shoud be iterable
        if not common_utility.is_iterable(input_files, str_ok=False):
            raise TypeError("`input_files` should be iterable, ex: list type.")

        _compress_directory(input_files, output_file_name)

    else:
        # input_files shoud not be iterable (string expected)
        # string is not considered file name in this case
        if common_utility.is_iterable(input_files, str_ok=False):
            raise TypeError("`input_files` should be String not iterable.")

        _compress_file(input_files, output_file_name)
