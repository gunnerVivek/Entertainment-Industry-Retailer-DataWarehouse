
from pathlib import Path

def get_file_size(full_file_name):
    '''
        Returns size of a file object in Bytes. Accepts only files as input.

        Parameters:
        -------------------
        full_file_name: Fully qualified Name of the file object
    '''

    try:
        # needed for avoiding directories, thay are also valid file path
        if Path(full_file_name).is_file():
            file_size = Path(full_file_name).stat().st_size
        else:
            raise OSError(f"Not valid file path: {full_file_name}")
    except OSError as e:
        print(f"May be not file or error in opening file: {full_file_name}")
        raise e
    except Exception as e:
        print(f"Unexpected Exception ocurred while calculating file size of: {full_file_name}")
        raise e

    return file_size
