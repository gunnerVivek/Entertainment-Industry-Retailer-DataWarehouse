
import pathlib
import os.path

ROOT_DIR = pathlib.Path('src').resolve().parent
SOURCE_DATA_PATH = os.path.join(ROOT_DIR, "source_system_data")
SOURCE_SYSTEM_OUT_LOG_PATH = os.path.join(pathlib.Path(r'src/process_source_system/logs/').resolve(), 'output')
SOURCE_SYSTEM_ERR_LOG_PATH = os.path.join(pathlib.Path(r'src/process_source_system/logs/').resolve(), 'error')