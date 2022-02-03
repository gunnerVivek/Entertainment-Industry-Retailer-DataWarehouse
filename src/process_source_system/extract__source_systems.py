'''
    Get Data from the Source data systems as csv files.
    It provides functions to get either full data extract or incremental data extract.
    The source data systems are Microsoft SQL Server.

    Extraction is done in the following steps:
    1. Get the list of tables in the database.
    2. Get the format of each table and dump it.
    3. Get the data from each table and dump it into a csv file. 

    # TODO: update extract_format_only() with log file and error file.
    # TODO: Add checks for db_name, table, etc.
    # TODO: Add logging.
    # TODO: Add exception handling.
    # TODO: Add parallel processing.
    # TODO: Add support fot -S option in bcp command. Extend to any uri.
    # TODO: Add Pyodbc exception implementation
    #       https://github.com/mkleehammer/pyodbc/wiki/Exceptions
    #       https://www.python.org/dev/peps/pep-0249/#exceptions
'''

import subprocess
import sys
import os

import pyodbc
import argparse

from src.process_source_system import SOURCE_DATA_PATH, SOURCE_SYSTEM_OUT_LOG_PATH, SOURCE_SYSTEM_ERR_LOG_PATH
from src.utils import date_utility


def get_connection():
    '''
        Returns a connection to the database.
    '''
    conn_str = '''
               Driver={ODBC Driver 17 for SQL Server};
               Server=localhost;
               Database=zerodha_NFO;
               Trusted_Connection=yes;
             '''
    connection = pyodbc.connect(conn_str)
    return connection


def create_directory_if_not_exists(path):
    '''
        Creates the directory if it does not exist.
        If the directory exists, it does nothing.
    '''
    if not os.path.exists(path):
        # exist_ok makes sure no OSError is Raised in case the directory already exist
        os.makedirs(path, exist_ok=True) 

        
def get_tables(db_name, connection, schemas=['dbo']):
    '''
        Fetches names of all tables in the provided database.

        The provided Database, Schema and Connection should alreday exist.

        Parameters
        ----------------
        db_name: Fully qualified Name of the intended databse
    '''

    # This query will return all the tables in the database.schema
    # last part of the query (the format string part), will be replaced by
    # as many ? as many parameters (schemas) are required to be queried in the DB.
    table_name_query =\
        f'''
        SELECT DISTINCT(TABLE_NAME) FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'Base Table'
	        AND TABLE_SCHEMA IN ({",".join("?" * len(schemas))})
        ;
    '''

    try:
        # connection.cursor returns list of tuples
        return [x[0] for x in
                connection.cursor()
                .execute(table_name_query, schemas)
                .fetchall()
                ]

    except Exception as e:
        print(e)  # TODO: Logging
    finally:
        connection.close()


def dump_table_format(db_name, tbl_name, outputfile_path, schema_name='dbo'
                    , log_file_path = None, error_file_path = None
                    , userName=None, password=None): # error file and log file 
    '''
        This method is used to get the format (schema) of the table in XML format.
        The xml schema is dumped into the output file location.
        It only supports one table name for each instance of the function call.

        Either both userName and password or none of them should be provided.
        If both are provided, the userName and password will be used to connect to the database.
        If none of them are provided, the connection will be attempted with trusted mode.

        The provided Database, Schema and Table should alreday exist.

        Parameters
        ----------------
        tbl_name       : Name of the table whose format is to be fetched.
                         it should be the fully qualifed name of the table.
        outputfile_path: Path to the directory where the csv files will be dumped.
                         The directory will be created if not exist.
                         Ex:- data_dir/db_name/YYYYMMDD/ 
        userName        : Username of the user. This is optional. If not provided, 
                          default id to use -T (trusted connection) option. 
        password        : Password of the user.


        bcp jade.dbo.address_type format null -c -x -f .\original-data\jade\address_type_format.xml -t"," -T 
    '''
    try:
        create_directory_if_not_exists(outputfile_path)
        
        full_TableName = f'{db_name}.{schema_name}.{tbl_name}'      
        
        outputfile_name = os.path.join(outputfile_path, f'{tbl_name}_format.xml') # data_dir/yyyymmdd/source(db)/table_format.xml

        # if not log_file_path:
        #     log_file_path = os.path.join(SOURCE_SYSTEM_OUT_LOG_PATH, date_utility.get_current_date())
        log_file_name = os.path.join(log_file_path or 
                            os.path.join(SOURCE_SYSTEM_OUT_LOG_PATH, date_utility.get_current_date())
                            , 'output.log')
        # if not error_file_path:
        #     error_file_path = os.path.join(SOURCE_SYSTEM_ERR_LOG_PATH, date_utility.get_current_date())
        error_file_name = os.path.join(error_file_path or
                            os.path.join(SOURCE_SYSTEM_ERR_LOG_PATH, date_utility.get_current_date())
                            , 'error.log')

        fmt_stmt = f'bcp {full_TableName} format null -c -x -f {outputfile_name} -t"," -o {log_file_name} -e {error_file_name}'
        
        if (userName is None) or (password is None):
            format_command = fmt_stmt+ " " + '-T'
        else:
            format_command = fmt_stmt+ " " + f'-U {userName} -P {password}'
        
        subprocess.run(format_command)

    except Exception as e:
        print(e)


def db_dump_full_extract(db_name, tbl_name, outputfile_path, schema_name='dbo'
                    , log_file_path = None, error_file_path = None
                    , userName=None, password=None):
    '''
        This method is used to extract one table from database for each invocation.
        The data is dumped into the output file location as csv.

        The provided Database, Schema and Table should alreday exist. 
        

        Parameters
        ----------------
        db_name        : Fully qualified Name of the intended databse
        tbl_name       : Name of the table whose data is to be extracted.
        outputfile_path: Path to the directory where the csv files will be dumped.
                         The directory will be created if not exist.
                         Ex:- data_dir/db_name/YYYYMMDD/ 
        schema_name    : Name of the schema from which the data is to be extracted.
                         Default is 'dbo'.
        userName       : Username of the user. This is optional. If not provided,
                         default id to use -T (trusted connection) option.
        password       : Password of the user.

        bcp jade.dbo.address_type out .\original-data\jade\address_type.csv -c -t"," -T
    '''
    try:
        create_directory_if_not_exists(outputfile_path)

        full_outputFileName = os.path.join(outputfile_path, f'{tbl_name}.csv')

        # set the output log file name
        log_file_name = os.path.join(log_file_path or 
                            os.path.join(SOURCE_SYSTEM_OUT_LOG_PATH, date_utility.get_current_date())
                            , 'output.log')
        
        # set the error log file name
        error_file_name = os.path.join(error_file_path or
                            os.path.join(SOURCE_SYSTEM_ERR_LOG_PATH, date_utility.get_current_date())
                            , 'error.log')

        stmt = f'bcp {db_name}.{schema_name}.{tbl_name} out {full_outputFileName} -c -t"," -o {log_file_name} -e {error_file_name}'

        if (userName is None) or (password is None):
            statement = stmt + ' ' + '-T'
        else:
            statement = stmt + ' ' + f'-U {userName} -P {password}'
            
        subprocess.run(statement)
 
    except Exception as e:
        # print(e)
        raise e


def db_dump_incremental_extract(db_name, tbl_name, last_extract_time, lte_column, current_extract_time
                    , outputfile_path, log_file_path = None, error_file_path = None
                    , schema_name='dbo', userName=None, password=None):
    
    '''
        The provided Database, Schema and Table should alreday exist.

        Parameters
        ----------------
        db_name          : Name of the database.
        tbl_name         : Name of the table whose format is to be fetched.
        outputfile_path  : Path to the directory where the csv files will be dumped.
                           The directory will be created if not exist.
                           Ex:- data_dir/db_name/YYYYMMDD/ 
        last_extract_time: Time of the last extract.
        lte_column       : used to determine the last extract time.
        schema_name      : Name of the schema.
        userName         : Username of the user. This is optional. If not provided,
                           default id to use -T (trusted connection) option.
        password         : Password of the user.

        Returns
        ----------------
        None. Objective is to dump the records in the output directory


        BCP "SELECT [SalesOrderID], [SalesOrderDetailID],[CarrierTrackingNumber] FROM [AdventureWorks2014].[dbo].[SalesOrderDetail]"
        queryout C:\SOQueryOut.txt -S hqdbt01\SQL2017 -T -c

    '''
    try:
        
        if not last_extract_time < current_extract_time:
            raise ValueError(f'Last extract time {last_extract_time} is greater than current extract time {current_extract_time}')

        full_TableName = f'{db_name}.{schema_name}.{tbl_name}'
        
        query = f'SELECT * FROM {full_TableName} WHERE'+\
                f'{full_TableName}.{lte_column} > {last_extract_time} and'+\
                f'{full_TableName}.{lte_column} <= {current_extract_time}'
        
        create_directory_if_not_exists(outputfile_path)
        
        full_outputFileName = os.path.join(outputfile_path, f'{tbl_name}.csv')
       
        # set the output log file name
        log_file_name = os.path.join(log_file_path or 
                            os.path.join(SOURCE_SYSTEM_OUT_LOG_PATH, date_utility.get_current_date())
                            , 'output.log')
        
        # set the error log file name
        error_file_name = os.path.join(error_file_path or
                            os.path.join(SOURCE_SYSTEM_ERR_LOG_PATH, date_utility.get_current_date())
                            , 'error.log')

        
        stmt = f'bcp {query} queryout {full_outputFileName} -c -t"," -o {log_file_name} -e {error_file_name}'

        if (userName is None) or (password is None):
            statement = stmt + ' ' + '-T'
        else:
            statement = stmt + ' ' + f'-U {userName} -P {password}'
        
        subprocess.run(statement)    
    
    except Exception as e:
        print(e)
        # TODO: Per Exception implement


def extract(db_name, last_extract_time=None, lte_column=None, current_extract_time=None
            , table_names=None, schemas=['dbo'], extract_mode='full'
            , extract_format=False, top_level_directory=SOURCE_DATA_PATH, date=None
            , log_file_path = None, error_file_path = None
            , username=None, password=None):
    '''
    This is the Master extraction function and intended to serve as Entry 
    point ot the Extract system.

    It facilitates extraction of multiples schemas and tables, but only single
    database per invocation of the fucntion.

    Any data extracted will be dumped to the output directory location.
    The output directory will have the format as top_level_directory/yyyymmdd/db_name.
    In the further downstream where data is actually extracted and dumped the directory 
    structure is expected to be further appended with table Name.
    Ex: top_level_directory/yyyymmdd/db_name/tbl_name.
    If the directory does not exist it will be created, not just rightmost leaf but the
    whole path.


    Parameters
    -----------
    db_name            : Name of the database to extract.
    table_names        : Name of the tables to be extracted. It is optional and if 
                         not provided defaults to None; in which case all tables in
                         the Database is extracted.
    schemas            : Name of the schemas to which the tables belongs to in DB.
                         It is optional and defaults to 'dbo'. 
    extract_mode       : Data extract mode.
                         Either 'full' or 'incremental' only supported at this moment.
    extract_format     : Whether to extract the formats of the tables
    top_level_directory: Top level Directory of the data store path
    date               : A date string in 'YYYYMMDD' format. It appended to 
                         :param: top_level_directory to construct output path to dump table
                         data and/or table format.
                         Defaults to Null; in that case current date in UTC is used.
    username           : Username of the user. This is optional. If not provided, 
                         trusted connection is assumed.
    password           : Password of the user.

    '''

    try:
        if not table_names:
            connection = get_connection()
            table_names = get_tables(db_name=db_name, connection=connection, schemas=schemas)
              
        # this will be unique to every date for every db
        # this will be passed to the extract method downstream 
        # where the full path will be constructed; table name will be appended
        if date: # if date is provided
            output_directory = os.path.join(top_level_directory, date, db_name)
        else: # if date is not provided; use current date
            output_directory = os.path.join(top_level_directory, date_utility.get_current_date(), db_name)
                
        if extract_mode == "full" and extract_format and username:
            # This segment is to extract the full table, format of the table 
            # using the Username and Password combination provied by the user.
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        , 'userName':username, 'password':password}
                    
                    # dump table format
                    dump_table_format(**params)

                    # dump table data
                    db_dump_full_extract(**params)

        elif extract_mode == "full" and extract_format:
            # This segment is to extract the full table, format of the table 
            # using trusted connection (-T).

            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        }
                    
                    # dump table format
                    dump_table_format(**params)
                    
                    # dump table data
                    db_dump_full_extract(**params)
        
        elif extract_mode == "full" and username:
            # This segment is to extract the full table without table format.
            # using the Username and Password combination provied by the user.
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        , 'userName':username, 'password':password}
                    
                    # dump table data
                    db_dump_full_extract(**params)

        elif extract_mode == "full":
            # This segment is to extract the full table without table format.
            # using trusted connection (-T).
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        }
                    
                    # dump table data
                    db_dump_full_extract(**params)
                    
        elif extract_mode == "incremental" and extract_format and username:
            # extract incremental data and format of the table using the 
            # Username and Password combination provied by the user.
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        , 'userName':username, 'password':password}
                    
                    # dump table format
                    dump_table_format(**params)

                    # dump table data
                    # add remaing params required for incremental extract
                    db_dump_incremental_extract(**params.update({'last_extract_time':last_extract_time
                                                    , 'lte_column':lte_column
                                                    , 'current_extract_time':current_extract_time})
                            )

        elif extract_mode == "incremental" and extract_format:
            # incrementally extract data and format of the table
            #  using trusted connection (-T).
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        }
                    
                    # dump table format
                    dump_table_format(**params)

                    # dump table data
                    # add remaing params required for incremental extract
                    db_dump_incremental_extract(**params.update({'last_extract_time':last_extract_time
                                                    , 'lte_column':lte_column
                                                    , 'current_extract_time':current_extract_time})
                            )

        elif extract_mode == "incremental" and username:
            # incrementally extract the data using the username and password
            # but without extracting table format.
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        , 'userName':username, 'password':password
                        , 'last_extract_time':last_extract_time, 'lte_column':lte_column
                        , 'current_extract_time':current_extract_time}
                    
                    # dump table data
                    db_dump_incremental_extract(**params)

        elif extract_mode == "incremental":
            # incrementally extract the data, but without table format
            # and using trusted connection (-T).
            for schema in schemas:
                for tbl_name in table_names:
                    params = {'db_name':db_name, 'tbl_name':tbl_name
                        , 'outputfile_path':output_directory, 'schema_name':schema
                        , 'log_file_path':log_file_path, 'error_file_path':error_file_path
                        , 'last_extract_time':last_extract_time, 'lte_column':lte_column
                        , 'current_extract_time':current_extract_time}
                    
                    # dump table data
                    db_dump_incremental_extract(**params)
        else:
            raise ValueError("Provided Parameters Not Valid")

    except pyodbc.Error as e:
        print(e)
        
    except ValueError as ve:
        print(ve)
        sys.exit(1)
    
    except Exception as e:
        print(e)
    finally:
        # close the connection if exist
        try:
            connection
        except (NameError, Exception): # overall exception is to deal with already closed connection exception
            ''
        else:
            connection.close()


def extract_format_only(db_name, table_names=None, schemas=['dbo'], username=None, password=None, top_level_directory=SOURCE_DATA_PATH):
    '''
        This method is to extract only schema of the table, without extracting 
        the data.

        Parameters:
            db_name            : Name of the database.
            table_names        : List of table names.
            schemas            : List of schemas.
            username           : Username of the database.
            password           : Password of the database.
            top_level_directory: Top level directory where the data will be dumped.
                                 

    '''
    try:
        # this will be passed to the extract method downstream 
        # where the full path will be constructed; table name will be appended
        output_directory = os.path.join(top_level_directory, date_utility.get_current_date(), db_name)
        
        if not table_names:

            connection = get_connection()
            table_names = get_tables(db_name=db_name, connection=connection, schemas=schemas)

            # try:
            #     connection = get_connection()
            #     table_names = get_tables(db_name=db_name, connection=connection, schemas=schemas)
            
            # except Exception as e:
            #     raise e
            # finally:
            #     # close the connection if exist
            #     try:
            #         connection
            #     except (NameError, Exception):
            #         ''
            #     else:
            #         connection.close()

        for schema in schemas:
            for tbl_name in table_names:
                if username:
                    # dump table format using Username and Password proivied by the user.
                    dump_table_format(db_name=db_name, tbl_name=tbl_name
                        , outputfile_path=output_directory, schema_name=schema
                        , userName=username, password=password)
                else:
                    # dump table format using Trusted Connection.
                    dump_table_format(db_name=db_name, tbl_name=tbl_name
                        , outputfile_path=output_directory, schema_name=schema)

    except pyodbc.Error as e:
        print(e)
        
    except ValueError as ve:
        print(ve)
        sys.exit(1)
    
    except Exception as e:
        print(e)

    finally:
                # close the connection if exist
                try:
                    connection
                except (NameError, Exception):
                    ''
                else:
                    connection.close()


if __name__ == '__main__':
    
    import argparse

    argparser = argparse.ArgumentParser(description="Extracts data from source systems,\
        inclduing data and table schema.")

    argparser.add_argument('-db', '--db_name', help='Name of the data base to extract from'
                        , required=True) # nargs by deffault is 1 -> 'item_itself'
    argparser.add_argument('-tbl', '--table_names', help='Name of the table(s) to extract from'
                        , default=None, required=False, nargs='*')
    argparser.add_argument('-sch', '--schemas', help='Name of the schema(s) to extract from'
                        , default=['dbo'], required=False, nargs='*')
    argparser.add_argument('-lte', '--lte_column', help='Name of the column to use as the last extract time'
                        , default=None, required=False) # nargs by deffault is 1 -> 'item_itself'
    argparser.add_argument('-cet', '--current_extract_time', help='Current extract Time to be used as upper bound for incremental extract'
                        , default=None, required=False) # nargs by deffault is 1 -> 'item_itself'
    argparser.add_argument('-le', '--last_extract_time', help='Last extract Time to be used as lower bound for incremental extract'
                        , default=None, required=False) # nargs by deffault is 1 -> 'item_itself'
    argparser.add_argument('-em', '--extract_mode', help='Extract mode to be used'
                        , default='full', choices=['full', 'incremental'], required=False) # nargs by deffault is 1 -> 'item_itself'
    argparser.add_argument('-ef', '--extract_format', help='Whether to Extract format (schema) of the table. Default is set to True.'
                        , type= bool, default=True, choices=[True, False],required=False)
    argparser.add_argument('-u', '--username', help='Username to be used for connection'
                        , default=None, required=False)
    argparser.add_argument('-p', '--password', help='Password to be used for connection'
                        , default=None, required=False)

    args = argparser.parse_args()
    
    extract(**vars(args))
    