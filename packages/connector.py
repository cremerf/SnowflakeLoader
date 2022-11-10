import snowflake.connector
import snowflake.connector.pandas_tools
import multiprocessing
import pandas as pd
from functools import partial
from snowflake.connector.pandas_tools import write_pandas
import os
import glob

CHUNK_SIZE = 1000

USER = os.getenv('USER'),
ACCOUNT = os.getenv('ACCOUNT'),
PASSWORD = os.getenv('PASSWORD'),
WAREHOUSE = os.getenv('WAREHOUSE'),
DATEBASE= os.getenv('DB_NAME'),
SCHEMA = os.getenv('SCHEMA'),
AUTHENTICATOR = os.getenv('AUTHENTICATOR'),
AUTOCOMMIT = True

def get_list_of_paths(csvs_file_path:str):

    all_files = glob.glob(os.path.join(csvs_file_path, "*.csv"))

    return all_files

def load_df_to_snowflake_tb(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, df_chunk: pd.DataFrame):

    write_pandas(
            conn= conn,
            df= df_chunk,
            table_name= fully_qualified_table_name,
            database= DATEBASE,
            schema= SCHEMA
        )


def load_csv_to_snowflake_table(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, csv_path: str):

    df_iterator = pd.read_csv(
            filepath_or_buffer= csv_path, 
            engine='python', 
            infer_datetime_format= True, 
            error_bad_lines=False, 
            warn_bad_lines=True, 
            chunksize= CHUNK_SIZE, 
            iterator=True)


    for chunk in df_iterator:
        load_df_to_snowflake_tb(conn, fully_qualified_table_name, chunk)


def load_csvs_to_snowflake_table(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, csv_file_paths: list[str]):

    unprocessed_files = []
    for csv_path in csv_file_paths:
        try:    
            load_csv_to_snowflake_table(conn, fully_qualified_table_name, csv_path)
        except Exception as ex:
            print(f'Error reading CSV: {csv_path}')
            unprocessed_files.append(csv_path)
    if len(unprocessed_files) == 0:
        print('All files were succesfully uploaded')
    else:
        print(f'{unprocessed_files}')




    '''
    pool = multiprocessing.Pool()
    func = partial(write_pandas, conn, fully_qualified_table_name)
    pool.map(func, csv_file_paths)
    pool.close()
    pool.join()
    '''

def run():

    fully_qualified_table_name = ''

    csvs_file_path = ''

    load_csvs_to_snowflake_table(conn= conn, fully_qualified_table_name= fully_qualified_table_name, csvs_file_path= csvs_file_path)


def main_loader():
    run()

if __name__ == '__main__':
    main_loader()  





