import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
load_dotenv()
import os
import glob

# Set the CHUNK_SIZE that fits the best trade off (performance/speed vs memory usage)
CHUNK_SIZE = 10000

# Bring the enviroment variables from .env
USER= os.getenv('USER')
ACCOUNT= os.getenv('ACCOUNT')
PASSWORD= os.getenv('PASSWORD')
WAREHOUSE= os.getenv('WAREHOUSE')
DB_NAME= os.getenv('DB_NAME')
SCHEMA= os.getenv('SCHEMA')
AUTHENTICATOR = os.getenv('AUTHENTICATOR')
ROL= os.getenv('ROL')

# Put the CSVs path
csvs_file_path = ''

AUTOCOMMIT = True

def get_list_of_paths(csvs_file_path:str, ext:str) -> list:
    """
    Get list of paths from directory. Specify the extension to read ONLY 
    the those files.

    Args:
        csvs_file_path (str): Path where persist all the files
        ext (str): Suffix at the end of a computer file i.e.: csv

    Returns:
        all_files: List of str (with paths to each file)
    """
    # Grabs all the paths which belongs to csv files and store them in a list
    all_files = glob.glob(os.path.join(csvs_file_path, f"*.{ext}"))

    return all_files

def load_df_to_snowflake_tb(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, df_chunk: pd.DataFrame):
    """
    Wraps up write_pandas, which writes a Dataframe to an existing table on Snowflake.

    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connector
        fully_qualified_table_name (str): Name of the table
        df_chunk (pd.DataFrame): pd.DataFrame, batch of the file to be uploaded
    """

    # Method to write a dataframe to a Snowflake EXISTING table
    write_pandas(
            conn= conn,
            df= df_chunk,
            table_name= fully_qualified_table_name,
            database= DB_NAME,
            schema= SCHEMA
        )


def load_csv_to_snowflake_table(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, csv_path: str):
    """
    This function enables the user to upload large files to a Snowflake table. 
    Creates an iterator to read by chunks and avoids memory crash.

    Regarding pd.read_csv, it's defined 'python' as the engine reader because it's more intelligent and effective to separate rows.

    Then, instantiates a pool of threads to paralellize loading the dataframe, optimizing resources to boost performance.


    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connector
        fully_qualified_table_name (str): Name of the Snowflake table
        csv_path (str): Path where persist the file
    """

    # Loads the data from CSV in CHUNKS to avoid memory crash. We infer date time format to avoid missing/null values.
    df_iterator = pd.read_csv(
            filepath_or_buffer= csv_path, 
            engine='python', 
            infer_datetime_format= True, 
            error_bad_lines=False, 
            warn_bad_lines=True, 
            chunksize= CHUNK_SIZE, 
            iterator=True)

    # The idea is to pool a bunch of chunks by each thread
    with ThreadPoolExecutor() as executor:
        list(executor.map(lambda chunk: load_df_to_snowflake_tb(conn, fully_qualified_table_name, chunk), df_iterator))


def load_csvs_to_snowflake_table(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, csv_file_paths: list[str]):
    """
    Performs the upload using the list of paths as the iterable. 

    Catch the exception if the reading task generates any issue.

    If exception exists, append the path of the corrupted file to a list.

    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Snowflake connector
        fully_qualified_table_name (str): Name of the Snowflake table
        csv_file_paths (list[str]): Lists of paths where persist the files
    """
    # If any file can't be loaded because it's corrupted, append it to this list
    unprocessed_files = []
    for csv_path in csv_file_paths:
        try:    
            load_csv_to_snowflake_table(conn, fully_qualified_table_name, csv_path)
        except Exception as ex:
            print(ex)
            print(f'Error reading CSV: {csv_path}')
            unprocessed_files.append(csv_path)
    if len(unprocessed_files) == 0:
        print('All files were succesfully uploaded')
    else:
        print(f'{unprocessed_files}')


def run():

    conn = snowflake.connector.connect(
            user= USER,
            password= PASSWORD,
            account= ACCOUNT,
            database= DB_NAME,
            schema=SCHEMA,
            rol=ROL,
            warehouse=WAREHOUSE
            )

    fully_qualified_table_name = 'TESTEO_12345'

    csv = 'csv'

    # Go to the top of this script to define the csvs_file_path
    csv_file_paths = get_list_of_paths(csvs_file_path= csvs_file_path, ext=csv)

    load_csvs_to_snowflake_table(conn= conn, fully_qualified_table_name= fully_qualified_table_name, csv_file_paths= csv_file_paths)


def main_loader():
    run()

if __name__ == '__main__':
    main_loader()  





