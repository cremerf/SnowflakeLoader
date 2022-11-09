import snowflake.connector
import snowflake.connector.pandas_tools
import multiprocessing
import pandas as pd
from functools import partial
from snowflake.connector.pandas_tools import write_pandas
import os
import glob


def load_csvs_to_snowflake_table(conn: snowflake.connector.connection.SnowflakeConnection, fully_qualified_table_name: str, csvs_file_path: str, csv_file_paths: list[str]):

    cur = conn.cursor()

    all_files = glob.glob(os.path.join(csvs_file_path, "*.csv")) 

    pd.read_csv(filepath_or_buffer='', engine='python', infer_datetime_format= True, error_bad_lines=False, warn_bad_lines=True)


    pool = multiprocessing.Pool()
    func = partial(write_pandas, conn ,fully_qualified_table_name)
    pool.map(func, csv_file_paths)
    pool.close()
    pool.join()

    None

def run():

    conn = snowflake.connector.connect(
            user= os.getenv('USER'),
            account= os.getenv('ACCOUNT'),
            password= os.getenv('PASSWORD'),
            warehouse= os.getenv('WAREHOUSE'),
            database= os.getenv('DB_NAME'),
            schema= os.getenv('SCHEMA'),
            authenticator = os.getenv('AUTHENTICATOR'),
            autocommit = True
            )

    load_csvs_to_snowflake_table(conn= conn)

def main_loader():
    run()

if __name__ == '__main__':
    main_loader()  





