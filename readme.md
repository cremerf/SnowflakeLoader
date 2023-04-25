# Loader to Snowflake

![SNFLOGO](https://logos-world.net/wp-content/uploads/2022/11/Snowflake-Logo.png)

- To kickstart this project, first you must set up a database in Snowflake. You can refer to this medium post: [Getting Started with Snowflake](https://medium.com/@datacouch/getting-started-with-snowflake-6a09688fed35) to help you with the setup and some entry-level theory about this provider.
<br>

- This Python project focuses on using multithreading without crashing the memory to load up data into a data warehouse. It basically grabs a bunch of CSV files and loads into a Snowflake database. 

To install the dependencies required by the project, open the terminal and execute the following command: (remember to build a virtual/working env with venv or Conda before)
<br>
```
pip install -r requirements.txt
```
<br>

The code is full of comments and each function has a docstring that explains its purpose. Here's an overview of the project structure:

### Project Structure


#### Packages:

[Connector](packages\connector.py): Set of functions to execute what was asked. 

- get_list_of_paths()
- load_df_to_snowflake_tb()
- load_csv_to_snowflake_table()
- load_csvs_to_snowflake_table()
- run(): wrap up the function’s program 

In ```run()``` you must define the next variables:
<br>
1) The Snowflake connector with the required variables (which you will previously define in **`.env`**)
1) The name of the table in snowflake to upload the csv files
1) In order to meet the user's requirements, the program must be passed a list of CSV file paths that are specific to the user's needs. While I tested the function by using an auxiliary function called  get\_list\_of\_paths to create a list of paths, this list should be replaced with the client's own list of paths in order to properly use the program for their own data.

- **`.env`**: A file where you define the variables required to connect to Snowflake via Python.
- **`Blackboard.ipynb`**: A Jupyter Notebook file where you can test some code before executing [main](main.py).
- **`main.py`**: This script runs the program once you define .env and what is needed in `run()`.

#### Modifications if deployed in Production

In a real world project, users or clients wants to have a service deployed, by service, I mean an application(product) that executes several connected instructions and produces some useful output. In order to deliver this kind of service, here are some possible upgrades to consider:

- **Full development in OOP**: The project can be converted into a scalable product by leveraging OOP concepts. The functions can be packaged and converted into a scalable product, which can be used for other projects or demands from our client or any other client that has the same necessity. This will honor the DRY (Don’t Repeat Yourself) principle and make it easier to debug and add new functionalities.
- **Preprocessing, cleaning, and normalizing**: The project may require preprocessing of data, such as filling zeros, null/nan values or empty rows, standardizing values, parsing datetime, casting columns as integers or strings, renaming columns, and deleting duplicated rows or columns. These tasks can be performed to ensure data quality.
- **Keep our product safe**: A pipeline with solid logic and flow control can be developed to ensure that the product is safe to use in production. For example, the product could evaluate file’s modification date or check if preprocessing is OK before deploying to avoid any possible further issues or bugs in production.
- **Multiprocessing**: It may be possible to implement multiprocessing and try to upload multiple files at the same time. However, caution should be exercised as most of the available libraries execute instructions on the iterables in an unordered way. Care should be taken before upgrading to production this functionality.

Here are the first few lines of code for implementing multiprocessing:

(Note: Note that to implement multiprocessing, you may need to make some changes to [connector.py](packages\connector.py).)
<br>
```
import multiprocessing
from connector import main_loader

if __name__ == '__main__':
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(main_loader, csvs_files_paths)
```
<br>
Keep in mind that you may need to adapt the code to fit your specific use case and ensure that it works as intended.
<br>
<br>


Author: [@cremerf](https://github.com/cremerf)











