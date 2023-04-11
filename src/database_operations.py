# Import in-built libraries
import os, sys
# Import data manipulation libraries
import pandas as pd
import numpy as np
# Import python library for postgres sql
import psycopg2

# Import classes and methods from data_preparation and utils module
from src.data_preparation import PrepareData
from src.utils import db_params, logging, CustomException
# Import datetime
from datetime import datetime



class DBOperations:
    """
    A class for performing various database operations.

    Parameterized Constructor:
    ----------
    db_params (dict): A dictionary containing database connection parameters like hostname, port, database name, username, and password.
    conn (psycopg2.extensions.connection): A connection object to PostgreSQL database.
    cursor (psycopg2.extensions.cursor): A cursor object to execute database operations.

    """
    def __init__(self, db_params):
        """
        Initializes a connection and cursor object to the PostgreSQL database.

        Parameters:
        ----------
        db_params (dict): A dictionary containing database connection parameters like hostname, port, database name, username, and password.
            
        Raises:
        -------
        CustomException (exception): Raised when there is an error connecting to the database.

        """
        try:
            self.conn = psycopg2.connect(
                host = db_params['hostname'], 
                port= db_params['port'], 
                database= db_params['dbname'], 
                user= db_params['username'], 
                password = db_params['password']
            )
            self.cursor = self.conn.cursor()
            logging.info('Postgres Database connection and cursor objects initialized')
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)
        

    def create_weather_data_table(self):
        """
        Creates a table named "weather_data" in the PostgreSQL database to store weather data.

        Raises:
        -------
        CustomException (exception): Raised when there is an error creating the table.
            
        """
        try:
            create_table = '''
                            CREATE TABLE IF NOT EXISTS weather_data(
                            date DATE NOT NULL,
                            max_temp INTEGER NOT NULL,
                            min_temp NUMERIC NOT NULL,
                            precipitation_amt NUMERIC NULL,
                            station_id TEXT NULL,
                            wid TEXT PRIMARY KEY NOT NULL);
                            '''
            self.cursor.execute(create_table)
            self.conn.commit()
            logging.info("Table created: weather_data")
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)


    def create_crop_yield_table(self):
        """
        Creates a table named "crop_yield_data" in the PostgreSQL database to store crop yield data.

        Raises:
        -------
        CustomException (exception): Raised when there is an error creating the table.
            
        """
        try:
            create_table = '''
                            CREATE TABLE IF NOT EXISTS crop_yield_data(
                            year NUMERIC NOT NULL,
                            crop_grain_yield NUMERIC NOT NULL);
                        '''
            self.cursor.execute(create_table)
            self.conn.commit()
            logging.info('Table created: crop_yield_data')
            print(self.conn)
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)
        

    def create_weather_data_transformed_table(self):
        """
        Creates a table named "weather_data_transformed" in the PostgreSQL database to store crop yield data.

        Raises:
        -------
        CustomException (exception): Raised when there is an error creating the table.
            
        """
        try:
            create_table = '''
                            CREATE TABLE IF NOT EXISTS weather_data_transformed(
                            years NUMERIC NOT NULL,
                            station_id TEXT NULL,
                            avg_min_temp NUMERIC NOT NULL,
                            avg_max_temp NUMERIC NOT NULL,
                            total_precipitation_amt NUMERIC NULL);
                            '''
            self.cursor.execute(create_table)
            self.conn.commit()
            logging.info("Table created: weather_data_transformed")
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)
        
    
    def fetch_data_table(self, table_name):
        """
        Fetches all records from the specified table and returns the results.

        Args :
            table_name (str): The name of the table to fetch data from.

        Returns:
            results (list): A list of tuples containing the fetched records.
        
        Raises:
            CustomException: If an error occurs while fetching data from the table.
        """
        try:
            fetch_query = f'''
                        SELECT * FROM {table_name};
                        '''
            self.cursor.execute(fetch_query)
            results = self.cursor.fetchall()
            logging.info(f"Fetch finished: {len(results)} records from {table_name}")

            return results
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)


    def insert_data_into_table(self, class_instance, table_name):
        """
        Inserts data into the specified table from a pandas dataframe and logs the results.

        Args:
            class_instance (object): An instance of a class containing a method for preparing data to prepare dataframe which would be then inserted into the table.
                                    Object of PrepareData class from data_preparation module containing the constructor parameter "cwd"
                                    Here cwd -> The current working directory containing folders of weather data and crop yield data.
                                    This would then be used to preare weather and crop dataframe.
                                    For example: prep_data = PrepareData()  {contains the current folder path which contains all the required data}
                                    prep_data.prepare_weather_data()  {that path is passed to this method which then uses this path to read data and prepare the dataframe ultimately}
            table_name (str): The name of the table to insert data into.

        Returns:
            None

        Raises:
            CustomException: If an error occurs while inserting data into the table.
        """
        try:
            # Counter variable to keep track of the number of records ingested.
            count = 0
            # start_time variable to log the ingestion process start time
            start_time = datetime.now()
            
            if table_name == 'weather_data':
                df = class_instance.prepare_weather_data()  # store the weather dataframe in df
                df.date = df.date.astype(str)               # convert the date column into string data type
            elif table_name == 'crop_yield_data':
                df = class_instance.prepare_crop_data()     # store the crop dataframe in df
            elif table_name == 'weather_data_transformed':
                try:
                    # Perform statistical transformations on raw weather data and ingest the transformed data into weather_data_transformed table
                    start_time = datetime.now()
                    sql_query = f'''
                        SELECT EXTRACT(YEAR FROM date) as years, station_id, AVG(min_temp) as avg_min_temp, AVG(max_temp) as avg_max_temp, SUM(precipitation_amt) as total_precipitation_amt 
                        FROM weather_data
                        WHERE min_temp <> -999.9 OR max_temp <> -999.9 OR precipitation_amt <> -99.99
                        GROUP BY years, station_id;
                        '''
                    df = pd.read_sql(sql_query, con=self.conn)
                    end_time = datetime.now()
                    logging.info(f"Data Transformation process started at {start_time} and finished at {end_time}, and a total number of {len(df)} \
                                records were transformed.")
                except Exception as e:
                    raise CustomException(e, sys)

            # Convert dtypes of columns to native python dtypes from numpy.dtypes
            df = df.convert_dtypes()
            # Define column names for the tables
            column_names = df.columns.tolist()

            # Loop through rows of Pandas DataFrame and insert into PostgreSQL table
            for index, row in df.iterrows():
                # Create a tuple of values to insert
                values = tuple(row.values)

                # Check if the row already exists in the database
                sql_query = "SELECT EXISTS (SELECT 1 FROM {} WHERE {})".format(table_name, " AND ".join([f"{column}=%s" for column in column_names]))
                self.cursor.execute(sql_query, values)
                result = self.cursor.fetchone()

                # If the row doesn't already exist, insert it
                if not result[0]:
                    sql_query = "INSERT INTO {} ({}) VALUES {}".format(table_name, ", ".join(column_names), values)
                    self.cursor.execute(sql_query)
                    count += 1

            # Commit changes to the database
            self.conn.commit()
            end_time = datetime.now()
            logging.info(f"Ingestion process started at {start_time} and finished at {end_time}, and a total number of {count} records were ingested.")
        except Exception as e:
            self.conn.close()
            raise CustomException(e, sys)
        
    