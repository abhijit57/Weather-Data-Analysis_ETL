import os, sys
import pandas as pd
import numpy as np
import psycopg2

from data_preparation import PrepareData

from datetime import datetime
from utils import db_params
from utils import logging
from utils import CustomException

# Instantiate PrepareData class
prepare_data = PrepareData() 

class DBOperations:

    def __init__(self, **db_params):
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
            raise CustomException(e, sys)


    def create_weather_data_table(self):
        
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
            self.conn.close()
            logging.info("Table created: weather_data")
        except Exception as e:
            raise CustomException(e, sys)


    def create_crop_yield_table(self):
        
        try:
            create_table = '''
                            CREATE TABLE IF NOT EXISTS crop_yield_data(
                            year NUMERIC NOT NULL,
                            crop_grain_yield NUMERIC NOT NULL);
                        '''
            self.cursor.execute(create_table)
            self.conn.commit()
            self.conn.close()
            logging.info('Table created: crop_yield_data')
        except Exception as e:
            raise CustomException(e, sys)
        

    def create_weather_data_transformed_table(self):
        
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
            self.conn.close()
            logging.info("Table created: weather_data_transformed")
        except Exception as e:
            raise CustomException(e, sys)


    def insert_data_into_db(self, table_name):
        
        try:
            count = 0
            start_time = datetime.now()
            
            if table_name == 'weather_data':
                df = prepare_data.prepare_weather_data()
                df.date = df.date.astype(str)
            elif table_name == 'crop_yield_data':
                df = prepare_data.prepare_crop_data()
            elif table_name == 'weather_data_transformed':
                try:
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


            # Convert columns to the best possible dtypes using dtypes supporting pd.NA
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
            self.cursor.close()
            end_time = datetime.now()
            logging.info(f"Ingestion process started at {start_time} and finished at {end_time}, and a total number of {count} records were ingested.")
        except Exception as e:
            raise CustomException(e, sys)
        
    