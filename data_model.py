
# Import in-built libraries
import os, sys  
# Import argument parser library
import argparse 
# Import python library for postgres sql
import psycopg2
# Import data manipulation libraries
import pandas as pd
import numpy as np

# Import python classes from python modules present in src directory
from src.database_operations import DBOperations
from src.data_preparation import PrepareData
from src.utils import db_params, logging, CustomException


# Instantiate DBOperations class
db_operations = DBOperations(db_params)  



if __name__ == "__main__":
    '''
    This block accepts arguments from the user to execute the DBOperations methods and PrepareData methods.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--create_weather_data_tbl', type=str, required=False, help='Create weather_data table under the created database in Postgres SQL')
    parser.add_argument('--create_crop_yield_tbl', type=str, required=False, help='Create crop_yield_data table under the created database in Postgres SQL')
    parser.add_argument('--create_weather_data_transformed_tbl', type=str, required=False, help='Create weather_data_transformed table under created database in Postgres SQL')

    parser.add_argument('--insert_data_tbl', type=str, help='Inserts data into Postgres SQL tables')
    parser.add_argument('--dir', type=str, help='provide the folder where the data files are stored')
    parser.add_argument('--src_tbl_name', type=str, help='pass the raw data table name on which fetch and transformation operations are to be performed')
    parser.add_argument('--tbl_name', type=str, help='pass the table name')

    args = parser.parse_args() # Create an object to accept input parameters to command line scripts


    # Create weather data table
    if args.create_weather_data_tbl:
        db_operations.create_weather_data_table()

    # Create crop yield data table
    if args.create_crop_yield_tbl:
        db_operations.create_crop_yield_table()
    
    # Create weather data transformed table
    if args.create_weather_data_transformed_tbl:
        db_operations.create_weather_data_transformed_table()


    # Insert data into weather_data and crop_yield_data tables
    if args.insert_data_tbl:
        if args.dir:
            prep_data = PrepareData(args.dir)
            db_operations.insert_data_into_table(prep_data, args.tbl_name)

    # Insert data into weather_data_transformed table by checking if weather_data table is populated with data or not
    if args.insert_data_tbl and args.tbl_name == 'weather_data_transformed':
        try:
            if db_operations.fetch_data_table(args.src_tbl_name):
                db_operations.insert_data_into_table(None, args.tbl_name)
        except Exception as e:
            raise CustomException(e, sys)



    

    

        


    
