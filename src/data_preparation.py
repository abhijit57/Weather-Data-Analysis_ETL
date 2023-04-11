# Import in-built libraries
import os, sys
# Import data manipulation libraries
import pandas as pd
import numpy as np

# Import datetime and logging, CustomException from utils module
from datetime import datetime
from src.utils import logging, CustomException


# Data Preparation class
class PrepareData:
    """
    A class that prepares weather and crop data for ingestion into a Postgres table.

    ...

    Parameterized Constructor
    ----------
    cwd (str): The current working directory containing folders of weather data and crop yield data. 
               Each folder would then be containing respective data files in txt or csv formats .
    weather_data_path (str): folder name containing the weather data txt or csv files.
    crop_data_path (str): The path to the directory containing the crop data.

    Methods
    -------
    prepare_weather_data() -> Prepares weather data dataframe ready for ingestion into a Postgres table.
    prepare_crop_data() -> Prepares crop data dataframe ready for ingestion into a Postgres table.
    """
    def __init__(self, cwd):
        """
        Initializes the PrepareData class.

        Parameters
        ----------
        cwd (str): The current working directory.
        """
        try:
            self.cwd = cwd
            self.weather_data_path = os.path.join(self.cwd, 'wx_data')
            self.crop_data_path = os.path.join(self.cwd, 'yld_data')
            logging.info("weather data and crop data path variables created")
        except Exception as e:
            raise CustomException(e, sys)

    def prepare_weather_data(self):
        """
        Prepares weather data for ingestion into a Postgres table.

        Returns
        -------
        pandas.DataFrame ->  A pandas DataFrame containing the weather data.
        """
        try:
            filelists = []  # list to capture each dataframe obtained from each text or csv file present in the weather data folder
            for f in os.listdir(self.weather_data_path):
                # Read the text file as a dataframe
                tempdf = pd.read_csv(os.path.join(self.weather_data_path, f), sep = '\t', names = ['date', 'max_temp', 'min_temp', 'precipitation_amt'])
                # Create a station_id variable from the file name excluding everything '.' onwards
                station_id = f[:f.index('.')]
                tempdf['max_temp'] = tempdf['max_temp']/10   # recording maximum temperature in celsius
                tempdf['min_temp'] = tempdf['min_temp']/10   # recording minimum temperature in celsius
                tempdf['precipitation_amt'] = tempdf['precipitation_amt']/100  # recording precipitation amount in centimeters
                # Appending the station id into a column in the temp dataframe
                tempdf['station_id'] = station_id
                # Convert the date read from the text file into pandas datetime format
                tempdf['date'] = pd.to_datetime(tempdf.date, format = '%Y%m%d').dt.date
                # Create a primary key column by concatenating station id and date for the postgre sql table
                tempdf['wid'] = tempdf['station_id'] + '_' + tempdf.date.astype(str)
                # Append the temp dataframe into filelists list
                filelists.append(tempdf)
            logging.info('weather dataframe created and ready for ingestion into Postgres Table')
            # Return the complete extracted weather dataframe
            return pd.concat(filelists, axis=0, ignore_index=True)
        except Exception as e:
            raise CustomException(e, sys)

    def prepare_crop_data(self):
        """
        Prepares crop yield data for ingestion into a Postgres table.

        Returns
        -------
        pandas.DataFrame ->  A pandas DataFrame containing the crop yield data.
        """
        try:
            for f in os.listdir(self.crop_data_path):
                # Read the text file as a dataframe
                df = pd.read_csv(os.path.join(self.crop_data_path, f), sep= '\t', names= ['year', 'crop_grain_yield'])
            logging.info('crop dataframe created and ready for ingestion into Postgres Table')
            
            return df
        except Exception as e:
            raise CustomException(e, sys)
        