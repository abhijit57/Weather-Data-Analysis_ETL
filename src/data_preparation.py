import os, sys
import pandas as pd
import numpy as np

from datetime import datetime
from src.utils import logging, CustomException


class PrepareData:

    def __init__(self, cwd):
        try:
            self.cwd = cwd
            self.weather_data_path = os.path.join(self.cwd, 'wx_data')
            self.crop_data_path = os.path.join(self.cwd, 'yld_data')
            logging.info("weather data and crop data path variables created")
        except Exception as e:
            raise CustomException(e, sys)

    def prepare_weather_data(self):
        
        try:
            filelists = []
            for f in os.listdir(self.weather_data_path):
                tempdf = pd.read_csv(os.path.join(self.weather_data_path, f), sep = '\t', names = ['date', 'max_temp', 'min_temp', 'precipitation_amt'])
                station_id = f[:f.index('.')]
                tempdf['max_temp'] = tempdf['max_temp']/10   # recording maximum temperature in celsius
                tempdf['min_temp'] = tempdf['min_temp']/10   # recording minimum temperature in celsius
                tempdf['precipitation_amt'] = tempdf['precipitation_amt']/100  # recording precipitation amount in centimeters
                tempdf['station_id'] = station_id
                tempdf['date'] = pd.to_datetime(tempdf.date, format = '%Y%m%d').dt.date
                tempdf['wid'] = tempdf['station_id'] + '_' + tempdf.date.astype(str)
                filelists.append(tempdf)
            logging.info('weather dataframe created and ready for ingestion into Postgres Table')
            
            return pd.concat(filelists, axis=0, ignore_index=True)
        except Exception as e:
            raise CustomException(e, sys)

    def prepare_crop_data(self):
        
        try:
            for f in os.listdir(self.crop_data_path):
                df = pd.read_csv(os.path.join(self.crop_data_path, f), sep= '\t', names= ['year', 'crop_grain_yield'])
            logging.info('crop dataframe created and ready for ingestion into Postgres Table')
            
            return df
        except Exception as e:
            raise CustomException(e, sys)
        