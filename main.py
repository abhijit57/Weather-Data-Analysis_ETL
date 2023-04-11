# Import necessary libraries
import os, sys, json
import pandas as pd
import numpy as np
import psycopg2

# Import methods and flask related libraries
from flask import Flask, request, jsonify
from flask_swagger import swagger
from flask_restful import Resource, Api
from flask_swagger_ui import get_swaggerui_blueprint

from src.utils import db_params, logging, CustomException
from datetime import datetime


# Constants for page size and number
PAGE_SIZE = 10
PAGE_NUMBER = 1

# Creating a postgres sql database connection string
conn = psycopg2.connect(
                host = db_params['hostname'], 
                port= db_params['port'], 
                database= db_params['dbname'], 
                user= db_params['username'], 
                password = db_params['password']
            )

# Sets a flask application named app
app = Flask(__name__)
# Sets an instance of the Flask-RESTful API named api to allow the Flask app to handle RESTful API requests.
api = Api(app)


# Configure Swagger UI documentation for a Flask RESTful
SWAGGER_URL = '/swagger'
API_URL = 'http://127.0.0.1:5000/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Weather_Crop_Data_Analysis_and_ETL API"
    }
)
# Register the Swagger UI Blueprint with the Flask application
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


@app.route('/swagger.json')
def swagger():
    """
    Endpoint to return the API specification in JSON format for use by the Swagger UI.

    Returns:
        JSON response: The API specification in JSON format.
    """
    with open('swagger.json', 'r') as f:
        return jsonify(json.load(f))
    


def get_weather_data(start_date, end_date , station_id, page_number, page_size):
    """
    Retrieves weather data from weather_data table based on user query parameters such as start date, end date and station id.

    Args:
        start_date (str): The start date of the period for which to retrieve weather data.
        end_date (str): The end date of the period for which to retrieve weather data.
        station_id (str): The ID of the weather station from which to retrieve data.
        page_number (int): The page number of the results to retrieve.
        page_size (int): The number of results to retrieve per page.

    Returns:
        str: The weather data in JSON format.
    """
    cur = conn.cursor()
    # build SQL query based on query parameters
    query = "SELECT * FROM weather_data WHERE 1=1"
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
    if station_id:
        query += f" AND station_id = '{station_id}'"
    query += f" LIMIT {page_size} OFFSET {page_number}"    # LIMIT-> page size and OFFSET -> page number


    # execute SQL query and fetch results
    cur.execute(query)
    results = cur.fetchall()
    # Store the results obtained from the weather_data table based on the user query into a pandas dataframe
    df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    # Convert the dataframe into json format
    res = df.to_json(orient='records')

    return res



# first api endpoint with GET method /api/weather
@app.route("/api/weather", methods=["GET"])
def get_weather_data_api_handle():
    """
    API endpoint for retrieving weather data using RESTful API handle.

    Returns:
        str: The weather data in JSON format.
    """
    pageSize = PAGE_SIZE
    pageNumber = PAGE_NUMBER
    # getting input query-params from API request
    args = request.args
    args = args.to_dict()

    # Checking if all required query params are passed 
    if 'start_date' not in args or 'station_id' not in args or 'end_date' not in args:
        response = {
            'success': 'ok',
            'message': 'Incomplete query params'
        }
        return  json.dumps(response, indent = 4)
    
    if 'page_size' in args:
        pageSize = args.get('page_size')

    if 'page_number' in args:
        pageNumber = args.get('page_number')

    start_date = args.get('start_date')
    end_date = args.get('end_date')
    station_id = args.get('station_id')

    # Fetching the results from weather_data table by passing the input query params from this function
    res = get_weather_data(start_date, end_date, station_id, pageNumber, pageSize)
    
    # if no records found for the input query - params
    if not res or not len(res):
        response = {
            'success': 'ok',
            'message': 'No records for this query'
        }
        return  json.dumps(response, indent = 4)
    
    return res

    
# endpoint for retrieving statistical computations on weather data
@app.route("/api/weather/stats", methods=["GET"])
def get_weather_data_stats():

    pageSize = PAGE_SIZE
    pageNumber = PAGE_NUMBER
    # getting input query - params from API request
    args = request.args
    args = args.to_dict()

    # Checking if all required query params are passed
    if 'start_date' not in args or 'station_id' not in args or 'end_date' not in args:
        response = {
            'success': 'ok',
            'message': 'Incomplete query params'
        }
        return  json.dumps(response, indent = 4)

    start_date = args.get('start_date')
    end_date = args.get('end_date')
    station_id = args.get('station_id')

    # If pagination params are provided in API request
    if 'page_size' in args:
        pageSize = args.get('page_size')

    if 'page_number' in args:
        pageNumber = args.get('page_number')
    
    # Fetch the weather data records based on query-params
    res = get_weather_data(start_date, end_date, station_id, pageNumber, pageSize)

    # If no response found
    if not res or not len(res):
        response = {
            'success': 'ok',
            'message': 'No records for this query'
        }
        return  json.dumps(response, indent = 4)
    
    # Read the JSON response into a pandas dataframe to compute the descriptive statistics on certain features of the weather data
    df = pd.read_json(res)
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = [x.year for x in df['date']]
    stats = pd.DataFrame(df.groupby(['year','station_id']).agg({'max_temp':'mean', 'min_temp':'mean','precipitation_amt':'sum'}).reset_index())
    stats = stats.to_dict()

    return jsonify(stats)



if __name__ == "__main__":
    app.run(host="127.0.0.1",debug=True, port=5000)




