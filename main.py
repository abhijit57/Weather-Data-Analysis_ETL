import os, sys
import pandas as pd
import numpy as np

from flask import Flask, request, jsonify, session
import json
import psycopg2

from src.utils import db_params, logging, CustomException

conn = psycopg2.connect(
                host = db_params['hostname'], 
                port= db_params['port'], 
                database= db_params['dbname'], 
                user= db_params['username'], 
                password = db_params['password']
            )


app = Flask(__name__)
app.secret_key = 'your_secret_key'

# endpoint for retrieving weather data
@app.route("/api/weather", methods=["POST"])
def get_weather_data():
    cur = conn.cursor()

    start_date = request.form['start_date']
    end_date = request.form['end_date']
    station_id = request.form['station_id']

    # build SQL query based on query parameters
    query = "SELECT * FROM weather_data WHERE 1=1"
    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"
    if station_id:
        query += f" AND station_id = '{station_id}'"


### LIMIT 20 (page size)
### OFFSET 20;  (page number), rest order by and sorting


    # execute SQL query and fetch results
    cur.execute(query)
    results = cur.fetchall()
    df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])

    res = df.to_json(orient='records')
    # if you want both apis on same front end when the user query the database based on date and id then he wants to see the stats of same data he extracted
    session['items'] = res
    return res
    

@app.route("/api/weather/stats", methods=["POST"])
def get_weather_data_stats():
    # df = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
    # we can do below to fetch the queried data or we can call the function here
    # df = get_weather_data()
    df = pd.read_json(session['items'])
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = [x.year for x in df['date']]
    stats = pd.DataFrame(df.groupby(['year','station_id']).agg({'max_temp':'mean', 'min_temp':'mean','precipitation_amt':'sum'}).reset_index())

    stats = stats.to_dict()

    return jsonify(stats)




if __name__ == "__main__":
    app.run(host="127.0.0.1",debug=True, port=5000)





    # def main(url):

#     #Extract_Data
#     logging.info("Extracting data from - {}".format(url))
#     html_data = get_data(url)
#     logging.info("Parsing data")
#     data = parse_html(html_string = html_data)
#     logging.info("Saving raw data")
#     get_data_save(ur_data=data)

#     #Transform_Data
#     final_directory = os.path.join(os.getcwd(), r'raw_files')
#     csv_data = read_data(file_path = final_directory)
#     logging.info("Transfomation 1 - TXT to CSV")
#     transformed_data = transform_dataset(csv_data_df = csv_data)
#     logging.info("Aggregating CSV data")
#     aggregated_data = aggregated_dataset(transform_df = transformed_data)
#     logging.info("Sink tranformed data")
#     sink_dataset(df = transformed_data,file_name='transformed_data',foldername=r'enriched_files')
#     logging.info("Sink aggregated data")
#     sink_dataset(df = aggregated_data,file_name='aggregated_data',foldername=r'enriched_files')

#     #Write data
#     logging.info("Creating aggregate table")
#     db.create_aggregate_table()
#     logging.info("Creating transformed table")
#     db.create_transformed_table()
#     logging.info("Saving aggregated data to the table")
#     db.write_data(sub_1='enriched_files', sub_2= 'aggregated_data')
#     logging.info("Saving transformed_data data to the table")
#     db.write_data(sub_1='enriched_files', sub_2= 'transformed_data')
