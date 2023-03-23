#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AQI Data Scrapper 

By: Daniel Paulo Tipan
"""

from datetime import datetime, timedelta
import requests
import config

import psycopg2


base_url = 'http://api.weatherapi.com/v1/current.json?'
api_key = config.api_key

location = 'Manila'


def aq_get():
    response = requests.get(f'{base_url}key={api_key}&q={location}&aqi=yes')
    print(response)
    return response.json()

def clean_data():
    call_request = aq_get()

    aq_dict = { 
        'last_data_update': call_request['current']['last_updated'],
        'region' : call_request['location']['region'],
        'country' : call_request['location']['country'],
        'latitude' : call_request['location']['lat'],
        'longitude' : call_request['location']['lon'],
        'co' : call_request['current']['air_quality']['co'],
        'no2' : call_request['current']['air_quality']['no2'],
        'o3' : call_request['current']['air_quality']['o3'],
        'so2' : call_request['current']['air_quality']['so2'],
        'pm2_5' : call_request['current']['air_quality']['pm2_5'],
        'pm10' : call_request['current']['air_quality']['pm10'],
        'us-epa-index' : call_request['current']['air_quality']['us-epa-index']
    }
    return aq_dict

def insert_data(aq_dictionary): 
    date_now = (datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    print(date_now)

    connection = psycopg2.connect(user=config.puser,
                                  password=config.password,
                                  host=config.host,
                                  port=config.port,
                                  database=config.database)
    cursor = connection.cursor()


    insert_query = f""" 
    INSERT INTO weather.air_quality_api (read_date,region,country,latitude,longitude,co,no2,o3,so2,pm2_5,pm>
    VALUES ('{aq_dictionary['last_data_update']}',
            '{aq_dictionary['region']}',
            '{aq_dictionary['country']}',
            {aq_dictionary['latitude']},
            {aq_dictionary['longitude']},
            {aq_dictionary['co']},
            {aq_dictionary['no2']},
            {aq_dictionary['o3']},
            {aq_dictionary['so2']},
            {aq_dictionary['pm2_5']},
            {aq_dictionary['pm10']},
            {aq_dictionary['us-epa-index']},
            '{date_now}'
        )
    """

    cursor.execute(insert_query)
    connection.commit()
    
    cursor.close()
    connection.close()

def main():
    aq_data = clean_data()
    print(aq_data)

    insert_data(aq_data)
    
    return aq_data

if __name__ == '__main__':
    main()
