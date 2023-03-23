#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Weather Data Scrapper 

By: Daniel Paulo Tipan
"""

from datetime import datetime, timedelta
import requests
import config

import psycopg2


base_url = 'http://api.weatherapi.com/v1/current.json?'
api_key = config.api_key

location = 'Manila'


def weather_get():
    response = requests.get(f'{base_url}key={api_key}&q={location}')
    print(response)
    return response.json()

def clean_data():
    call_request = weather_get()

    weather_dict = { 
        'last_data_update': call_request['current']['last_updated'],
        'region' : call_request['location']['region'],
        'country' : call_request['location']['country'],
        'latitude' : call_request['location']['lat'],
        'longitude' : call_request['location']['lon'],
        'temp_c' : call_request['current']['temp_c'],
        'humidity' : call_request['current']['humidity'],
        'wind_kph' : call_request['current']['wind_kph'],
        'pressure_mb' : call_request['current']['pressure_mb'],
        'uv_val' : call_request['current']['uv'],
        'condition' : call_request['current']['condition']['text'],
        'icon' : call_request['current']['condition']['icon']
    }

    return weather_dict

def insert_data(weather_dictionary): 
    date_now = (datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    print(date_now)

    connection = psycopg2.connect(user=config.puser,
                                  password=config.password,
                                  host=config.host,
                                  port=config.port,
                                  database=config.database)
    cursor = connection.cursor()

    insert_query = f""" 
    INSERT INTO weather.weather_api (last_data_update,region,country,latitude,longitude,temp_c,humidity,win>
    VALUES ('{weather_dictionary['last_data_update']}',
            '{weather_dictionary['region']}',
            '{weather_dictionary['country']}',
            {weather_dictionary['latitude']},
            {weather_dictionary['longitude']},
            {weather_dictionary['temp_c']},
            {weather_dictionary['humidity']},
            {weather_dictionary['wind_kph']},
            {weather_dictionary['pressure_mb']},
            {weather_dictionary['uv_val']},
            '{weather_dictionary['condition']}',
            '{weather_dictionary['icon']}',
            '{date_now}'
        )
    """
    cursor.execute(insert_query)
    connection.commit()
    
    cursor.close()
    connection.close()

def main():
    weather_data = clean_data()
    print(weather_data)

    insert_data(weather_data)
    
    return weather_data

if __name__ == '__main__':
    main()
