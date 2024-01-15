#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  8 11:32:12 2023
@File name: create_scatterplot
@Andrew IDS: yangyond, hhe3, mfouad, ziruiw2
@Purpose: Prepare Scatterplot for weather data
"""
import os

import pandas as pd
import matplotlib.pyplot as plt


# Import datasets
def read_in_data_files():
    df_weather = pd.read_csv('weather/temperature_county_2022.csv')
    df_merged = pd.read_csv('merge/merged.csv')
    
    return df_weather, df_merged


# Clean data (desired data is at the monthly and state level)
def prep_weather_data_for_scatterplot():
    df_weather, df_merged = read_in_data_files()

    # create month variable
    df_weather['Month'] = pd.DatetimeIndex(df_weather['Date']).month
    df_weather = df_weather.groupby(['State', 'Month']).agg(
        {'Temperature_avg': 'mean', 'Temperature_min': 'min', 'Temperature_max': 'max'}).reset_index()
    
    # change States from Abbreviations to actualy state names
    states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
    }
    
    # replace state abbreviations with full name
    df_weather['State'] = df_weather['State'].replace(states)
    
    # merge weather data with final merged data to get university info + monthly temperatures
    weather_processed_df = pd.merge(df_weather, df_merged[["State", "University"]], on ='State',
                              how = 'inner')[['State', 'Month', 'Temperature_avg',
                                             'Temperature_min', 'Temperature_max', 'University']]

    # output the final dataset
    weather_processed_df.to_csv(os.path.join("merge", "weather_merged.csv"), index=False, encoding='utf-8-sig')
    return weather_processed_df


if __name__ == "__main__":
    filtered_university = 'Cornell University'
    weather_processed_df = prep_weather_data_for_scatterplot()

    # Filter the data to the university of choice
    weather_processed_df = weather_processed_df[weather_processed_df['University'] == filtered_university]
    plt.scatter(x = weather_processed_df['Month'], y = weather_processed_df['Temperature_avg'])
    
    title = "Average Temperature in " + filtered_university
    plt.title(title) 
    plt.xlabel("Month")
    plt.ylabel("Avg Temperature (F)")
    plt.show()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    