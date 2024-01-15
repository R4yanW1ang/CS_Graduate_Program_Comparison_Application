"""
@File name: data_cleaning_merge
@Andrew IDS: yangyond, hhe3, mfouad, ziruiw2
@Purpose: merge scraped data from three sources.
"""

import data_collection as dc
import numpy as np
import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 1000)

def data_input():
    df_population = dc.collect_and_clean_pop()
    df_criminal = dc.collect_and_clean_safety()
    df_weather = dc.collect_and_clean_weather()
    df_program = dc.collect_and_clean_program()

    return df_population, df_criminal, df_weather, df_program

def data_preprocess(df_population, df_program, df_weather, df_criminal):
    ''' df_program preprocess '''
    # remove extra spaces
    df_program['City'] = df_program.apply(lambda x: x['City'].strip() if not pd.isna(x['City']) else x['City'], axis=1)


    ''' df_population preprocess'''
    # rename cities for joining
    index1 = df_population[df_population['City'] == 'New York city'].index.tolist()
    index2 = df_population[df_population['City'] == 'Chapel Hill town'].index.tolist()
    df_population.loc[index1, ['City']] = ['New York City']
    df_population.loc[index2, ['City']] = ['Chapel Hill City', 'Chapel Hill City']

    # filter out unnecessary types of city
    df_population = df_population[~df_population['City'].str.contains('borough|town|township|County|village|(pt.)')]
    df_population = df_population.reset_index().drop(columns='index')
    # remove the city profix for future merge
    df_population['City'] = df_population.apply(
        lambda x: ' '.join(x['City'].split(' ')[:-1])
        if len(x['City'].split(' ')) > 1 and x['City'] != 'New York City'
        else x['City'], axis=1)


    ''' df_weather preprocess'''
    # create a mapping of state initial and state full name
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

    # create a mapping to turn date into season
    season = {1: 'winter', 2: 'winter', 3: 'spring', 4: 'spring', 5: 'spring', 6: 'summer', 7: 'summer', 8: 'summer',
              9: 'fall', 10: 'fall', 11: 'fall', 12: 'winter'}

    # replace state initial with full name
    df_weather['State'] = df_weather['State'].replace(states)

    # add column season based on date
    df_weather['Season'] = df_weather.apply(lambda x: season[int(x['Date'].split('-')[1])], axis=1)

    # aggregate on season
    df_weather = df_weather.groupby(['State', 'Season']).agg(
        {'Temperature_avg': 'mean', 'Temperature_min': 'min', 'Temperature_max': 'max'}).reset_index()
    # set up the new columns after aggregation
    columns = ['State']
    for i in ['Temperature_avg', 'Temperature_min', 'Temperature_max']:
        for j in ['fall', 'spring', 'summer', 'winter']:
            columns.append(i + '_' + j)

    # pivot the table and rename columns
    df_weather = df_weather.pivot(index='State', columns='Season',
                                  values=['Temperature_avg', 'Temperature_min', 'Temperature_max']).reset_index()
    df_weather.columns = columns


    ''' df_criminal preprocess'''
    # data preprocessing and renaming
    df_criminal['institution_name'] = df_criminal['institution_name'].str.replace('-', ' – ')
    df_criminal.rename(columns={'institution_name': 'University'}, inplace=True)

    # sum the number of crimes of all campus for each university
    df_criminal = df_criminal.groupby('University').agg({'Murder/Non-negligent manslaughter': 'sum',
                                                         'Rape_cases': 'sum',
                                                         'Robbery_cases': 'sum',
                                                         'Aggravated_assault_cases': 'sum',
                                                         'Burglary_cases': 'sum',
                                                         'Motor_vehicle_theft_cases': 'sum'
                                                         }).reset_index()

    return df_program, df_population, df_weather, df_criminal


def merge(df_population, df_criminal, df_weather, df_program):
    '''merge df_program and df_population'''
    df_program, df_population, df_weather, df_criminal = data_preprocess(df_population, df_program, df_weather,
                                                                         df_criminal)

    df_program_population = pd.merge(df_program, df_population, on=['State', 'City'], how='left')

    # build a dictionary, key is state, value is the average population of every cities in that state
    # used to substitute null values if there is match in the population scraped data
    state_pop = df_population.groupby('State').agg(
        {'Population_estimate_2020': 'mean', 'Population_estimate_2021': 'mean',
         'Population_estimate_2022': 'mean'}).reset_index()

    state_pop_dict = {}
    for index, row in state_pop.iterrows():
        key = row['State']
        values = [round(row['Population_estimate_2020'], 0), round(row['Population_estimate_2021'], 0),
                  round(row['Population_estimate_2022'], 0)]
        state_pop_dict[key] = values

    # Fill in the null values
    for index in df_program_population.index:
        if pd.isna(df_program_population.loc[index, 'Population_estimate_2020']):
            state = df_program_population.loc[index, 'State']
            if str(state) == "nan":
                print(state)
            df_program_population.loc[index, 'Population_estimate_2020'] = state_pop_dict[state][0]
            df_program_population.loc[index, 'Population_estimate_2021'] = state_pop_dict[state][1]
            df_program_population.loc[index, 'Population_estimate_2022'] = state_pop_dict[state][2]


    # generate popualation category column using quantile
    df_program_population['population_category'] = df_program_population.apply(
                    lambda x: 'large' if x['Population_estimate_2022'] > df_program_population['Population_estimate_2022'].quantile(0.8)
                    else ('medium' if x['Population_estimate_2022'] > df_program_population['Population_estimate_2022'].quantile(0.3)
                    else 'small'), axis=1)


    '''merge weather data into the result table'''
    df_program_population_weather = pd.merge(df_program_population, df_weather, on='State', how='left')

    # Calculate the average temperature of
    df_program_population_weather['Temperature_avg'] = df_program_population_weather.apply(
                    lambda x: np.mean([x['Temperature_avg_fall'],
                                       x['Temperature_avg_summer'],
                                       x['Temperature_avg_winter'],
                                       x['Temperature_avg_spring']]),axis=1)

    # generate weather category column using quantile
    df_program_population_weather['temperature_category'] = df_program_population_weather.apply(
                    lambda x: 'hot' if x['Temperature_avg'] > df_program_population_weather['Temperature_avg'].quantile(
                        0.8)
                    else ('medium' if x['Temperature_avg'] > df_program_population_weather['Temperature_avg'].quantile(0.3)
                    else 'cold'), axis=1)


    '''merge criminal data into the result table'''
    df_result = pd.merge(df_program_population_weather, df_criminal, on='University', how='left')
    na_columns = ['Murder/Non-negligent manslaughter', 'Rape_cases', 'Robbery_cases', 'Aggravated_assault_cases', 'Burglary_cases', 'Motor_vehicle_theft_cases']
    df_result[na_columns] = df_result[na_columns].fillna(0)

    # Calculate the total criminal cases for each University
    df_result['Total_criminal_count'] = df_result['Murder/Non-negligent manslaughter'] + df_result['Rape_cases'] + \
                                        df_result['Robbery_cases'] + df_result['Aggravated_assault_cases'] + df_result['Burglary_cases'] +\
                                        df_result['Motor_vehicle_theft_cases']

    # generate criminal category column using quantile
    df_result['safety_category'] = df_result.apply(
                    lambda x: 'high' if x['Total_criminal_count'] > df_result['Total_criminal_count'].quantile(0.8)
                    else ('medium' if x['Total_criminal_count'] > df_result['Total_criminal_count'].quantile(0.3)
                          else 'low'), axis=1)

    if not os.path.exists(os.path.abspath("./merge")):
        os.makedirs(os.path.abspath("./merge"))

    # output the final dataset
    df_result.to_csv(os.path.join("./merge/", "merged.csv"), index=False, encoding='utf-8-sig')

    return df_result

if __name__ == "__main__":
    df_population, df_criminal, df_weather, df_program = data_input()
    df_result = merge(df_population, df_criminal, df_weather, df_program)