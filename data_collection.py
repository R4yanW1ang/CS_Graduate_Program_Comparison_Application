"""
@File name: data_collection
@Andrew IDS: yangyond, hhe3, mfouad, ziruiw2
@Purpose: scrape data from three sources and clean them.
"""
import time

# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.request import urlopen
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings("ignore")


def collect_and_clean_pop():
    """
    Collect and clean population size of the city or town of 51 U.S.states
    for 2020-2022 from the Census Bureau data.
    :return:
    """
    if not os.path.exists(os.path.abspath("./population")):
        os.makedirs(os.path.abspath("./population"))

    link = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/cities/totals/sub-est2022.csv"
    df_pop = pd.read_csv(link)

    # Filter the whole population data of the state
    df_pop.query("COUNTY !=0 or PLACE !=0", inplace=True)

    # Rename the columns
    columns = ["STNAME", "NAME", "POPESTIMATE2020", "POPESTIMATE2021", "POPESTIMATE2022"]
    name_map = {
        "STNAME": "State",
        "NAME": "City",
        "POPESTIMATE2020": "Population_estimate_2020",
        "POPESTIMATE2021": "Population_estimate_2021",
        "POPESTIMATE2022": "Population_estimate_2022"
    }
    df_pop = df_pop[columns].rename(columns=name_map)

    # Remove Null and duplicate values
    df_pop.dropna(inplace=True)
    df_pop.drop_duplicates(subset=["State", "City"], inplace=True)
    print(df_pop.head())
    print("Population dataset size of all U.S. states: {}".format(df_pop.shape[0]))
    df_pop.to_csv(os.path.join("./population", "population_county_2022.csv"), index=False)

    return df_pop


def collect_and_clean_safety():
    """
    Collect and clean campus safety information from the U.S. Department of Educatio
    , for all universities in 2021.
    :return:
    """
    # Set the download path of csv file
    chrome_options = webdriver.ChromeOptions()

    if not os.path.exists(os.path.abspath("./download")):
        os.makedirs(os.path.abspath("./download"))

    if not os.path.exists(os.path.abspath("./safety")):
        os.makedirs(os.path.abspath("./safety"))

    prefs = {
        "download.default_directory": os.path.abspath("./download"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Open Chrome
    driver = webdriver.Chrome(options=chrome_options)

    # Open the target website
    driver.get("https://ope.ed.gov/campussafety/#/customdata/datafiltered")
    wait = WebDriverWait(driver, 10)

    # Choose year in checkbox
    year_checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="input2021"]')))
    year_checkbox.click()

    # Choose criminal category in checkbox
    crime_checkbox = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="content"]/div/div/div[2]/ul/li[1]/ul/li[2]/label/input')))
    driver.execute_script("arguments[0].scrollIntoView();", crime_checkbox)
    crime_checkbox.click()

    # Click the download button
    download_button = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/div/div['
                                                                       '3]/button[3]')))
    download_button.click()
    driver.implicitly_wait(20)
    time.sleep(15)

    # Close the website
    driver.quit()

    # Read and clean data
    df_safety = pd.DataFrame()
    for file in os.listdir("./download"):
        if not file.endswith("csv"): continue
        df = pd.read_csv(os.path.join("./download", file))
        df_safety = pd.concat([df_safety, df])

    # Remove unexpected left or right space in column name
    df_safety.columns = [col.strip() for col in df_safety.columns.tolist()]
    print(df_safety.columns.tolist())

    columns = ["Survey year", "Institution name", "Campus Name", "Murder/Non-negligent manslaughter", "Rape", "Robbery", "Aggravated assault", "Burglary", "Motor vehicle theft"]
    df_safety = df_safety[columns]
    df_safety.rename(columns={"Survey year": "year",
                              "Institution name": "institution_name",
                              "Campus Name": "campus_name",
                              "Murder/Non-negligent": "Murder_cases",
                              "Rape": "Rape_cases",
                              "Robbery": "Robbery_cases",
                              "Burglary": "Burglary_cases",
                              "Aggravated assault": "Aggravated_assault_cases",
                              "Motor vehicle theft": "Motor_vehicle_theft_cases"},
                     inplace=True)

    # Remove duplicate data
    df_safety.drop_duplicates(subset=["campus_name"], inplace=True)
    print(df_safety.head())
    df_safety.to_csv(os.path.join("./safety", "crinimal_offenses_on_campus_2021.csv"), index=False)

    return df_safety


def collect_and_clean_weather():
    """
    Collect and clean weather data from the National Centers for Environmental Information(NCEI)
    , for all counties in 2022.
    :return:
    """
    if not os.path.exists(os.path.abspath("./weather")):
        os.makedirs(os.path.abspath("./weather"))

    base_dir = "https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/2022/"
    months = [str(i) if len(str(i)) == 2 else "0"+str(i) for i in range(1, 13)]
    temperature_types = ["tavg", "tmax", "tmin"]
    temper_type_map = {"tavg": "Temperature_avg", "tmax": "Temperature_max", "tmin": "Temperature_min"}
    file_name_temp = "{}-2022{}-cty-scaled.csv"
    days = [str(i) if len(str(i)) == 2 else "0"+str(i) for i in range(1, 32)]

    # Scrape daily data of average, max, min temperature
    df_weather = pd.DataFrame()
    for month in months:
        df_merge_type = pd.DataFrame()
        for type_ in temperature_types:
            columns = ["Region_type", "County_code", "County", "Year", "Month", "Temper_type"]
            day_list = ["2022-{}-{}".format(month, day) for day in days]
            file_name = file_name_temp.format(type_, month)
            df_weather_month = pd.read_csv(os.path.join(base_dir, file_name))
            df_weather_month.columns = columns + day_list

            # Merge data together
            df_merge_month = pd.DataFrame()
            for day in day_list:
                df_day = df_weather_month[columns+[day]]
                df_day["Date"] = day
                df_day = df_day[["Date", "County", day]]
                # Remove nonexistent days such as February 30 are set to -999.99
                df_day.rename(columns={day: temper_type_map[type_]}, inplace=True)
                df_day.query("{} > -999.99".format(temper_type_map[type_]), inplace=True)

                # Split state and county
                states = []
                counties = []
                for value in df_day["County"].tolist():
                    state, county = value.strip().split(":")
                    states.append(state)
                    counties.append(county)
                df_day["County"] = counties
                df_day["State"] = states

                df_merge_month = pd.concat([df_merge_month, df_day])
                df_merge_month.drop_duplicates(subset=["Date", "State", "County"], inplace=True)

            if len(df_merge_type) == 0:
                df_merge_type = df_merge_month
            else:
                df_merge_type = pd.merge(df_merge_type, df_merge_month, on=["Date", "State", "County"])

        df_weather = pd.concat([df_weather, df_merge_type])

    print(df_weather.tail())
    print("Weather data size in 2022: {}".format(df_weather.size))
    (df_weather[["Date", "State", "County", "Temperature_avg", "Temperature_max", "Temperature_min"]]
     .to_csv("./weather/temperature_county_2022.csv", index=False))

    return df_weather


def collect_and_clean_program():
    """
    Collect and clean program data from computer science degree hub.
    , for the latest data.
    :return:
    """
    html = urlopen('https://www.computersciencedegreehub.com/masters-computer-science')

    bsyc = BeautifulSoup(html.read(), "lxml")

    fout = open('bsyc_temp.txt', 'wt', encoding='utf-8')
    fout.write(str(bsyc))
    fout.close()

    data_list = bsyc.findAll('h3')
    data = []
    unparsed = []
    for d in data_list:
        degree_name = d.text
        unwanted_texts = ['Rankings', 'Infographics', 'Site Info']
        if degree_name in unwanted_texts:
            continue  # Skip this iteration and move to the next item

        # Match the pattern: "Ranking Number. University NameDegree Name"
        match = re.match(r'(\d+)\.\s*([^\d]+)(Master.+)', degree_name)
        if match:
            # Extract and print ranking number, university name, and degree name
            ranking = match.group(1)
            university = match.group(2).strip()
            degree = match.group(3).strip()
            data.append({'University': university, 'Degree': degree, 'Ranking': ranking})

        else:
            # Handle texts that do not match the pattern (optional)
            unparsed.append(degree_name)

    df1 = pd.DataFrame(data)
    df2 = pd.DataFrame(unparsed)
    df3 = df2.iloc[[0, 2, 4]].copy()
    df4 = df2.iloc[[1, 3, 5]].copy()
    df3['Ranking'] = df3[0].str.extract(r'(\d+)')
    df3[0] = df3[0].str.replace(r'^\d+\.\s*', '', regex=True)
    df3.rename(columns={0: 'University'}, inplace=True)
    df3.reset_index(drop=True, inplace=True)
    df4.reset_index(drop=True, inplace=True)
    df3['Degree'] = df4[0]
    df5 = df3[['University', 'Degree', 'Ranking']]

    top_df = df1.iloc[:12].copy()
    bottom_df = df1.iloc[12:].copy()
    df = pd.concat([top_df, df5, bottom_df], ignore_index=True)

    p_elements = bsyc.find_all('p')

    uni_data = []
    for p in p_elements:
        text = p.get_text()  # Get text content of the <p> element
        if "Points:" in text:
            points = text.split("Points:")[1].split("2020")[0].strip()
            uni_data.append(points)  # Append points to uni_data

    uni_data = ['20' if x == '' else x for x in uni_data]
    uni_data = [int(x) for x in uni_data]

    # Convert the list to a DataFrame
    df6 = pd.DataFrame(uni_data, columns=['Points'])
    df['Points'] = df6

    tuition_data = []
    for p in p_elements:
        text = p.get_text()
        if "Tuition:" in text:
            # Extract and clean data
            tuition = text.split("Tuition:")[1].split()[0].strip()  # get the tuition value
            tuition = tuition.replace('$', '').replace(',', '')  # remove dollar sign and commas
            tuition = float(tuition)
            tuition_data.append(tuition)

    df7 = pd.DataFrame(tuition_data, columns=['Tuition'])

    out_of_state_tuition_data = []
    for p in p_elements:
        text = p.get_text()
        if "Tuition:" in text:
            # Check if out-of-state tuition is provided
            if '(out of state)' in text:
                out_of_state_tuition = text.split('(out of state)')[0].split('$')[
                    -1]  # get the part before 'out of state)' and after the last '$'
            else:
                out_of_state_tuition = text.split('Tuition:')[1].split()[
                    0].strip()  # if no out-of-state tuition provided, use the regular tuition

            # Remove commas only and keep the tuition as string
            out_of_state_tuition = out_of_state_tuition.replace(',', '').strip()
            out_of_state_tuition_data.append(out_of_state_tuition)

    # Convert list to DataFrame
    df_out_of_state = pd.DataFrame(out_of_state_tuition_data, columns=['Out_of_State_Tuition'])
    df_out_of_state['Out_of_State_Tuition'] = df_out_of_state['Out_of_State_Tuition'].apply(
        lambda x: x.replace('$', ''))

    df["In_State_Tuition"] = df7.astype(int)
    df["Out_of_State_Tuition"] = df_out_of_state

    # Scrape comments and location from website

    # We would like to get the information between two <h3> tags including
    # the description and the bullet points listing out coursework
    descriptions = []
    for heading in bsyc.select('h3'):
        vals = ''
        for ns in (heading.fetchNextSiblings()):
            if ns.name == "h3":
                break
            if ns.name == "p" and "Points" not in ns.text:
                vals += ns.text
            if ns.name == "ul":
                bullet_points = ns.text.strip().replace("\n", ",")
                vals = vals + " " + bullet_points
        descriptions.append(vals)
    descriptions = [element for element in descriptions if (len(element) > 0)]

    # To limit to only relvant comments, we ensure that the university name is mentioned
    # Get all university names in list
    university_names = df['University'].tolist()

    # To ensure that location was probably pulled using regular expression,
    # confirm state exits in list of states
    us_states = ['Alaska', 'Alabama', 'Arkansas', 'Arizona', 'California', 'Colorado',
                 'Connecticut', 'District of Columbia', 'Delaware', 'Florida', 'Georgia',
                 'Hawaii', 'Iowa', 'Idaho', 'Illinois', 'Indiana', 'Kansas', 'Kentucky',
                 'Louisiana', 'Massachusetts', 'Maryland', 'Maine', 'Michigan',
                 'Minnesota', 'Missouri', 'Mississippi', 'Montana', 'North Carolina',
                 'North Dakota', 'Nebraska', 'New Hampshire', 'New Jersey', 'New Mexico',
                 'Nevada', 'New York', 'Ohio', 'Oklahoma', 'Oregon',
                 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
                 'Tennessee', 'Texas', 'Utah', 'Virginia', 'Vermont', 'Washington',
                 'Wisconsin', 'West Virginia', 'Wyoming']

    relevant_comments_dict = {}
    city_dict = {}
    state_dict = {}

    for i in range(0, len(descriptions)):
        text = descriptions[i]
        university = university_names[i]
        relevant_comments_dict[university] = (text)
        # ensure the location matched is in the format of "in city, state"
        # note that both city and state can be composed of two words
        match_location = re.search(r'in ([^\d\W]+(?: [^\d\W]+)?), ([^\d\W]+(?: [^\d\W]+)?)', text)
        # check if matched and ensure that the state is a real state
        if match_location and match_location.group()[3:].split(',')[1].strip() in us_states:
            location = match_location.group()[3:].split(',')
            city_dict[university] = (location[0])
            state_dict[university] = (location[1]).strip()
        # if the regex does not match any location, check if the description
        # mentions a state
        # Please note that it's entirely possible that the description includes
        # NO information on location. For example saying University of California
        # implies the university is in California, without stating its location
        else:
            state = [state for state in us_states if (state in text)]
            if len(state) != 0:
                state_dict[university] = state[0]
                # in certain cases, the name of the university includes the city
                # for example "University of Illinois – Urbana-Champaign".
                # In these case, the description does not include location so we pull it
                # from the university name
            if re.search(r'–', university):
                city_dict[university] = university.split('–')[1].strip()
            # if the name of the city has yet to be found, look for "located"
            elif re.search(r'located|Located', text):
                # extra 3-word cities like "New York City", "Salt Lake City"
                match = re.search(r'[L|l]ocated in ([^\d\W]+(?: [^\d\W]+)?(?: [^\d\W]+)?)[.|,]', text)
                if match:
                    city_dict[university] = match.group()[10:len(match.group()) - 1]
                elif re.search(r'located [^,]*, [^\d\W]+', text):
                    city_dict[university] = re.search(r'located [^,]*, [^\d\W]+', text).group().split(",")[1]
                elif re.search(r'in the city of ', text):
                    city_dict[university] = re.search(r'in the city of ([^\d\W]+(?: [^\d\W]+)?)', text).group()[15:]

    # For 5 universities (out of 50), the location is not mentioned in the description
    # in the website (i.e the info on the website is incomplete). To avoid
    # having an incomplete dataset, we will manually enter the missing locations
    # This approach was discussed and approved by Professor John Ostlund
    city_dict['University of Pennsylvania'] = 'Philadelphia'
    city_dict['Virginia Polytechnic Institute and State University'] = 'Blacksburg'
    city_dict['North Carolina State University'] = 'Raleigh'
    city_dict['Pennsylvania State University'] = 'State College'
    city_dict['University of Pittsburgh'] = 'Pittsburgh'
    state_dict['University of Pittsburgh'] = 'Pennsylvania'

    comment_df = pd.DataFrame(relevant_comments_dict.items(),
                              columns=['University', 'Description'])
    city_df = pd.DataFrame(city_dict.items(),
                           columns=['University', 'City'])

    state_df = pd.DataFrame(state_dict.items(),
                            columns=['University', 'State'])

    # Join cdescription, city and state in one df
    comment_location_df = pd.merge(comment_df,
                                   pd.merge(city_df, state_df,
                                            on='University', how='outer'),
                                   on='University', how='outer')

    # Final merge
    final_df = pd.merge(df,
                        comment_location_df,
                        on='University', how='outer')

    if not os.path.exists(os.path.abspath("./program")):
        os.makedirs(os.path.abspath("./program"))

    final_df.to_csv(os.path.join("./program/", "scraped_data_program.csv"), index=False, encoding='utf-8-sig')

    return final_df


if __name__ == "__main__":
    collect_and_clean_pop()
    collect_and_clean_safety()
    collect_and_clean_weather()
    collect_and_clean_program()