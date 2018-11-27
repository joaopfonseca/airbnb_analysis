# coding: utf-8

import pandas as pd
import geograpy

pd.options.mode.chained_assignment = None # default = 'warn'


def preprocess_countries_reviews_table(table, directory=''):
    #reviews = pd.read_csv('data/Portugal_Review_2018-02-02.csv')
    #countries = pd.read_csv('country_code_and_iso.csv')
    reviews = table.drop_duplicates()

    weirdaf_origins = reviews['Country'].unique().tolist()
    weirdaf_cities = reviews['City'].unique().tolist()

    def upper_case(text):
        return str(text).upper()

    origins = pd.DataFrame({'weirdaf_origins': weirdaf_origins})

    successful_parse = 0
    unsuccessful_parse = 0

    origins['country_after_parse'] = 0
    for row in range(len(origins['weirdaf_origins'])):
        text = origins['weirdaf_origins'][row]
        text = str(text).title()
        try:
            geograpy_frame = geograpy.get_place_context(text = text).countries
            if len(geograpy_frame) > 0:
                successful_parse+=1
                origins['country_after_parse'][row] = geograpy_frame[0]
                if origins['weirdaf_origins'][row] == 'United Kingdom':
                    origins['country_after_parse'][row] = 'United Kingdom'
            else:
                unsuccessful_parse+=1
                origins['country_after_parse'][row] = '-'
        except KeyError as e:
            origins['country_after_parse'][row] = e


    cities = pd.DataFrame({'weirdaf_cities': weirdaf_cities})

    print('Successful parse using Countries count: ', successful_parse)
    print('Unsuccessful parse using Countries count: ', unsuccessful_parse)
    print('Total uniques using Countries values: ', len(weirdaf_origins))

    successful_parse_ = 0
    unsuccessful_parse_ = 0

    cities['country_from_city_parse'] = 0
    for row in range(len(cities['weirdaf_cities'])):
        text = cities['weirdaf_cities'][row]
        text = str(text).title()
        try:
            geograpy_frame = geograpy.get_place_context(text = text).countries
            if len(geograpy_frame) > 0:
                successful_parse_+=1
                cities['country_from_city_parse'][row] = geograpy_frame[0]
            else:
                unsuccessful_parse_+=1
                cities['country_from_city_parse'][row] = '-'
        except KeyError as e:
            cities['country_from_city_parse'][row] = e


    print('\nSuccessful parse using Cities count: ', successful_parse_)
    print('Unsuccessful parse using Cities count: ', unsuccessful_parse_)
    print('Total uniques using Cities values: ', len(weirdaf_cities))

    print('\nApplying parsed countries to the complete Data Set...\n')

    reviews['Country'] = reviews['Country'].apply(upper_case)
    origins['Country'] = origins['weirdaf_origins'].apply(upper_case)
    origins = origins.drop_duplicates(subset=['country_after_parse', 'Country'])
    table_merge = pd.merge(reviews, origins, how='left', on='Country').drop_duplicates()

    cities['City'] = cities['weirdaf_cities'].apply(upper_case)
    table_merge['City'] = table_merge['City'].apply(upper_case)
    cities = cities.drop_duplicates()#subset=['country_from_city_parse', 'City'])
    table_merge2 = pd.merge(table_merge, cities, how='left', on='City').drop_duplicates()

    table_merge2['final_country_parse'] = table_merge2['country_after_parse'].where(table_merge2['country_after_parse'] != '-',table_merge2['country_from_city_parse'])

    final_table = table_merge2[[ 'Property ID', 'Latitude', 'Longitude', 'Address', 'Review Date',
               'Review Text', 'User ID', 'Member Since', 'First Name', 'Country',
               'State', 'City', 'country_after_parse', 'country_from_city_parse',
               'Description', 'School', 'Work', 'Profile Image URL', 'Profile URL',
               'final_country_parse']].drop_duplicates()

    def correction(row):
        if row['Country'] == 'DE' or row['Country'] == 'GERMANY':
            return 'Germany'
        elif row['Country'] == 'SOUTH AFRICA':
            return 'South Africa'
        elif row['Country'] == 'FR':
            return 'France'
        elif row['Country'] == 'NETHERLANDS':
            return 'Netherlands'
        elif row['Country'] == 'UNITED STATES':
            return 'United States'
        elif row['Country'] == 'PT':
            return 'Portugal'
        elif row['Country'] == 'UNITED ARAB EMIRATES':
            return 'United Arab Emirates'
        elif row['Country'] == 'AU':
            return 'Australia'
        elif row['Country'] == 'GB':
            return 'United Kingdom'
        elif row['Country'] == 'BR':
            return 'Brazil'
        elif row['Country'] == 'INDIA':
            return 'India'
        else:
            return row['final_country_parse']   

    final_table['final_country_parse'] = final_table.apply(lambda x: correction(x), axis=1)
    
    final_unsuccesful = len(final_table[final_table['final_country_parse'] == '-']['final_country_parse'])
    total_processed = len(final_table['final_country_parse'])
    final_successful = total_processed - final_unsuccesful
    
    final_table.to_csv(directory+'Portugal_Review_Standardized.csv', index=False)

    print('Successfully parsed origins: ', final_successful)
    print('Unsuccessfully parsed origins: ', final_unsuccesful)
    print('Total origins processed: ', total_processed)


    print('Done!')
