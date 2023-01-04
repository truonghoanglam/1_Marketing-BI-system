# -- Function list --

# Import library
import requests 
import pandas as pd

from datetime import date
from datetime import timedelta

import datetime
import time

# Function to extract data from a dictionary
def extract_data(column, key_data_to_extract):
    list_to_collect_data = []
    for i in column:
        if type(i) == dict and key_data_to_extract in i:
            list_to_collect_data.append(i[key_data_to_extract])
        else:
            list_to_collect_data.append(0)
    return list_to_collect_data

# Function to convert time valuevalue
def convert_time(x):
    convert_time = []
    for i in x:
        if i != '':
            i = i.split('T', 1)[0] +' '+ i.split('T', 1)[1]
            i = pd.to_datetime(
            
                    i[:19]
            
                ) + pd.DateOffset(hours=7)
            convert_time.append(i)
        else:
            convert_time.append('')
    return convert_time

# Function to convert data to text format
def convert_col_to_text(x):
    x_value = []
    for i in x:
        i = str(i)
        x_value.append(i)
    return x_value

ninety_days_ago   = (date.today() + timedelta(days= -90)).strftime("%Y-%m-%d")
thirty_days_ago   = (date.today() + timedelta(days= -30)).strftime("%Y-%m-%d")
five_days_ago     = (date.today() + timedelta(days= -5)).strftime("%Y-%m-%d")
yesterday         = (date.today() + timedelta(days= -1 )).strftime("%Y-%m-%d")

# -- [FB] Page metric --

fb_token = 'ADD YOUR TOKEN HERE'

fb_page_metric_since_time = five_days_ago

fb_page_metric_list = ','.join([
                                'page_consumptions_by_consumption_type', 
                                'page_fan_adds', 
                                'page_views_total', 
                                'page_impressions_unique', 
                                'page_engaged_users'])

fb_page_api_url = f'https://graph.facebook.com/8640159245089/insights?metric={fb_page_metric_list}&access_token={fb_token}&since={fb_page_metric_since_time}'

data  = requests.get(fb_page_api_url).json()

data = {'id'                : data['data'][0]['values'], # this is date column, used as id key
        'link_click'        : data['data'][0]['values'],
        'other_clicks'      : data['data'][0]['values'],
        'video_play'        : data['data'][0]['values'],
        'new_like'          : data['data'][1]['values'],
        'visit'             : data['data'][2]['values'],
        'reach'             : data['data'][3]['values'],
        'engagement'        : data['data'][4]['values']}

fb_page_df = pd.DataFrame(data)

# Transfrom column

# extract date from id column
fb_page_df['id']                   = extract_data(fb_page_df['id'], 'end_time')
fb_page_df['id']                   = convert_time(fb_page_df['id'])

date = []
for i in fb_page_df['id']:
    date.append(str(i)[:10])
fb_page_df['id'] = date

# extract data from other column
fb_page_df['link_click']           = extract_data(fb_page_df['link_click'], 'value')
fb_page_df['link_click']           = extract_data(fb_page_df['link_click'], 'link clicks')

fb_page_df['other_clicks']         = extract_data(fb_page_df['other_clicks'], 'value')
fb_page_df['other_clicks']         = extract_data(fb_page_df['other_clicks'], 'other clicks')

fb_page_df['video_play']           = extract_data(fb_page_df['video_play'], 'value')
fb_page_df['video_play']           = extract_data(fb_page_df['video_play'], 'video play')

fb_page_df['new_like']             = extract_data(fb_page_df['new_like'], 'value')
fb_page_df['visit']                = extract_data(fb_page_df['visit'], 'value')
fb_page_df['reach']                = extract_data(fb_page_df['reach'], 'value')
fb_page_df['engagement']           = extract_data(fb_page_df['engagement'], 'value')

display(fb_page_df)

# -- [FB] Post metric --

# post list
fb_post_metric_since_time = ninety_days_ago

data_need = []

fb_post_api_url  = f'https://graph.facebook.com/v15.0/8640159245089/feed?fields=created_time%2Cpermalink_url%2Cattachments%2Cshares&access_token={fb_token}&since={fb_post_metric_since_time}'
print(fb_post_api_url)
data  = requests.get(fb_post_api_url).json()

data_need.extend(data['data'])
url_1 = data['paging']['next']

while True:
    print(url_1)
    data = requests.get(url_1).json()
    data_need.extend(data['data'])

    # Are there any data left?
    if 'next' in data['paging'] :
        url_1 = data['paging']['next']
    else:
        break
        
# Now we got data_need that contain all the data. Make it a dataframe:
fb_post_df = pd.DataFrame(data_need)

# filter only data that needed
fb_post_df  = fb_post_df[['id', 'created_time', 'permalink_url', 'shares']]

display(fb_post_df.head(3))

## post metric
post_clicks_by_type = []
post_reactions_by_type_total = []
page_posts_impressions_unique = []
post_engaged_users = []

post_metric_list = ['post_clicks_by_type', 'post_reactions_by_type_total', 
                    'post_impressions_unique', 'post_engaged_users']
post_metric_list = ','.join(post_metric_list)

for i in fb_post_df['id']:
    post_url = f'https://graph.facebook.com/v15.0/{i}/insights?metric={post_metric_list}&access_token={fb_token}'
    data  = requests.get(post_url).json()

    df = pd.DataFrame(data['data'])

    post_clicks_by_type.append(df['values'][0][0]['value'])
    post_reactions_by_type_total.append(df['values'][1][0]['value'])        
    page_posts_impressions_unique.append(df['values'][2][0]['value'])
    post_engaged_users.append(df['values'][3][0]['value'])


fb_post_df['post_clicks_by_type']              =  post_clicks_by_type
fb_post_df['post_reactions_by_type_total']     = post_reactions_by_type_total
fb_post_df['page_posts_impressions_unique']    = page_posts_impressions_unique
fb_post_df['post_engaged_users']               = post_engaged_users

display(fb_post_df.head())

# extract date from id column
fb_post_df['created_time']                   = convert_time(fb_post_df['created_time'])

date = []
for i in fb_post_df['created_time']:
    date.append(str(i)[:10])
fb_post_df['created_time'] = date

# create total reaction column
total_reaction = []

for i in fb_post_df['post_reactions_by_type_total']:
    list_value = []
    for key in i.keys() :
        list_value.append(i[key])
        
    total_reaction.append(sum(list_value))
fb_post_df['total_reaction'] = total_reaction

# other column
fb_post_df['shares']                 = extract_data(fb_post_df['shares'], 'count')
fb_post_df['link_clicks']            = extract_data(fb_post_df['post_clicks_by_type'], 'link_clicks')

# drop no need column
fb_post_df = fb_post_df.drop(columns = ['post_clicks_by_type', 
                                        'post_reactions_by_type_total'])

# rename column
fb_post_df.columns = ['id', 'created_time', 'url', 'shares', 'impressions', 
                       'engagement', 'total_reaction', 'link_clicks']

display(fb_post_df.head())


# -- [FB] Audience metric --


fb_audience_metric_since_time = yesterday

fb_audience_metric_list = ','.join([
                                'page_fans_city',
                                'page_fans_country',
                                'page_fans_gender_age'])

fb_audience_api_url = f'https://graph.facebook.com/8640159245089/insights?metric={fb_audience_metric_list}&access_token={fb_token}&since={fb_audience_metric_since_time}'

print(fb_audience_api_url)
data  = requests.get(fb_audience_api_url).json()

# CREATE DF FOR AGE AND GENDER DATA

fb_au_age_gender_df = pd.DataFrame.from_dict(
                            data['data'][2]['values'][0]['value'], 
                            orient ='index') 
fb_au_age_gender_df = fb_au_age_gender_df.reset_index()

# create gender column
gender = []
for i in fb_au_age_gender_df['index']:
    gender.append(i[0])

fb_au_age_gender_df['gender'] = gender

# create age column
age = []
for i in fb_au_age_gender_df['index']:
    age.append(i[2:])

fb_au_age_gender_df['age'] = age

# create fan column
fb_au_age_gender_df['no_of_fans'] = fb_au_age_gender_df[0]

# filer only needed columns
fb_au_age_gender_df = fb_au_age_gender_df[['gender', 'age', 'no_of_fans']]

display(fb_au_age_gender_df.head(5))

# CREATE DF FOR FAN CITY DATA
fb_au_city_df = pd.DataFrame.from_dict(
                            data['data'][0]['values'][0]['value'], 
                            orient ='index') 
fb_au_city_df = fb_au_city_df.reset_index()

# create column
fb_au_city_df['city']  = fb_au_city_df['index']
fb_au_city_df['no_of_fans']  = fb_au_city_df[0]

# filter only needed column
fb_au_city_df = fb_au_city_df[['city', 'no_of_fans']]

display(fb_au_city_df.head(5))

# CREATE DF FOR FAN COUNTRY DATA
fb_au_country_df = pd.DataFrame.from_dict(
                            data['data'][1]['values'][0]['value'], 
                            orient ='index') 
fb_au_country_df = fb_au_country_df.reset_index()

# create column
fb_au_country_df['country']  = fb_au_country_df['index']
fb_au_country_df['no_of_fans']  = fb_au_country_df[0]

# filter only needed column
fb_au_country_df = fb_au_country_df[['country', 'no_of_fans']]

display(fb_au_country_df.head(5))
