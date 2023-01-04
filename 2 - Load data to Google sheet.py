# -- Load --

# pip install gspread
# pip install oauth2client
# pip install pandas

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Connect to Google Sheets
scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

credentials = ServiceAccountCredentials.from_json_keyfile_name("gs_credentials.json", scope)
gc = gspread.authorize(credentials)

# Function to insert new data and update old data (with primary key id)---------------------------

def load(data_to_load, destination_database_url, destination_sheet):
    # open sheet and take id list

    wks = gc.open_by_key(destination_database_url).worksheet(destination_sheet)
    id_column = wks.col_values(1)

    #------------------------------------------------------
    # function to update data to database

    def append_from_df_to_db(df):
        a = len(wks.col_values(1)) +1
        position_to_insert = 'A' + str(a)
        wks.update(f'{position_to_insert}', df.values.tolist())
        
    #------------

    def update_row(df_1_row):
        a = id_column.index(df_1_row['id'][0]) + 1
        position = 'A' + str(a)
        wks.update(f'{position}', df_1_row.values.tolist())
            
    def check_if_record_exists(x):
        if x in id_column:
            return 'Yes'
        else: 
            return 'No'
        
    #------------

    def update_db(df):
        tmp_df = pd.DataFrame(columns = list(df.columns))

        for i, row in df.iterrows():
            # If record already exists then we will update
            if check_if_record_exists(row['id']) == 'Yes':
                # append that row to a df
                df_1_row = pd.DataFrame(columns = list(df.columns)).append(row)
                # reset index of df
                df_1_row.reset_index(drop = True, inplace = True)
                # update that row to sheet
                update_row(df_1_row)

            # The record doesn't exists so we will add it to a temp df and append it using append_from_df_to_db
            else: 
                tmp_df = tmp_df.append(row)

        return tmp_df

    #------------------------------------------------------
    #update data for existing videos
    new_record_df = update_db(data_to_load)

    #insert new data into db table
    append_from_df_to_db(new_record_df)

    # wait before move to next task
    time.sleep(1 * 60)


# Function to insert/remove all data (except header) ---------------------------
def remove(destination_database_url, destination_sheet):

    wks = gc.open_by_key(destination_database_url).worksheet(destination_sheet)
    wks.batch_clear(['A2:Z'])
    # wait before move to next task
    time.sleep(5)

def insert_all(data_to_load, destination_database_url, destination_sheet):
    wks = gc.open_by_key(destination_database_url).worksheet(destination_sheet)
    wks.update('A2', data_to_load.values.tolist())
    # wait before move to next task
    time.sleep(1 * 60)

# -- Flow --

# LOAD FACEBOOK DATA
destination_database_url   = 'YOUR URL HERE'

# page metric
load(fb_page_df, destination_database_url, 'page_metric')
# post metric
load(fb_post_df, destination_database_url, 'post_metric')

# audience metric
remove(destination_database_url, 'au_age_gender')
insert_all(fb_au_age_gender_df, destination_database_url, 'au_age_gender')

remove(destination_database_url, 'au_city')
insert_all(fb_au_city_df, destination_database_url, 'au_city')

remove(destination_database_url, 'au_country')
insert_all(fb_au_country_df, destination_database_url, 'au_country')

