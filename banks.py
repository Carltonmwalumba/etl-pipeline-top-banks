#Importing the required libraries
import os
from datetime import datetime
import glob
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

#Define your output files
log_file = 'code_log.txt'
target_file = 'largest_banks_data.csv'

#Define your link for webscraping
url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
line = 10

#Define function for performing the web scrapping and extracting the data to a datframe
def extract(link, number):
    response= requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    #table = soup.find_all('table')[2]
    # header = table.find_all('th')
    # elements = [header[i] for i in [0, 4]]
    # title = [title.text.strip() for title in elements]
    df = pd.DataFrame(columns = ['Name', 'MC_USD_Billion'])
    count = 0
    body = soup.find_all('tbody')[2]
    rows = body.find_all('tr')

    for row in rows:
        if count < number:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {"Name": col[0].text.strip(),
                             'MC_USD_Billion': col[2].text.strip()
                             }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat((df, df1), ignore_index=True)
                count +=1
        else:
            break
    return df


# Define your data for transforming the dataframe
def read_provided_data():
    provided_data = pd.read_csv('exchange_rate.csv')
    return provided_data

# Transform the dataframe by adding columns converting the amount to different currencies
def transform(data):
    provided = read_provided_data()
    eur = provided.loc[0, 'Rate']
    gbp = provided.loc[1, 'Rate']
    inr = provided.loc[2, 'Rate']
    data['MC_USD_Billion'] = pd.to_numeric(data['MC_USD_Billion'], errors="coerce")
    data['MC_EUR_Billion'] = round(data['MC_USD_Billion'].apply(lambda x: x * eur), 2)
    data['MC_GBP_Billion'] = round(data['MC_USD_Billion'].apply(lambda x: x * gbp), 2)
    data['MC_INR_Billion'] = round(data['MC_USD_Billion'] .apply(lambda x: x * inr), 2)
    return data

# Load the data to a target output file
def load_to_csv(file, transformed_data):
    transformed_data.to_csv(target_file)

#Define your data base directory database and table name
# as well as the argument for searching your directory for the required data
input_folder = 'db'
db_name = 'Banks.db'
name = 'Largest_banks'
value = '*banks_data.csv'

#Create function to load dataframe to the database as a table
def load_db(folder, db):
    if not os.path.exists(input_folder):
        os.mkdir(input_folder)
    data_path = os.path.join(input_folder, db_name)
    conn = sqlite3.connect(data_path)
    table_name = name
    for csvfile in glob.glob(value):
        df = pd.read_csv(csvfile)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

    conn.close()

db_path = os.path.join(input_folder, db_name)

# Define function torun queries on the database table.
def run_queries(path):
    conn = sqlite3.connect(path)
    table_name = name
    query_statement = f'SELECT * FROM {table_name}'
    query_statement2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
    query_statement3 = f'SELECT Name FROM {table_name} LIMIT 5'
    query_output = pd.read_sql(query_statement, conn)
    query_output2 = pd.read_sql(query_statement2, conn)
    query_output3 = pd.read_sql(query_statement3, conn)
    print(query_output)
    print(query_output2)
    print(query_output3)
    conn.close()

# Document the pipeline process using timestamps
def log_progress(message):
    time_format = '%Y-%h-%D-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(time_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')

log_progress('ETL Has Started')

log_progress('Extraction Has Started')
extracted_data = extract(url, line)
print('Extracted Data\n')
print(extracted_data)

log_progress('Transformation Has Began')
transformed = transform(extracted_data)
print('Transformed Data\n')
print(transformed)

log_progress('Transformation has Ended!!')

log_progress('Loading phase started')
load_to_csv(target_file, transformed)

log_progress('Loading of Data is Successful')

log_progress('Loading to Database!!')
load_db(input_folder, db_name)
print('Table Successfully Created\n')
print('Loading to Database Complete\n')

log_progress('Running Queries!!')
run_queries(db_path)

log_progress('Process has ended')

