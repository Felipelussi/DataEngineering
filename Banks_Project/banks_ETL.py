import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import datetime


def log_progress(message):
    timestamp = '%Y-%m-%d %H-%M-%S'
    now = datetime.datetime.now()
    now = now.strftime(timestamp)
    
    with open(log_file, 'a') as log:
        log.write(now + ' ' + message + '\n')
        
def extract(url, table_attribs):
    page = requests.get(url).text
    
    soup = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)    
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
             
            data_dict = {'Name': col[1].find_all('a')[1],
                         'MC_USD_Billion': float(col[2].contents[0][:-1])}
            
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df

def transform(df, csv_path):
    exchange_rates = pd.read_csv(csv_path, index_col=0)  
    
   #dict = exchange_rates.set_index('Currency').to_dict()['Rate']
    
    df['MC_GBP_Billion'] = [np.round(x*(exchange_rates['Rate']['GBP']), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*(exchange_rates['Rate']['EUR']), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*(exchange_rates['Rate']['INR']), 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)
    
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
    
def run_query(query_statement, sql_connection):
     print(query_statement)
     query_output = pd.read_sql(query_statement, sql_connection)
     print(query_output)
     
     
data_url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'

rates_csv = 'exchange_rate.csv'

table_attribs = ['Name', 'MC_USD_Billion']

output_path = 'Largest_banks.csv'

db_name = 'Banks.db'

table_name = 'Largest_banks'

log_file = 'log_file.txt'

log_progress("Preliminaires complete. Initiating the ETL process")

df = extract(data_url, table_attribs)

print(df)

log_progress('Data extraction complete. Initiating data transformation')

df = transform(df, rates_csv)

print(df)

log_progress('Data transformation complete. Initiating data loading')

load_to_csv(df, output_path)

log_progress('Data saved to csv file')

conn = sqlite3.connect(db_name)

log_progress('SQL connection complete')

load_to_db(df, conn, table_name)

log_progress('Data loaded to Database as table. Running a query')

run_query('SELECT * FROM Largest_banks', conn)
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
run_query('SELECT Name from Largest_banks LIMIT 5', conn)

log_progress('Process complete.')