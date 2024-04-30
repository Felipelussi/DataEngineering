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