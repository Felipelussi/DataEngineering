import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import datetime


data_url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'



def log_progress(message):
    timestamp = '%Y-%m-%d %h-%m-%s'
    now = datetime.datetime.now()
    now = now.strftime(timestamp)
    
    with open('code_log.txt', 'a') as log:
        log.write(now + " " + message + "\n")
        
def extract(url):
    page = requests.get(url).text
    
    soup = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=['Name', 'MC_USD_Billion'])    
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0 :
             
            data_dict = {'Name': col[1].find_all('a')[1],
                         'MC_USD_Billion': str(col[2].contents[0]).split('\n')[0]}
            
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df

print(extract(data_url))
    