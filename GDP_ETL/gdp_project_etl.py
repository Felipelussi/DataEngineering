import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
import datetime


# Code for ETL operations on Country-GDP data





def extract(url, table_attribs):
    page = requests.get(url).text   
     
    soup = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)
    
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col) !=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)   
  
    return df



def transform(df):
   gdp_list = df["GDP_USD_millions"].tolist()
   gdp_list = [float("".join(x.split(","))) for x in gdp_list]
   gdp_list = [np.round(x/1000) for x in gdp_list]
   df.iloc[:,1] = gdp_list
   df.reindex(columns=['Country', 'GDP_USD_billions'])
   print(df)
   return df



def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
   print(query_statement)
   query_output = pd.read_sql(query_statement, sql_connection)
   print(query_output)

def log_progress(message):
    timestamp = '%Y-%m-%d %H-%M-%S'
    now = datetime.datetime.now()
    now = now.strftime(timestamp)
    
    with open(log_file, 'a') as log:
        log.write(now + ' ' + message + '\n')
        
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
        
table_attribs = ['Country', 'GDP_USD_millions']

db_name = 'World_Economies.db'

table_name = 'Countries_by_GDP'

csv_path = 'Countries_by_GDP.csv'

log_file = 'log_file.txt'


query = 'SELECT * from Countries_by_GDP'

    
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extration complete. Initiating Transformation process')
 
df = transform(df)

log_progress('Data Transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to csv file')

conn = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, conn, table_name)

log_progress('Data loaded to Database as table. Running a query')

run_query(query, conn)
conn.close()

log_progress('Process complete.')