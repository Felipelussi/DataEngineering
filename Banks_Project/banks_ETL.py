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
        
