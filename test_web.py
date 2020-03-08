import datetime
import requests
from lxml import etree
import sqlite3
import time
import pandas as pd
import configparser
import random

conn = sqlite3.connect('D:\\projects\\log\\goods.db')
# print('%d-%02d-%02d' % (year,mouth,day))
cur = conn.cursor()
url = 'http://www.100ppi.com/sf/day-' + date + '.html'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
response = requests.get(url, headers=headers)
# print(response.text)
dom_tree = etree.HTML(response.text)

