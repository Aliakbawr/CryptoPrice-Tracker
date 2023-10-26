import requests
import pandas as pd
import json
import sqlite3

#Setting up the database
conn = sqlite3.connect('currencydb.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Currency;

CREATE TABLE Currency (
                  id                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                  name                  TEXT UNIQUE,
                  symbol                TEXT, 
                  price                 INTEGER,
                  percent_change_1h     INTEGER,
                  percent_change_24h    INTEGER,
                  percent_change_7d     INTEGER,
                  percent_change_30d    INTEGER,
                  percent_change_60d    INTEGER,
                  percent_change_90d    INTEGER,
                  volume_24h            INTEGER,
                  market_cap            INTEGER,
                  last_updated          INTEGER
)
''')

#open text file which includes the API-key
with open('api_key.txt') as file:
    API_KEY = file.read()    

#URL and Headers needed:
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
HEADERS = {'Accepts': 'application/json',
           'X-CMC_PRO_API_KEY': API_KEY}

#Get the first 1000 rows
PARAMS = {'convert': 'USD',
          'limit': 1001}

#satisfy all the requirements
resp = requests.get(url, headers=HEADERS, params=PARAMS)

def get_data(resp):
    json_data = json.loads(resp.content)
    # print(json_data)
    data = json.loads(resp.content)['data']
    # print(type(data))
    rows = list()
    
    for item in data:
        rows.append([
             item['name'], 
             item['symbol'],
             item['quote']['USD']['price'],
             item['quote']['USD']['volume_change_24h'],
             item['quote']['USD']['percent_change_1h'],
             item['quote']['USD']['percent_change_24h'],
             item['quote']['USD']['percent_change_7d'],
             item['quote']['USD']['percent_change_30d'],
             item['quote']['USD']['percent_change_60d'],
             item['quote']['USD']['percent_change_90d'],
             item['quote']['USD']['market_cap'],
             item['quote']['USD']['volume_24h'],
             item['quote']['USD']['last_updated']])

    df = pd.DataFrame(rows, columns=['name','symbol','price','volume_change_24h','percent_change_1h',
                                     'percent_change_24h','percent_change_7d','percent_change_30d',
                                     'percent_change_60d','percent_change_90d','market_cap'
                                     ,'volume_24h','last_updated'])

    # remove timezone from datetime64[ns, UTC] in `last_updated`
    df['last_updated'] = pd.to_datetime(df.last_updated).dt.tz_localize(None)
    df.index.name = 'id'
    return(df)

if resp.status_code == 200:
    df = get_data(resp)
    # write to csv
    df.to_csv('cryptocurrency_updates.csv')
else:
    # bad request, handle error
    print(f"status code:{resp.status_code}")
#test the df
print(df.head())
#write to sql
df.to_sql('Currency', conn, if_exists='replace', index=False) # - writes the pd.df to SQLIte DB
pd.read_sql('select * from Currency', conn)
conn.commit()
conn.close()