#%%
API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'

# Standard Libraries
import json
import pandas as pd
import requests

# Custom Libraries
from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth

# Scrape Authentication
auth = Auth()
auth.load()

accounts = pd.DataFrame()
for i in auth.df.index:
    data = requests.get(
        url=API_URL+r'/accounts', 
        headers={'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])}).json()
    df_iter = pd.json_normalize(data['data'])
    df_iter['owner'] = auth.df.iloc[i,0]
    accounts = pd.concat([df_iter,accounts]).reset_index(drop=True)

