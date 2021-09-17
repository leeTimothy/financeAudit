#%%
API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'

# Standard Libraries
import json
import pandas as pd
import requests

# Custom Libraries
# from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth

class Account(object):

    def __init__(self):
        auth = Auth()
        self.df = pd.DataFrame()
        for i in auth.df.index:
            data = requests.get(
                url=API_URL+r'/accounts', 
                headers={'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])}).json()
            df_iter = pd.json_normalize(data['data'])
            df_iter['owner'] = auth.df.iloc[i,0]
            self.df = pd.concat([df_iter,self.df]).reset_index(drop=True)

        self.df.columns = self.df.columns.str.replace('.', '_', regex=False)
        self.df['twoUp'] = self.df.groupby(['id'])['type'].transform('count')
        self.df = self.df.drop_duplicates('id', keep='first')
        self.df.loc[self.df.twoUp == 2, 'owner'] = 'jointAccount'
        self.df = self.df[['id','attributes_displayName','attributes_balance_value','owner']]
        return

class Transactions(object):
    def __init__(self):
        auth = Auth()
        self.df = pd.DataFrame()
        for i in auth.df.index:
            data = requests.get(
                url=API_URL+r'/transactions', 
                headers={'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])}).json()
            df_iter = pd.json_normalize(data['data'])
        return

#%%

API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'

# Standard Libraries
import json
import pandas as pd
import requests

# Custom Libraries
# from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth

counter = 0
auth = Auth()
df = pd.DataFrame()
for i in auth.df.index:
    data = requests.get(
        url = API_URL+r'/transactions', 
        headers = {'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])},
        params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100
                }).json()
    df_iter = pd.json_normalize(data['data'])
    df_iter['source'] = auth.df.iloc[i]['name']
    df = pd.concat([df_iter,df])

    more_transactions = data['links']['next']!=None
    while more_transactions:
        url = data['links']['next']
        data = requests.get(
            url = url,
            headers = {
                'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])
                },
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100
                }).json()
        df_iter = pd.json_normalize(data['data'])
        df_iter['source'] = auth.df.iloc[i]['name']
        df = pd.concat([df_iter,df])
        more_transactions = data['links']['next']!=None
        counter+=1

