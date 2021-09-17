#%%
# Standard Libraries
import json
import pandas as pd
import requests

from IPython.display import display, HTML
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Custom Libraries
# from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth
from source.utilities.functions import up_request

class Up(object):
    def __init__(self):
        self.accounts = self.Account().df
        self.transactions = self.Transactions().df

    class Account(object):
        def __init__(self):
            auth = Auth()
            endpoint = 'accounts'
            params = None
            self.df = pd.DataFrame()
            self.df = up_request(endpoint = endpoint, params=None)
            self.df.columns = self.df.columns.str.replace('.', '_', regex=False)
            self.df['twoUp'] = self.df.groupby(['id'])['type'].transform('count')
            self.df = self.df.drop_duplicates('id', keep='first')
            self.df.loc[self.df.twoUp == 2, 'source'] = 'jointAccount'
            self.df = self.df[['id','attributes_displayName','attributes_balance_value','source']]
            return

    class Transactions(object):
        def __init__(self):
            auth = Auth()
            try:
                self.df = pd.read_parquet(r'.\cache\up\transactions.pq')
            except FileNotFoundError:
                print("Cached read does not exist - Extracting All Settled Transactions from Up")
                self.read_all()
                print("Done!")
            return

        def read_all(self):
            auth = Auth()
            endpoint = 'transactions'
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100
                }
            self.df = up_request()
            self.df.to_parquet(r'.\cache\up\transactions.pq')

            def update_all(self):
                if self.df.empty:
                    return print('Data unavailable for updating\nEnsure that there is data prior to updating')
            filter_date = self.df.attributes.createdAt.max()
            for i in auth.df.index:
                data = requests.get(
                    url = API_URL+r'/transactions', 
                    headers = {'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])},
                    params = {
                            'filter[status]': 'SETTLED',
                            'page[size]': 100,
                            'filter[since]': f'{filter_date}'
                            }).json()
                df_iter = pd.json_normalize(data['data'])
                df_iter['source'] = auth.df.iloc[i]['name']
                self.df = pd.concat([df_iter,self.df])
            return

#%%

from source.accounts import accounts
up = accounts.Up()
up.accounts