#%%
# Standard Libraries
import datetime
import os
import pandas as pd
pd.options.display.float_format = '${:,.2f}'.format

from IPython.display import display, HTML
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Custom Libraries
# from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth
from source.utilities.functions import up_request, ing_parse

class Up(object):
    def __init__(self):
        self.accounts = self.Account()
        self.transactions = self.Transactions()

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
            start = datetime.datetime.now()
            auth = Auth()
            try:
                self.df = pd.read_parquet(r'.\cache\up\transactions.pq')
            except FileNotFoundError:
                print("Cached read does not exist - Extracting All Settled Transactions from Up")
                self.read_all()
                print(f"Read completed in {datetime.datetime.now() - start}")
            self.update_all()
            return

        def read_all(self):
            auth = Auth()
            endpoint = 'transactions'
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100
                }
            self.df = up_request(endpoint = endpoint, params = params)
            self.df.to_parquet(r'.\cache\up\transactions.pq')

        def update_all(self):
            if self.df.empty:
                return print('Data unavailable for updating\nEnsure that there is data prior to updating')
            init_length = len(self.df)
            filter_date = self.df['attributes.settledAt'].max()
            print(f'Last Transaction settled at {filter_date}, commencing iterative read from here.')
            endpoint = 'transactions'
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100,
                'filter[since]': f'{filter_date}'
                }
            df_iter = up_request(endpoint=endpoint, params=params)
            self.df = pd.concat([df_iter,self.df])
            self.df = self.df.drop_duplicates(['id']).reset_index(drop=True)
            final_length = len(self.df)
            new_lines = final_length - init_length
            if new_lines==0:
                print('0 new transactions found.')
            else:
                print(f'{str(new_lines)} new transactions found!')
            self.df.to_parquet(r'.\cache\up\transactions.pq')
            return

class ING(object):
    def __init__(self):
        self.path = r'.\cache\ing\data.pq'
        self.read_parse()
        self.accounts = self.Account()
        self.transactions = self.Transactions()
        return

    def read_parse(self):
        df = pd.DataFrame()
        try: 
            df = pd.read_parquet(self.path)
            print(f'{len(df)} lines read from cached data')
        except FileNotFoundError: 
            print("Cached Data unavailable - commence read from inputs folder")
        csvs = []
        for i in os.listdir(r'.\inputs\ing'):
            if i.find('.csv')>=0:
                csvs.append(i)
        if len(csvs) != 0:
            print(f'{len(csvs)} input files detected - reading and parsing..')
            for i in csvs:
                df_iter = pd.read_csv(r'.\inputs\ing\\' + i)
                df = pd.concat([df_iter,df])
            df.to_parquet(self.path)
        else:
            print('No new inputs detected.')
        for i in os.listdir(r'.\inputs\ing'): # Clear out inputs as they've been collected and parsed
            if i.find('.csv')>=0:
                os.remove(r'.\inputs\ing\\'+i)
        return


    class Account(object):
        def __init__(self):
            self.df = pd.read_parquet(r'.\cache\ing\data.pq')
            self.df = ing_parse(self.df)
            self.df = self.df.groupby(['Account'])['Total'].sum().reset_index()
            return
    
    class Transactions(object):
        def __init__(self):
            self.df = self.df = pd.read_parquet(r'.\cache\ing\data.pq')
            self.df = ing_parse(self.df)

