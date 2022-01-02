# %%
# Standard Libraries
from source.utilities.banks.functions import up_request, ing_parse
from source.utilities.config import Auth as Auth
from IPython.core.interactiveshell import InteractiveShell
from IPython.display import display, HTML
import datetime
import os
import pandas as pd
import shutil
import yaml
pd.options.display.float_format = '${:,.2f}'.format
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

InteractiveShell.ast_node_interactivity = "all"

# Custom Libraries

# Import Configs
with open(r'.\maps\ing_to_up_columns.yaml', 'r') as file:
    ing_up_map = yaml.safe_load(file)
with open(r'.\maps\ing_account.yaml', 'r') as file:
    ing_accounts = yaml.safe_load(file)
with open(r'.\maps\tags.yaml', 'r') as file:
    tags = yaml.safe_load(file)


class Up(object):
    def __init__(self, amalgam=False):
        print('===UP Account===')
        self.accounts = self.Account()
        account_map = self.accounts.df.set_index(
            'id')['attributes_displayName'].to_dict()
        self.transactions = self.Transactions(account_map)
        if amalgam == True:
            self.transactions.df['attributes_settledAt'] = pd.to_datetime(
                self.transactions.df['attributes_settledAt'], format='%Y-%m-%d')
        print()

    class Account(object):
        def __init__(self):
            endpoint = 'accounts'
            params = None
            self.df = pd.DataFrame()
            self.df = up_request(endpoint=endpoint, params=params)
            self.df.columns = self.df.columns.str.replace(
                '.', '_', regex=False)
            self.df['twoUp'] = self.df.groupby(
                ['id'])['type'].transform('count')
            self.df = self.df.drop_duplicates('id', keep='first')
            self.df.loc[self.df.twoUp == 2, 'source'] = 'jointAccount'
            self.df = self.df[['id', 'attributes_displayName',
                               'attributes_balance_value', 'source']]
            self.df['attributes_balance_value'] = pd.to_numeric(
                self.df['attributes_balance_value']).copy()
            return

    class Transactions(object):
        def __init__(self, account_map):
            start = datetime.datetime.now()
            account_map = account_map
            try:
                self.df = pd.read_parquet(r'.\cache\up\transactions.pq')
                print(f'{len(self.df)} lines read from cached data')
            except FileNotFoundError:
                print(
                    "Cached read does not exist - Extracting All Settled Transactions from Up")
                self.read_all()
                print(f"Read completed in {datetime.datetime.now() - start}")
            self.update_all()
            self.df.columns = self.df.columns.str.replace(
                '.', '_', regex=False)
            self.df['attributes_amount_value'] = pd.to_numeric(
                self.df['attributes_amount_value']).copy()
            self.df['relationships_account_data_id'] = self.df['relationships_account_data_id'].replace(
                account_map).copy()
            self.df['relationships_transferAccount_data_id'] = self.df['relationships_transferAccount_data_id'].replace(
                account_map).copy()
            self.df['attributes_settledAt'] = self.df['attributes_settledAt'].str.slice(
                0, 10).copy()  # We don't really need the granularity of datetime, date is fine
            self.df['source'] = 'up'
            self.df['attributes_rawText'] = self.df['attributes_rawText'].astype(
                'str')

            cond1 = self.df['attributes_rawText'] == 'None'

            df_desc = self.df[cond1].copy()
            df_desc['tags'] = df_desc['attributes_description'].replace(
                tags['up']['attributes_description'], regex=True)

            df_raw = self.df[~cond1].copy()
            df_raw['tags'] = df_raw['attributes_rawText'].replace(
                tags['up']['attributes_rawText'], regex=True)

            self.df = pd.concat([df_raw, df_desc])

            cond1 = self.df['tags'] == None
            self.df.loc[cond1, 'tags'] = self.df[cond1]['attributes_description']
            return

        def read_all(self):
            endpoint = 'transactions'
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100
            }
            self.df = up_request(endpoint=endpoint, params=params)
            self.df.to_parquet(r'.\cache\up\transactions.pq')

        def update_all(self):
            if self.df.empty:
                return print('Data unavailable for updating\nEnsure that there is data prior to updating')
            init_length = len(self.df)
            filter_date = self.df['attributes.settledAt'].max()
            print(
                f'Last Transaction settled at {filter_date}, commencing iterative read from here.')
            endpoint = 'transactions'
            params = {
                'filter[status]': 'SETTLED',
                'page[size]': 100,
                'filter[since]': f'{filter_date}'
            }
            df_iter = up_request(endpoint=endpoint, params=params)
            self.df = pd.concat([df_iter, self.df])
            self.df = self.df.drop_duplicates(['id']).reset_index(drop=True)
            final_length = len(self.df)
            new_lines = final_length - init_length
            if new_lines == 0:
                print('0 new transactions found.')
            else:
                print(f'{str(new_lines)} new transactions found!')
            self.df.to_parquet(r'.\cache\up\transactions.pq')
            return


class ING(object):
    def __init__(self, amalgam=False):
        print('===ING Account===')
        self.path = r'.\cache\ing\data.pq'
        self.read_parse()
        self.accounts = self.Account()
        self.transactions = self.Transactions()
        if amalgam == True:
            self.accounts.df = self.accounts.df.rename(
                columns=ing_up_map['accounts'])
            self.transactions.df = self.transactions.df.rename(
                columns=ing_up_map['transactions'])
            self.transactions.df['attributes_settledAt'] = pd.to_datetime(
                self.transactions.df['attributes_settledAt'], format='%d/%m/%Y')
            max_date = self.transactions.df['attributes_settledAt'].max()
            print(f'Last transaction settled at {max_date}')
        return

    def read_parse(self):
        df = pd.DataFrame()
        try:
            df = pd.read_parquet(self.path)
            print(f'{len(df)} lines read from cached data')
        except FileNotFoundError:
            print("Cached Data unavailable - commence read from inputs folder")
        csvs = []
        if 'Transactions.csv' in os.listdir(r'C:\Users\leeti\Downloads'):
            shutil.move(
                r'C:\Users\leeti\Downloads\Transactions.csv',
                r'.\inputs\ing\Transactions.csv')
        for i in os.listdir(r'.\inputs\ing'):
            if i.find('.csv') >= 0:
                csvs.append(i)
        if len(csvs) != 0:
            print(f'{len(csvs)} input files detected - reading and parsing..')
            for i in csvs:
                df_iter = pd.read_csv(r'.\inputs\ing\\' + i)
                df = pd.concat([df_iter, df])
                df = df.drop_duplicates().reset_index(drop=True)
            df.to_parquet(self.path)
        else:
            print('No new inputs detected.')
        # Clear out inputs as they've been collected and parsed
        for i in os.listdir(r'.\inputs\ing'):
            if i.find('.csv') >= 0:
                os.remove(r'.\inputs\ing\\'+i)
        return

    class Account(object):
        def __init__(self):
            self.df = pd.read_parquet(r'.\cache\ing\data.pq')
            self.df = ing_parse(self.df)
            self.df = self.df.groupby(['Account', 'id'])[
                'Total'].sum().reset_index()
            self.df['source'] = 'ing'
            return

    class Transactions(object):
        def __init__(self):
            self.df = self.df = pd.read_parquet(r'.\cache\ing\data.pq')
            self.df = ing_parse(self.df)
            self.df['source'] = 'ing'
            self.df['tags'] = self.df['Description'].replace(
                tags['ing']['Description'], regex=True)
            # Remove internal transfers - this muddies up sum totals
            # self.df = self.df[self.df.Description.str.find(
            #     'Internal Transfer') < 0]
            # self.df[self.df['Description']==self.df['tags'], 'tags'] = None
