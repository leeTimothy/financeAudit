#%%
import pandas as pd
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000
import yaml

with open(r'.\source\banks\transactions\names.yaml', 'r') as file:
    names = yaml.safe_load(file)

import source.banks.transactions as trans

class Amalgam(object):
    def __init__(self):
        self.up = trans.Up(amalgam=True)
        self.ing = trans.ING(amalgam=True) # Assumed all values within are provided at settledAt rather than createdAt - see accounts.ING for more info
        self.accounts = pd.concat([self.up.accounts.df,self.ing.accounts.df])
        self.df = pd.concat([self.up.transactions.df, self.ing.transactions.df])

        self.df = parse_transactions(self.df)
        self.up = self.up.transactions.df
        self.ing = self.ing.transactions.df
        return 

def parse_transactions(df):
    df = df.sort_values('attributes_settledAt', ascending = False).reset_index(drop=True)
    df['month'] = df['attributes_settledAt'].dt.month
    df['year'] = df['attributes_settledAt'].dt.year
    df['month_year'] = df['attributes_settledAt'].dt.to_period('M')
    return df

def tag_transactions(df):
    
    return



# %%
