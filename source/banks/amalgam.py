#%%
import pandas as pd
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

from source import accounts

def parse_transactions(df):
    df = df.sort_values('attributes_settledAt', ascending = False).reset_index(drop=True)
    df['month'] = df['attributes_settledAt'].dt.month
    df['year'] = df['attributes_settledAt'].dt.year
    df['month_year'] = df['attributes_settledAt'].dt.to_period('M')
    return df

def tag_transactions(df):
    return

class Banks(object):

    def __init__(self):
        self.up = accounts.Up(amalgam=True)
        self.ing = accounts.ING(amalgam=True) # Assumed all values within are provided at settledAt rather than createdAt - see accounts.ING for more info
        self.accounts = pd.concat([self.up.accounts.df,self.ing.accounts.df])
        self.transactions = pd.concat([self.up.transactions.df, self.ing.transactions.df])
        self.transactions = parse_transactions(self.transactions)
        self.up = self.up.transactions.df
        self.ing = self.ing.transactions.df
        return



# %%
