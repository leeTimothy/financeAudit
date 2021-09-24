#%%
import pandas as pd
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

from source import accounts
from source.utilities.amalgam.functions import parse_transactions

up = accounts.Up(amalgam=True)
up.transactions.df['source'] = 'up'
ing = accounts.ING(amalgam=True) # Assumed all values within are provided at settledAt rather than createdAt - see accounts.ING for more info
ing.transactions.df['source'] = 'ing'

accounts = pd.concat([up.accounts.df,ing.accounts.df])

transactions = pd.concat([up.transactions.df, ing.transactions.df])
transactions = parse_transactions(transactions)



# %%
