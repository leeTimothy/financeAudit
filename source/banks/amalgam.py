#%%
import pandas as pd
import plotly as 

from source import accounts

up = accounts.Up(amalgam=True)
ing = accounts.ING(amalgam=True) # Assumed all values within are provided at settledAt rather than createdAt - see accounts.ING for more info


accounts = pd.concat([up.accounts.df,ing.accounts.df])
transactions = pd.concat([up.transactions.df, ing.transactions.df])
transactions = transactions.sort_values('attributes_settledAt', ascending = False).reset_index(drop=True)


# %%
# TODO - 