API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'

import pandas as pd
import requests
import signal
import yaml

from source.utilities.config import Auth as Auth

# Import Configs
with open('.\maps\ing_account.yaml', 'r') as file:
    ing_map = yaml.safe_load(file)

def up_request(endpoint: str, params: dict=None): #TODO - this method currently looks at it from an auth bearer point of view - but you should make this across the board account # specific.
    if endpoint == params == None:
        signal.SIGBREAK
        return 
    auth = Auth()
    df = pd.DataFrame()
    for i in auth.df.index:
        data = requests.get(
            url = API_URL+r'/{}'.format(endpoint), 
            headers = {'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])},
            params = params).json()
        df_iter = pd.json_normalize(data['data'])
        df_iter['source'] = auth.df.iloc[i]['name']
        df = pd.concat([df_iter,df])

        more_data = data['links']['next']!=None
        while more_data:
            url = data['links']['next']
            data = requests.get(
                url = url,
                headers = {
                    'Authorization': 'Bearer {}'.format(auth.df.iloc[i,1])
                    },
                params = params).json()
            df_iter = pd.json_normalize(data['data'])
            df_iter['source'] = auth.df.iloc[i]['name']
            df = pd.concat([df_iter, df])
            df = df.reset_index(drop=True)
            more_data = data['links']['next']!=None
    df = df.drop_duplicates(['id']).reset_index(drop=True)
    df = df.sort_values(['attributes.createdAt'], ascending = False).reset_index(drop=True)
    return df

def ing_parse(df):
    df['Credit'] = pd.to_numeric(df['Credit']).fillna(0)
    df['Debit'] = pd.to_numeric(df['Debit']).fillna(0)
    df['Total'] = df[['Credit','Debit']].sum(axis=1)
    df['id'] = df['Account'].copy()
    df['Account'] = df['Account'].replace(ing_map)
    return df
