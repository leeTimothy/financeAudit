API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'

import pandas as pd
import requests

from source.utilities.config import Auth as Auth

def up_request(endpoint: str=None, params: dict=None):
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
    return df