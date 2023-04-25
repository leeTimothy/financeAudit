from source.utilities.config import Auth as Auth
import yaml
import signal
import requests
import pandas as pd
API_URL = r"https://api.up.com.au/api/v1"
METHOD = 'get'


def up_request(endpoint: str, params: dict = None):
    if endpoint == params == None:
        signal.SIGBREAK
        return
    auth = Auth()
    df = pd.DataFrame()
    for i in auth.df.index:
        data = requests.get(
            url=API_URL+r'/{}'.format(endpoint),
            headers={'Authorization': 'Bearer {}'.format(auth.df.iloc[i, 1])},
            params=params).json()
        df_iter = pd.json_normalize(data['data'])
        df_iter['source'] = auth.df.iloc[i]['name']
        df = pd.concat([df_iter, df])

        more_data = data['links']['next'] != None

        while more_data:
            url = data['links']['next']
            data = requests.get(
                url=url,
                headers={
                    'Authorization': 'Bearer {}'.format(auth.df.iloc[i, 1])
                },
                params=params).json()
            df_iter = pd.json_normalize(data['data'])
            df_iter['source'] = auth.df.iloc[i]['name']
            df = pd.concat([df_iter, df])
            df = df.reset_index(drop=True)
            more_data = data['links']['next'] != None
            df = df.drop_duplicates(['id']).reset_index(drop=True)
            df = df.sort_values(['attributes.createdAt'],
                                ascending=False).reset_index(drop=True)
    return df


accounts = up_request(endpoint='accounts')
