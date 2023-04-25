# %%

from source.utilities.config import Auth as Auth
import datetime
import pandas as pd
import requests
import signal
import yaml
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
        df = df.sort_values(['id'],
                            ascending=False).reset_index(drop=True)
    return df


accounts = up_request(endpoint='accounts')

account_map = accounts.set_index('id')['attributes.displayName'].to_dict()


def read_all():
    endpoint = 'transactions'
    params = {
        'filter[status]': 'SETTLED',
        'page[size]': 100
    }
    df = up_request(endpoint=endpoint, params=params)
    df.to_parquet(r'.\cache\up\transactions.pq')
    return


def update_all():
    try:
        df = pd.read_parquet(r'.\cache\up\transactions.pq')
    except FileNotFoundError:
        print('Data unavailable for updating\nEnsure that there is data prior to updating')
    init_length = len(df)
    filter_date = df['attributes.settledAt'].max()
    print(
        f'Last Transaction settled at {filter_date}, commencing iterative read from here.')
    endpoint = 'transactions'
    params = {
        'filter[status]': 'SETTLED',
        'page[size]': 100,
        'filter[since]': f'{filter_date}'
    }
    df_iter = up_request(endpoint=endpoint, params=params)
    df = pd.concat([df_iter, df])
    df = df.drop_duplicates(['id']).reset_index(drop=True)
    final_length = len(df)
    new_lines = final_length - init_length
    if new_lines == 0:
        print('0 new transactions found.')
    else:
        print(f'{str(new_lines)} new transactions found!')
    df.to_parquet(r'.\cache\up\transactions.pq')
    return df


start = datetime.datetime.now()
try:
    df = pd.read_parquet(r'.\cache\up\transactions.pq')
    print(f'{len(df)} lines read from cached data')
except FileNotFoundError:
    print(
        "Cached read does not exist - Extracting All Settled Transactions from Up")
    read_all()
    print(f"Read completed in {datetime.datetime.now() - start}")

df = pd.concat([df, update_all()])
df.columns = df.columns.str.replace(
    '.', '_', regex=False)
df['attributes_amount_value'] = pd.to_numeric(
    df['attributes_amount_value']).copy()
df['relationships_account_data_id'] = df['relationships_account_data_id'].replace(
    account_map).copy()
df['relationships_transferAccount_data_id'] = df['relationships_transferAccount_data_id'].replace(
    account_map).copy()
df['attributes_settledAt'] = df['attributes_settledAt'].str.slice(
    0, 10).copy()  # We don't really need the granularity of datetime, date is fine
df['source'] = 'up'
df['attributes_rawText'] = df['attributes_rawText'].astype(
    'str')

cond1 = df['attributes_rawText'] == 'None'

df_desc = df[cond1].copy()
# df_desc['tags'] = df_desc['attributes_description'].replace(
#     tags['up']['attributes_description'], regex=True)

df_raw = df[~cond1].copy()
# df_raw['tags'] = df_raw['attributes_rawText'].replace(
#     tags['up']['attributes_rawText'], regex=True)

df = pd.concat([df_raw, df_desc])

cond1 = df['tags'] == None
df.loc[cond1, 'tags'] = df[cond1]['attributes_description']
