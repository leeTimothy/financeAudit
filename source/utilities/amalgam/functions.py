import pandas as pd

def parse_transactions(df):
    df = df.sort_values('attributes_settledAt', ascending = False).reset_index(drop=True)
    df['month'] = df['attributes_settledAt'].dt.month
    df['year'] = df['attributes_settledAt'].dt.year
    df['month_year'] = df['attributes_settledAt'].dt.to_period('M')
    return df