# %%
import source.banks.transactions as trans
import pandas as pd
# pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000


class Amalgam(object):
    def __init__(self):
        self.up = trans.Up(amalgam=True)
        # Assumed all values within are provided at settledAt rather than createdAt - see accounts.ING for more info
        self.ing = trans.ING(amalgam=True)
        self.accounts = pd.concat([self.up.accounts.df, self.ing.accounts.df])
        self.df = pd.concat(
            [self.up.transactions.df, self.ing.transactions.df])
        self.df = tag_transactions(self.df)
        self.df = parse_transactions(self.df)
        self.up = self.up.transactions.df
        self.ing = self.ing.transactions.df
        self.summary = self.summary()
        self.rate = self.rate()
        # self.income = self.income()
        return

    def summary(self):
        summary_df = self.df.groupby(['month_year'])[
            'attributes_amount_value'].sum()
        return summary_df

    def rate(self):
        rate = self.df.groupby(['month_year'])[
            ['Credit', 'Debit']].sum().copy()
        rate['delta'] = rate['Credit'] + rate['Debit']
        return rate

    # def income(self):
    #     income_df = self.df[self.df.]


def parse_transactions(df):
    df = df.sort_values('attributes_settledAt',
                        ascending=False).reset_index(drop=True)
    df['month'] = df['attributes_settledAt'].dt.month
    df['year'] = df['attributes_settledAt'].dt.year
    df['month_year'] = df['attributes_settledAt'].dt.to_period('M')
    for column in df:  # Drop all empty columns
        if df[column].dropna().empty:
            df.drop(column, axis=1, inplace=True)
    return df


def tag_transactions(df):
    return df


# %%
