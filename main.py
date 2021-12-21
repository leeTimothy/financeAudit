# %%
import yaml
import source.banks as banks

banks = banks.Amalgam()
monthlySummary = banks.df.groupby(
    'month_year')['attributes_amount_value'].sum().reset_index()

if __name__ == '__main__':
    pass


with open(r'.\maps\tags.yaml', 'r') as file:
    tags = yaml.safe_load(file)

matches = banks.ing[banks.ing['attributes_rawText'] != banks.ing.tags].copy()
check = banks.ing[banks.ing['attributes_rawText'] == banks.ing.tags].copy()
check.drop('tags', axis=1, inplace=True)
check['tags'] = check['attributes_rawText'].str.extract(
    '(.*?) - .*? Purchase.*')
