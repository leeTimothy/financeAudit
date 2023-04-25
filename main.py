# %%
import locale
import yaml
import source.banks as banks

banks = banks.Amalgam()

if __name__ == '__main__':
    pass

with open(r'.\maps\tags.yaml', 'r') as file:
    tags = yaml.safe_load(file)
# Delta to Savings for Deposit
banks.df.groupby(['month_year'])['attributes_amount_value'].sum()

# Total Liability - Total Funds
# excess = 570000*0.15 + (615000-570000) - (615000*0.05) - \
# banks.accounts.attributes_balance_value.sum()
# excess = excess * -1
# excess = locale.currency(excess, grouping=True)
# print("\nStance after Apartment: $ {:0,.2f}".format(excess))
