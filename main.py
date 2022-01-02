# %%
import yaml
import source.banks as banks

banks = banks.Amalgam()

if __name__ == '__main__':
    pass

with open(r'.\maps\tags.yaml', 'r') as file:
    tags = yaml.safe_load(file)
