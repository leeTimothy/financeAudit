import pandas as pd

class Transactions(object):
    def __init__(self, path=None, accountHolder=None):
        self.raw = pd.DataFrame()
        self.df = pd.DataFrame()
        self.accountHolder = accountHolder
        self.path = path
        return

