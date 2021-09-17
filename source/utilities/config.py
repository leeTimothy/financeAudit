import json
import pandas as pd
import yaml

class Auth(object):
    def __init__(self):
        self.df = pd.DataFrame()
        with open(".\source\keys\key.yaml", 'r') as stream:
            try:
                config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        if len(config)!=0:
            self.df = pd.read_json(json.dumps(config)).reset_index()
            self.df = self.df.rename(
                columns={
                    'index': 'name'
                }
            )
        return

if __name__ == "__main__":
    pass