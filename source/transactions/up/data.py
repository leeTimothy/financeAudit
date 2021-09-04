
API_URL = r"https://api.up.com.au/api/v1"

# Standard Libraries
import requests
import pandas as pd

# Custom Libraries
from source.transactions.classes import Transactions as Transactions
from source.utilities.config import Auth as Auth

# Scrape Authentication
auth = Auth()
auth.load()

