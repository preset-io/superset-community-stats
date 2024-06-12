import json
import pandas as pd
from shillelagh.backends.apsw.db import connect
from shillelagh.adapters.api.gsheets import GSheetsAPI
import os
import json

# Load the Apache repos data
with open('data/apache_repos.json') as f:
    apache_repos_data = json.load(f)

# Load another data
with open('data/another_data.json') as f:
    another_data = json.load(f)

# Process the data (example: convert to DataFrames and merge)
apache_repos_df = pd.DataFrame(apache_repos_data)
another_data_df = pd.DataFrame(another_data)

# Example processing: Merge DataFrames
merged_df = pd.merge(apache_repos_df, another_data_df, how='outer')

# Further processing as needed
# ...

# Authenticate with Google Sheets using the service account key
service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
gsheets_adapter = GSheetsAPI(service_account_info=service_account_info)

# Connect to Google Sheets
connection = connect(":memory:", adapters=["gsheetsapi"], adapter_kwargs={"gsheetsapi": {"service_account_info": service_account_info}})
cursor = connection.cursor()

# Define the Google Sheets URL and sheet names
apache_repos_sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit#gid=0"
another_data_sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit#gid=1"
merged_data_sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SPREADSHEET_ID/edit#gid=2"

# Write the data to Google Sheets
apache_repos_df.to_sql("Sheet1", connection, if_exists="replace", index=False)
another_data_df.to_sql("Sheet2", connection, if_exists="replace", index=False)
merged_df.to_sql("Sheet3", connection, if_exists="replace", index=False)

print("Data has been written to Google Sheets.")
