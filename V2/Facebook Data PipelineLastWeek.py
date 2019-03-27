import os
import csv
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from google.cloud import bigquery

# Load environement variables
load_dotenv()

# Getting Acces ID from env_vars
my_app_id = os.environ.get("my_app_id")
my_app_secret = os.environ.get("my_app_secret")
my_access_token = os.environ.get("my_access_token")

# Initialitiong connection with Facebook
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Connecting to accounts
laSalle_account = AdAccount(os.environ.get("laSalle_AdAccount"))
interDec_account = AdAccount(os.environ.get("interDec_AdAccount"))

# Seting-Up my requests
params = {
    'date_preset': "last_week_mon_sun",
    "level": "campaign",
    'time_increment': "1"
}

fields = ["campaign_name", "reach", "clicks", "spend"]

# Requesting insights
laSalle_insights = laSalle_account.get_insights(params=params, fields=fields)
interDec_insights = interDec_account.get_insights(params=params, fields=fields)

# Transform insights into a list of dict
laSalle_insights = [dict(x) for x in laSalle_insights]
interDec_insights = [dict(x) for x in interDec_insights]

# Print result
print(laSalle_insights)
print(interDec_insights)

# Write insights into file to export data

workbook = "FacebookAdsData.csv"
path = os.getcwd()
filename = f"{path}\\{workbook}"

with open(filename, "w", newline='') as file:
    headers = ["campaign_name", "clicks", "reach", "spent", "date_start"]
    csv_writer = csv.DictWriter(file, fieldnames=headers)
    csv_writer.writeheader()
    for i in range(0, len(laSalle_insights)):
        csv_writer.writerow({
            "campaign_name": laSalle_insights[i]["campaign_name"],
            "clicks": laSalle_insights[i]["reach"],
            "reach": laSalle_insights[i]["clicks"],
            "spent": laSalle_insights[i]["spend"],
            "date_start": laSalle_insights[i]["date_start"],
            })
    for i in range(0, len(interDec_insights)):
        csv_writer.writerow({
            "campaign_name": interDec_insights[i]["campaign_name"],
            "clicks": interDec_insights[i]["reach"],
            "reach": interDec_insights[i]["clicks"],
            "spent": interDec_insights[i]["spend"],
            "date_start": interDec_insights[i]["date_start"],
            })

# Write insights into BigQueryfile
client = bigquery.Client()
dataset_id = "FacebookDataPipelineTest"
table_id = "FacebookSpentData"

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()

# The source format is CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True
job_config.write_disposition = "WRITE_TRUNCATE"

# Start writting into BigQuery
with open(filename, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_id, table_id))

# Delete the file
os.remove("FacebookAdsData.csv")
