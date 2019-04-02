import os
import csv
import ssl
import smtplib
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from google.cloud import bigquery
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


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
    headers = ["campaign_name", "clicks", "reach", "Spent", "Date"]
    csv_writer = csv.DictWriter(file, fieldnames=headers)
    csv_writer.writeheader()
    for i in range(0, len(laSalle_insights)):
        csv_writer.writerow({
            "campaign_name": laSalle_insights[i]["campaign_name"],
            "clicks": laSalle_insights[i]["reach"],
            "reach": laSalle_insights[i]["clicks"],
            "Spent": laSalle_insights[i]["spend"],
            "Date": laSalle_insights[i]["date_start"],
            })
    for i in range(0, len(interDec_insights)):
        csv_writer.writerow({
            "campaign_name": interDec_insights[i]["campaign_name"],
            "clicks": interDec_insights[i]["reach"],
            "reach": interDec_insights[i]["clicks"],
            "Spent": interDec_insights[i]["spend"],
            "Date": interDec_insights[i]["date_start"],
            })

# Write insights into BigQueryfile
# Connect to client
credentials = os.environ.get("bigquery_credentials")
client = bigquery.Client.from_service_account_json(
    credentials)
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

# Send email with result
port = 465  # For SSL
password = os.environ.get("mdp_mail")
context = ssl.create_default_context()
sender_email = "lci.automated.report@gmail.com"
receiver_email = "Benjamin.Marquis@lcieducation.com"

message = MIMEMultipart("alternative")
message["Subject"] = "multipart test"
message["From"] = sender_email
message["To"] = receiver_email

html = f"""
<html>
      <body>
        <table width=800 cellpadding=0 border=0 cellspacing=0>
          <tr>
            <td colspan=2 align=right>
              <div style='font: italic normal 10pt Times New Roman, serif
                  margin: 0; color: #666; padding-right: 5px;>
                  Powered by Airflow</div>
            </td>
          </tr>
          <tr bgcolor=#3c78d8>
            <td width=500>
              <div style=font: normal 18pt verdana, sans-serif;
              padding: 3px 10px; color: white>Ads data load to
              Bigquery report</div>
            </td>
            <td align=right>
              <div style='font: normal 18pt verdana, sans-serif
              padding: 3px 10px; color: white>
            </tr>
          </table>
          <table width=800 cellpadding=0 border=1 cellspacing=0>
            <tr bgcolor=#ddd>
              <td style=font: 12pt verdana, sans-serif
                  padding: 5px 0px 5px 5px; background-color: #ddd
                  text-align: left>Report</td>
              <td style=font: 12pt verdana, sans-serif
                  padding: 5px 0px 5px 5px; background-color: #ddd
                  text-align: left>JobId</td>
              <td style=font: 12pt verdana, sans-serif
                  padding: 5px 0px 5x 5px; background-color: #ddd
                  text-align: left>Rows</td>
              <td style=font: 12pt verdana, sans-serif
                  padding: 5px 0px 5x 5px; background-color: #ddd
                  text-align: left'>State</td>
              <td style=font: 12pt verdana, sans-serif
                  padding: 5px 0px 5x 5px; background-color: #ddd
                  text-align: left>ErrorResult</td>
            </tr>
            {job.output_rows}
        </table>
      </body>
    </html>
    """
part1 = MIMEText(html, "html")

message.attach(part1)

with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(
        sender_email, receiver_email, message.as_string()
    )


# Delete the file
os.remove("FacebookAdsData.csv")
