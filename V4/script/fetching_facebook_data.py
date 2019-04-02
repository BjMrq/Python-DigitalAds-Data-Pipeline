import os
import csv
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Load environement variables
load_dotenv()

LOCAL_DIR = '/tmp/'


def main():
    # Getting Acces ID from env_vars
    my_app_id = os.environ.get("my_app_id")
    my_app_secret = os.environ.get("my_app_secret")
    my_access_token = os.environ.get("my_access_token")

    # Initialitiong connection with Facebook
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

    # Connecting to accounts
    campus_account = AdAccount(os.environ.get("laSalle_AdAccount"))

    # Seting-Up my requests
    params = {
        'date_preset': "last_week_mon_sun",
        "level": "campaign",
        'time_increment': "1"
    }

    fields = ["campaign_name", "reach", "clicks", "spend"]

    # Requesting insights
    campus_insights = campus_account.get_insights(params=params, fields=fields)

    # Transform insights into a list of dict
    campus_insights = [dict(x) for x in campus_insights]

    # Print result
    print(campus_insights)

    # Write insights into file to export data
    # Locate file
    workbook = "facebook_data.csv"
    path = os.getcwd()
    filename = f"{path}\\data\\{workbook}"

    with open(filename, "w", newline='') as file:
        headers = ["campaign_name", "clicks", "reach", "spent", "date_start"]
        csv_writer = csv.DictWriter(file, fieldnames=headers)
        csv_writer.writeheader()
        for i in range(0, len(campus_insights)):
            csv_writer.writerow({
                "campaign_name": campus_insights[i]["campaign_name"],
                "clicks": campus_insights[i]["reach"],
                "reach": campus_insights[i]["clicks"],
                "spent": campus_insights[i]["spend"],
                "date_start": campus_insights[i]["date_start"],
                })


if __name__ == '__main___':
    main()
