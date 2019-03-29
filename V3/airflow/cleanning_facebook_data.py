import os
import pandas as pd

LOCAL_DIR = '/tmp/'


def main():
    # Locate file
    workbook = "facebook_data.csv"
    path = os.getcwd()
    filename = f"{path}\\data\\{workbook}"

    # Read the file
    facebook_data = pd.read_csv(filename + 'data_fetched.csv')

    # Format Date
    facebook_data = facebook_data.assign(date_start=pd.to_datetime(facebook_data.date_start))

    # Save in new file
    facebook_data.to_csv(filename + 'facebook_data_cleaned.csv', index=False)


if __name__ == '__main__':
    main()
