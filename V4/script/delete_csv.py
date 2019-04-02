import os

LOCAL_DIR = '/tmp/'


def main():
    # Locate file
    workbook = "facebook_data.csv"
    path = os.getcwd()
    filename = f"{path}\\data\\{workbook}"

    # Delete the cvs file
    os.remove(filename)


if __name__ == '__main___':
    main()
