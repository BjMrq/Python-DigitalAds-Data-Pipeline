import os
from google.cloud import bigquery

LOCAL_DIR = '/tmp/'


def main():
    # Locate file
    workbook = "facebook_data.csv"
    path = os.getcwd()
    filename = f"{path}\\data\\{workbook}"

    # Write insights into BigQueryfile
    credentials = os.environ.get("bigquery_credentials")
    client = bigquery.Client.from_service_account_json(credentials)
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


if __name__ == '__main___':
    main()
