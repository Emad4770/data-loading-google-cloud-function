import pandas as pd
from google.cloud import bigquery, storage
import io

# Initialize Google Cloud clients
bigquery_client = bigquery.Client()
storage_client = storage.Client()

def upload_to_bigquery(data, table_id):
    """Upload processed data to BigQuery."""
    try:
        # Ensure that the DataFrame columns remain as 'Sensor ID', 'Timestamp', 'Value'
        data.columns = ['Sensor ID', 'Timestamp', 'Value']

        # Convert the 'Timestamp' column to datetime format for BigQuery compatibility
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], errors='coerce')

        # Load data into BigQuery table
        job = bigquery_client.load_table_from_dataframe(data, table_id)

        # Wait for the job to complete
        job.result()  # Blocks until the job finishes
        print(f"Data successfully uploaded to BigQuery table: {table_id}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

def process_and_upload_to_bq(file_name, bucket_name):
    """Process the CSV file and upload it to the appropriate BigQuery table."""
    try:
        print(f"Processing file: {file_name} from bucket: {bucket_name}")
        
        # Extract the variable (flow/pressure/level) from the folder structure
        variable = file_name.split('/')[-2].lower()  # Extract the variable from the folder structure (flow, pressure, or level)
        table_id = f"smart-digital-water.sensor_measurement.{variable}"

        # Download the file from Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_content = blob.download_as_text()

        # Load the CSV content into a pandas DataFrame
        data = pd.read_csv(io.StringIO(file_content), sep=',')

        # Verify the columns and structure
        if data.shape[1] != 3:
            raise ValueError(f"CSV file {file_name} must have exactly three columns (Sensor ID, Timestamp, Value).")

        # Upload the processed data to BigQuery
        upload_to_bigquery(data, table_id)

    except Exception as e:
        print(f"Error processing or uploading file {file_name}: {e}")

def process_sensor_data(event, context):
    """Cloud Function triggered by new files in the raw sensor data bucket."""
    file_name = event['name']
    bucket_name = event['bucket']

    # Process and upload the file to BigQuery
    process_and_upload_to_bq(file_name, bucket_name)
