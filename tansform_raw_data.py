import functions_framework
import pandas as pd
from google.cloud import storage
import os
import io

# Define bucket names and file paths
CONFIG_BUCKET_NAME = 'sdw-config-files'
DATA_BUCKET_NAME = 'sdw-sensor-data'
LOOKUP_TABLE_FILE_PATH = 'lookup_table/LOOKUP_TABLE.xlsx'

# Initialize Google Cloud Storage client
storage_client = storage.Client()

def get_lookup_table():
    """Load the lookup table from the GCS bucket."""
    bucket = storage_client.get_bucket(CONFIG_BUCKET_NAME)
    blob = bucket.blob(LOOKUP_TABLE_FILE_PATH)
    lookup_table_content = blob.download_as_bytes()
    
    # Load the Excel file into a pandas DataFrame
    lookup_df = pd.read_excel(io.BytesIO(lookup_table_content))
    print("Lookup Table loaded successfully.")
    return lookup_df

def find_sensor_id(file_name, lookup_df):
    """Find the corresponding Sensor ID from the lookup table based on the file name."""
    base_file_name = '_'.join(file_name.split('_')[:-2]).strip()  # Keep everything except the last two parts
    sensor_info = lookup_df[lookup_df['File Name'] == base_file_name]
    
    if not sensor_info.empty:
        return sensor_info.iloc[0]  # Return the entire row of sensor info
    else:
        raise ValueError(f"Sensor ID not found for file {file_name} , it does not have a valid name structure.")

def process_csv(file_content, sensor_id):
    """Process the CSV content, rename columns, clean data, and add Sensor ID."""
    # Remove any trailing semicolons in the entire file content
    file_content = '\n'.join(line.rstrip(';') for line in file_content.splitlines())

    # Load the cleaned file content into a pandas DataFrame
    data = pd.read_csv(io.StringIO(file_content), sep=';')

    # Check the number of columns
    if data.shape[1] != 2:
        raise ValueError(f"CSV file must have exactly two columns, but found {data.shape[1]} columns.")

    # Rename columns and add Sensor ID as the first column
    data.columns = ['Timestamp', 'Value']
    data.insert(0, 'Sensor ID', sensor_id['Sensor ID'])  # Insert Sensor ID as the first column

    return data

def upload_to_bucket(data, new_file_path):
    """Upload processed CSV content to the new GCS bucket location."""
    output = io.StringIO()
    data.to_csv(output, index=False)
    output.seek(0)

    # Upload to the new bucket
    bucket = storage_client.get_bucket(DATA_BUCKET_NAME)
    blob = bucket.blob(new_file_path)
    blob.upload_from_string(output.getvalue(), content_type='text/csv')

    print(f"Uploaded file to {new_file_path} successfully.")

def process_and_copy_file(file_name, bucket_name, lookup_df):
    """Process the raw file, modify it, and upload to the destination bucket."""
    try:
        print(f"Processing file: {file_name}")
        # Download the file from the source bucket
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_content = blob.download_as_text()

        # Find the sensor ID using the lookup table
        sensor_info = find_sensor_id(file_name, lookup_df)

        # Process the CSV content: rename columns, add Sensor ID, clean up
        processed_data = process_csv(file_content, sensor_info)

        # Construct the new file path in the destination bucket
        variable = sensor_info['Variable']
       
        # Remove the '.csv' extension from the file name if present
        file_name = file_name.replace('.csv', '')
        # Split the file name and check how many timestamps are present
        timestamps = file_name.split('_')[-2:]

        if len(timestamps) == 2:
            start_timestamp = timestamps[0]
            end_timestamp = timestamps[1]
        else:
            # If only one timestamp is found, use it for both start and end
            start_timestamp = end_timestamp = timestamps[0]

        # Rename file according to the desired format with the available timestamps
        if 'Yes' in sensor_info['Tank']:
            tank_type = "tank_in" if 'in' in sensor_info['Tank'] else "tank_out"
            new_file_name = f"{sensor_info['City']}_{sensor_info['District']}_{tank_type}_{variable}_{start_timestamp}_{end_timestamp}.csv"
        else:
            new_file_name = f"{sensor_info['City']}_{sensor_info['District']}_{variable}_{start_timestamp}_{end_timestamp}.csv"


        # Construct the folder structure for upload in lowercase
        folder_structure = f"{sensor_info['City'].lower()}/{sensor_info['District'].lower()}/{variable.lower()}/"

        # Upload the processed file to the destination bucket in the corresponding folder
        new_file_path = folder_structure + new_file_name
        upload_to_bucket(processed_data, new_file_path)
        print(f"File processed and uploaded successfully: {new_file_path}")

    except ValueError as e:
        print(f"Error processing file {file_name}: {e}")
    except Exception as e:
        print(f"Unexpected error for file {file_name}: {e}")

def process_sensor_data(event, context):
    """Cloud Function triggered by new files in the raw sensor data bucket."""
    file_name = event['name']
    bucket_name = event['bucket']

    # Load the lookup table
    lookup_df = get_lookup_table()

    # Process and copy the file
    process_and_copy_file(file_name, bucket_name, lookup_df)
