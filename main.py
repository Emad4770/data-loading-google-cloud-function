import functions_framework
from google.cloud import storage
import pandas as pd
import logging
import io

# Initialize Google Cloud Storage client
storage_client = storage.Client()

# Load the lookup table from the Excel file
def load_lookup_table():
    bucket_name = "sdw-config-files"
    blob_name = "lookup_table/LOOKUP_TABLE.xlsx"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Download the blob as a byte stream
    data = blob.download_as_bytes()
    # Read the Excel file into a DataFrame
    df = pd.read_excel(io.BytesIO(data))
    
    # Convert DataFrame to a list of dictionaries
    return df.to_dict(orient='records')

# Triggered by a change in the storage bucket
@functions_framework.cloud_event
def process_sensor_data(cloud_event):
    try:
        lookup_table = load_lookup_table()  # Load lookup table from Excel

        data = cloud_event.data
        event_id = cloud_event["id"]
        event_type = cloud_event["type"]

        bucket = data["bucket"]
        file_name = data["name"]

        print(f"Event ID: {event_id}")
        print(f"Event type: {event_type}")
        print(f"Bucket: {bucket}")
        print(f"File: {file_name}")

        # Get the source bucket and blob (file)
        raw_bucket = storage_client.bucket(bucket)
        blob = raw_bucket.blob(file_name)

        # Strip the .csv extension for lookup purposes
        base_file_name = file_name.replace(".csv", "")

        # Lookup file name in the table
        match = next((item for item in lookup_table if item['File Name'] in base_file_name), None)

        if match:
            try:
                # Extract the timestamp from the original file name (the last two segments)
                parts = base_file_name.split('_')
                timestamp = parts[-2] + '_' + parts[-1]  # Assumes timestamp is in the last two segments

                # Construct the destination path based on the city, district, and variable
                city = match['City'].lower()
                district = match['District'].lower()  # Spaces will not be replaced with underscores
                variable = match['Variable'].lower()

                # Rename file according to the desired format with the original timestamp
                if 'Yes' in match['Tank']:
                    # Add tank info if applicable
                    tank = "tank_in" if 'in' in match['Tank'] else "tank_out"
                    new_file_name = f"{city}_{district}_{tank}_{variable}_{timestamp}.csv"
                else:
                    new_file_name = f"{city}_{district}_{variable}_{timestamp}.csv"

                # Destination bucket and path
                destination_bucket_name = "sdw-sensor-data"
                destination_path = f"{city}/{district}/{variable}/{new_file_name}"

                # Get destination bucket
                destination_bucket = storage_client.bucket(destination_bucket_name)

                # Copy the file to the new destination
                new_blob = raw_bucket.copy_blob(blob, destination_bucket, destination_path)

                print(f"Successfully copied {file_name} to {destination_path}")

            except Exception as e:
                # Handle any issues related to copying the file
                logging.error(f"Failed to copy file {file_name} to {destination_path}: {str(e)}")

        else:
            # File not found in the lookup table
            logging.error(f"File {file_name} not found in the lookup table.")
        
    except KeyError as e:
        # Handle missing expected fields in cloud_event
        logging.error(f"KeyError - missing field: {str(e)}")

    except Exception as e:
        # Handle other unforeseen errors
        logging.error(f"An error occurred: {str(e)}")
