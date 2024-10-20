# Data Loading Google Cloud Function

This repository contains a Google Cloud Function that processes sensor data files uploaded to a Google Cloud Storage bucket. The script is written in Python and leverages the `functions_framework` for creating the cloud function and the `google-cloud-storage` library for interacting with Google Cloud Storage.

## Overview

The function is triggered whenever a new CSV file is uploaded to a specified Google Cloud Storage bucket. It performs the following tasks:

1. Loads a lookup table from an Excel file stored in another bucket.
2. Processes the uploaded file based on the lookup table.
3. Renames and moves the file to a new location in a different bucket according to the processed information.

## Dependencies

- `functions_framework`
- `google-cloud-storage`
- `pandas`
- `logging`
- `io`

## Setup

### Prerequisites

- A Google Cloud project with billing enabled.
- Google Cloud SDK installed and configured.
- Required GCP permissions to access Cloud Storage.

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/Emad4770/data-loading-google-cloud-function.git
   cd data-loading-google-cloud-function
Install the required Python packages:
pip install -r requirements.txt
Function Details
main.py
The main script contains the following key components:

Google Cloud Storage Client Initialization:

storage_client = storage.Client()
Load Lookup Table:
Loads an Excel file from a bucket and converts it into a list of dictionaries.

def load_lookup_table():
    bucket_name = "sdw-config-files"
    blob_name = "lookup_table/LOOKUP_TABLE.xlsx"
    ...
Cloud Event Function:
This function is triggered by changes in the storage bucket.

@functions_framework.cloud_event
def process_sensor_data(cloud_event):
    ...
Process Flow
Trigger: The function is triggered by an event in the storage bucket.
Load Lookup Table: The function loads a lookup table from an Excel file.
Process File: Based on the lookup table, the function processes the uploaded file.
Rename and Move: The function renames the file and moves it to a new location in another bucket.
Error Handling
The function includes error handling for missing fields in the cloud event and issues related to copying files.

Usage
Deploy the function to Google Cloud Functions using the following command:

gcloud functions deploy process_sensor_data --runtime python39 --trigger-resource YOUR_TRIGGER_BUCKET --trigger-event google.storage.object.finalize
Replace YOUR_TRIGGER_BUCKET with the name of your bucket that will trigger the function.

License
This project is licensed under the MIT License. See the LICENSE file for details.

Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
