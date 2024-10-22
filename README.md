# Data Loading Google Cloud Function

This repository contains a Google Cloud Function that processes sensor data files uploaded to a Google Cloud Storage bucket. The script is written in Python and leverages the `functions_framework` for creating the cloud function and the `google-cloud-storage` library for interacting with Google Cloud Storage.

## Overview

The function is triggered whenever a new CSV file is uploaded to a specified Google Cloud Storage bucket. It performs the following tasks:

1. **Loads a Lookup Table**: Loads a lookup table from an Excel file stored in another bucket.
2. **Processes the Uploaded File**: Processes the uploaded file based on the lookup table, adding a `Sensor ID`, cleaning up the CSV file, and renaming its columns.
3. **Renames and Moves the File**: Renames the file based on sensor information and moves it to a new location in a different bucket according to the processed information.
   
### Renaming Logic

The file renaming follows these rules:
- If the sensor data is related to a tank (indicated in the lookup table by the "Tank" field):
  - The file is named as:  
    `City_District_tank_in/out_Variable_StartTimestamp_EndTimestamp.csv`
- If the sensor data is not related to a tank:
  - The file is named as:  
    `City_District_Variable_StartTimestamp_EndTimestamp.csv`

### Example

For an incoming file with the name:
```
A_004117_00002_1_MF_VITOR_PORRE_20190101102145_20240710132145.csv
```
The processed file is renamed to:
```
marene/marconi/flow/Marene_Marconi_Flow_20190101102145_20240710132145.csv
```
- **Folder structure**: `city/district/variable`
- **File name**: `City_District_Variable_StartTimestamp_EndTimestamp.csv`

## Dependencies

- `functions_framework`
- `google-cloud-storage`
- `pandas`
- `logging`
- `io`

## How It Works

1. **Lookup Table**: The lookup table is stored in the bucket `sdw-config-files` under the path `lookup_table/LOOKUP_TABLE.xlsx`. The table includes details such as file name, city, district, tank type, variable (e.g., Flow, Pressure), and `Sensor ID`.
   
2. **Triggering**: The function is triggered when a new file is uploaded to the designated "raw sensor data" bucket.

3. **Processing**: The function processes the file:
   - Loads the file and finds matching sensor information from the lookup table.
   - Cleans the CSV file by removing any trailing semicolons and renaming columns to `Timestamp` and `Value`.
   - Inserts the `Sensor ID` as the first column.
   
4. **File Upload**: The processed file is renamed, and both start and end timestamps are included in the file name. It is then uploaded to the `sdw-sensor-data` bucket under the appropriate folder structure.

