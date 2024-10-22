# Data Loading and Transformation for Google Cloud Functions

This repository contains two Google Cloud Functions: one for processing and transforming sensor data files uploaded to a Google Cloud Storage bucket (`transform_raw_data.py`), and another for moving the processed data to BigQuery (`move_to_bq.py`). Both scripts are written in Python and utilize the `functions_framework` for creating cloud functions, along with other Google Cloud libraries for storage and data interaction.

## Overview

### 1. **Transform Raw Data (transform_raw_data.py)**

This function is triggered whenever a new CSV file is uploaded to a specified Google Cloud Storage bucket. It performs the following tasks:

- **Loads a Lookup Table**: Loads a lookup table from an Excel file stored in a separate bucket.
- **Processes the Uploaded File**: Matches the file with metadata (e.g., city, district, sensor type) from the lookup table.
- **Renames and Moves the File**: Renames the file based on metadata and moves it to an organized folder structure in a different bucket.

#### Renaming Logic

- If the sensor data is related to a tank:
  - The file is named as:  
    `City_District_tank_in/out_Variable_StartTimestamp_EndTimestamp.csv`
- If the sensor data is not related to a tank:
  - The file is named as:  
    `City_District_Variable_StartTimestamp_EndTimestamp.csv`

#### Example

Incoming file name:
```
A_004117_00002_1_MF_VITOR_PORRE_20190101102145_20240710132145.csv
```

Renamed and moved file path:
```
marene/marconi/flow/Marene_Marconi_Flow_20190101102145_20240710132145.csv
```

- **Folder structure**: `city/district/variable`
- **File name**: `City_District_Variable_StartTimestamp_EndTimestamp.csv`

### 2. **Move Data to BigQuery (move_to_bq.py)**

After the file has been processed and moved to the new storage bucket, this function moves the data into BigQuery for further analysis.

- **Ingest Data**: Reads the cleaned and organized CSV file from Google Cloud Storage.
- **Load Data to BigQuery**: Uploads the file to specific tables in BigQuery (e.g., `flow`, `pressure`, `level`) based on the type of sensor data. 
- **Error Handling**: If the data format or file structure is incorrect, it logs the issue for debugging.

#### Example Workflow

- The function automatically determines which BigQuery table to upload the data to based on the folder structure of the file:
  - `flow` data goes to the table `sensor_measurement.flow`
  - `pressure` data goes to the table `sensor_measurement.pressure`
  - `level` data goes to the table `sensor_measurement.level`
  
- It ensures the file has exactly three columns: `Sensor ID`, `Timestamp`, and `Value`.

### Example Processed File:

| Sensor ID | Timestamp           | Value |
|-----------|---------------------|-------|
| MAR_SAP_FLW       | 2023/08/15 12:30:00 | 10.5  |
| MAR_SAP_FLW       | 2023/08/15 12:45:00 | 11.2  |

### 3. **Error Handling**

Both functions contain error handling mechanisms that log errors in case of invalid file names, incorrect file structure, or BigQuery upload failures. These logs can be accessed through Google Cloud Logging for debugging.

## File Structure

- **transform_raw_data.py**: Responsible for processing raw sensor data, renaming files, and organizing them into structured folders in the target bucket.
- **move_to_bq.py**: Responsible for moving the processed data to BigQuery, ingesting it into the appropriate table based on the sensor type.

## Dependencies

Both scripts use the following libraries:

```
functions-framework==3.*
pandas
google-cloud-storage
google-cloud-bigquery
pyarrow
openpyxl
```

## Workflow

1. **File Upload**: A new CSV file is uploaded to the raw data bucket.
   
2. **Transform Raw Data**:
   - Triggered when the file is uploaded.
   - Processes the file, renames it based on the metadata from the lookup table, and moves it to the correct folder in the target bucket.

3. **Move Data to BigQuery**:
   - Triggered when a new file is uploaded to the organized folder bucket.
   - Ingests the file into the corresponding BigQuery table (e.g., `flow`, `pressure`, `level`).

## Example Workflow

1. **Raw File Upload**:
   - File: `A_004117_00002_1_MF_VITOR_PORRE_20190101102145_20240710132145.csv`
   - Bucket: `raw-sensor-data`
   
2. **Transformed File**:
   - Renamed to: `Marene_Marconi_Flow_20190101102145_20240710132145.csv`
   - Bucket: `sdw-sensor-data/marene/marconi/flow`

3. **Moved to BigQuery**:
   - Table: `sensor_measurement.flow`
   - Dataset: `sensor_measurement`

