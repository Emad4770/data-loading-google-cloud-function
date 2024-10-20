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

