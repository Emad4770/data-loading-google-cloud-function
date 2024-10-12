import functions_framework

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]  # Full path including folder structure and file name
    metageneration = data["metageneration"]
    time_created = data["timeCreated"]
    updated = data["updated"]

    # Split the full name by '/'
    path_parts = name.split('/')
    
    # Extract city, district, and sensor type from the folder structure
    city = path_parts[0]  # First folder (e.g., marene)
    district = path_parts[1]  # Second folder (e.g., marconi)
    sensor_type = path_parts[2]  # Third folder (e.g., flow or pressure)
    
    # Generate a unique ID based on folder structure (city/district/sensor_type)
    unique_id = f"{city}/{district}/{sensor_type}"

    # Extract file name (last part of the path)
    file_name = path_parts[-1]

    # Split the file name by underscores (as before) to extract additional info
    file_parts = file_name.split('_')
    
    # Extract location and dates (optional, depending on your use case)
    location = '_'.join(file_parts[:-2])  # Everything before the date parts
    start_date = file_parts[-2]  # The second last part is the start date
    end_date = file_parts[-1].split('.')[0]  # Last part before the extension is the end date


    print(f"Processing {sensor_type} data for city: {city}, district: {district}")
    print(f"id: {unique_id}")
    print(f"Location: {location}")
    print(f"Start Date: {start_date} | End Date: {end_date}")
    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {time_created}")
    print(f"Updated: {updated}")
