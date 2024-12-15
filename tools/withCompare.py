import json
import os
import csv
import datetime
from collections import defaultdict
import cantools
from openpilot.tools.lib.logreader import LogReader
from fuzzywuzzy import process
import re

# Paths
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'  # Update with your actual path
base_path = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'  # Update with your actual path
dbc_directory = '/path/to/opendbc'  # Update with your actual path to the opendbc repo

# Load database.json
with open(database_json_path, 'r') as file:
    database = json.load(file)

# Extract car models
car_models = list(database.keys())

# Function to standardize names
def standardize_name(name):
    name = name.lower()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# Standardize car models
standardized_car_models = [standardize_name(model) for model in car_models]

# List all DBC files and standard__import__(f'opendbc.car.{brand_name}.{INTERFACE_ATTR_FILE.get(attr, "values")}', fromlist=[attr])ize their names
dbc_files = []
for root, dirs, files in os.walk(dbc_directory):
    for file in files:
        if file.endswith('.dbc'):
            dbc_files.append(os.path.join(root, file))

dbc_name_to_path = {}
for dbc_path in dbc_files:
    filename = os.path.basename(dbc_path)
    name = os.path.splitext(filename)[0]
    standardized_name = standardize_name(name)
    dbc_name_to_path[standardized_name] = dbc_path

# Map car models to DBC files using fuzzy matching
car_model_to_dbc = {}

for model in car_models:
    standardized_model = standardize_name(model)
    choices = list(dbc_name_to_path.keys())
    match, score = process.extractOne(standardized_model, choices)
    if score >= 80:  # Adjust threshold as needed
        dbc_path = dbc_name_to_path[match]
        car_model_to_dbc[model] = dbc_path
    else:
        print(f"No good match for car model: {model}")

def format_timestamp(nanoseconds):
    seconds = nanoseconds / 1e9
    return datetime.datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S.%f')

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    # Get DBC file path from automated mapping
    dbc_file_path = car_model_to_dbc.get(car_model)
    if not dbc_file_path or not os.path.isfile(dbc_file_path):
        print(f"DBC file not found for car model {car_model}")
        continue
    db = cantools.database.load_file(dbc_file_path)

    for log_file in log_files:
        # Construct the segment path
        segment_path = os.path.join(base_path, log_file)
        if not os.path.isdir(segment_path):
            print(f"Segment directory not found: {segment_path}")
            continue

        # Path to the rlog.bz2 file
        rlog_path = os.path.join(segment_path, 'rlog.bz2')

        if os.path.isfile(rlog_path):
            # Define the output CSV file path
            output_csv_path = os.path.join(segment_path, 'output_data.csv')

            try:
                # Open the CSV file for writing
                with open(output_csv_path, mode='w', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file)

                    # Write the header row
                    csv_writer.writerow(['Timestamp', 'Message Count', 'Message Details'])

                    # Initialize LogReader with the rlog.bz2 file
                    lr = LogReader(rlog_path)

                    # Dictionary to aggregate messages by timestamp
                    messages_by_timestamp = defaultdict(list)

                    # Iterate through messages in the log
                    for msg in lr:
                        if msg.which() == 'can':
                            for can_msg in msg.can:
                                try:
                                    # Decode the CAN message
                                    decoded = db.decode_message(can_msg.address, can_msg.dat)
                                    decoded_str = ', '.join(f"{k}: {v}" for k, v in decoded.items())
                                    timestamp = format_timestamp(msg.logMonoTime)
                                    messages_by_timestamp[timestamp].append(decoded_str)
                                except (KeyError, cantools.database.errors.Error):
                                    # Message ID not defined in DBC or decode error
                                    continue

                    # Write the aggregated data to the CSV
                    for timestamp, message_details in messages_by_timestamp.items():
                        csv_writer.writerow([timestamp, len(message_details), " || ".join(message_details)])

                print(f"Data has been exported to {output_csv_path}")

            except Exception as e:
                print(f"Error processing file {rlog_path}: {e}")
        else:
            print(f"rlog.bz2 not found in {segment_path}")
