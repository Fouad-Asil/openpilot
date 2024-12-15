import json
import os
import csv
import datetime
from openpilot.tools.lib.logreader import LogReader
from collections import defaultdict
from opendbc.car.car_helpers import get_car_interface, CarParams,gen_empty_fingerprint  # Import the function
from opendbc.car import structs

# Path to the database.json file
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'

# Load the JSON data
with open(database_json_path, 'r') as file:
    database = json.load(file)

# Base path where the log files are stored
base_path = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'

def format_timestamp(nanoseconds):
    seconds = nanoseconds / 1e9
    return datetime.datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S.%f')

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    for log_file in log_files:
        segment_path = os.path.join(base_path, log_file)[:-2]
        print(f'  Processing segment directory: {segment_path}')

        # Define the path to the rlog.bz2 file
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

                    # Initialize CarParams
                    CP = CarParams()
                    CP.carFingerprint = car_model.upper().replace(' ', '_')
                    # CP.fingerprint = gen_empty_fingerprint()

                    # Get the CarInterface
                    Car_Interface = get_car_interface(CP)

                    # Initialize the car interface
                    # car_interface = CarInterface(CP, None, None)

                    # Dictionary to aggregate messages by timestamp
                    messages_by_timestamp = defaultdict(list)

                    # Iterate through messages in the log
                    for msg in lr:
                        if msg.which() == 'can':
                            for can_msg in msg.can:
                                decoded_msg = Car_Interface.decode_can_message(can_msg)
                                if decoded_msg:
                                    timestamp = format_timestamp(msg.logMonoTime)
                                    messages_by_timestamp[timestamp].append(decoded_msg)

                    # Write the aggregated data to the CSV
                    for timestamp, message_details in messages_by_timestamp.items():
                        csv_writer.writerow([timestamp, len(message_details), " || ".join(message_details)])

                print(f"Data has been exported to {output_csv_path}")

            except Exception as e:
                print(f"Error processing file {rlog_path}: {e}")
        else:
            print(f"rlog.bz2 not found in {segment_path}")