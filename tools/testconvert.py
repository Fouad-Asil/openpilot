import json
import os
import csv
import datetime


from openpilot.tools.lib.logreader import LogReader
import capnp
from collections import defaultdict

# Path to the database.json file
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'

# Load the JSON data
with open(database_json_path, 'r') as file:
    database = json.load(file)

# Base path where the log files are stored
base_path = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'

# Function to convert nanoseconds to a readable time format
def format_timestamp(nanoseconds):
    seconds = nanoseconds / 1e9
    return datetime.datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S.%f')

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    for log_file in log_files:
        segment_path = os.path.join(base_path, log_file)[:-2]  # Adjusted path slicing
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

                    # Dictionary to aggregate messages by timestamp
                    messages_by_timestamp = defaultdict(list)

                    # Iterate through messages in the log
                    for msg in lr:
                        try:
                            # Check if the message has the 'which' method and is callable
                            if hasattr(msg, 'which') and callable(getattr(msg, 'which')):
                                msg_type = msg.which()
                                msg_content = getattr(msg, msg_type, None)
                            else:
                                # Handle non-union types separately
                                msg_type = 'non_union'
                                msg_content = msg

                            timestamp = format_timestamp(msg.logMonoTime)
                            details = [f"Type: {msg_type}"]

                            if msg_content:
                                # Check if the message content is a list
                                if isinstance(msg_content, capnp.lib.capnp._DynamicListReader):
                                    for i, element in enumerate(msg_content):
                                        element_details = [f"Element {i + 1}:"]
                                        for field in dir(element):
                                            if not field.startswith("_"):  # Skip private attributes
                                                field_value = getattr(element, field)
                                                element_details.append(f"{field}: {field_value}")
                                        details.append(", ".join(element_details))
                                else:
                                    # Iterate over fields if the content is not a list
                                    for field in dir(msg_content):
                                        if not field.startswith("_"):  # Skip private attributes
                                            field_value = getattr(msg_content, field)
                                            details.append(f"{field}: {field_value}")

                            # Aggregate messages by timestamp
                            messages_by_timestamp[timestamp].append(" | ".join(details))
                        except Exception as msg_error:
                            print(f"Error processing message: {msg_error}")

                    # Write the aggregated data to the CSV
                    for timestamp, message_details in messages_by_timestamp.items():
                        csv_writer.writerow([timestamp, len(message_details), " || ".join(message_details)])

                print(f"Data has been exported to {output_csv_path}")

            except Exception as e:
                print(f"Error processing file {rlog_path}: {e}")
        else:
            print(f"rlog.bz2 not found in {segment_path}")
