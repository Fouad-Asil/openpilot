import os
from openpilot.tools.lib.logreader import LogReader
import capnp

import json

# Path to the database.json file
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'

# Load the JSON data
with open(database_json_path, 'r') as file:
    database = json.load(file)

import os
from openpilot.tools.lib.logreader import LogReader

# Base path where the log files are stored
base_path = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'

# Function to print message details
def print_message_details(msg, count):
    msg_type = msg.which()
    print(f"\nMessage Type: {msg_type} (Count: {count})")
    msg_content = getattr(msg, msg_type)

    # Check if the message content is a list
    if isinstance(msg_content, capnp.lib.capnp._DynamicListReader):
        print(f"  This message contains a list with {len(msg_content)} elements.")
        for i, element in enumerate(msg_content):
            print(f"  Element {i + 1}:")
            for field in element.schema.fields:
                field_name = field
                field_value = getattr(element, field_name)
                print(f"    {field_name}: {field_value}")
    else:
        for field in msg_content.schema.fields:
            field_name = field
            field_value = getattr(msg_content, field_name)
            print(f"  {field_name}: {field_value}")

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    for log_file in log_files:
        segment_path = os.path.join(base_path, log_file)
        segment_path = segment_path[:-2]
        print(f'  Processing segment directory: {segment_path}')

        # Define the path to the rlog.bz2 file
        rlog_path = os.path.join(segment_path, 'rlog.bz2')

        if os.path.isfile(rlog_path):
            try:
                # Initialize LogReader with the rlog.bz2 file
                lr = LogReader(rlog_path)

                # Dictionary to count occurrences of each message type
                msg_counts = {}

                # Iterate through messages in the log
                for msg in lr:
                    msg_type = msg.which()
                    if msg_type not in msg_counts:
                        msg_counts[msg_type] = 0
                    msg_counts[msg_type] += 1

                    # Print details for the first 5 messages of each type
                    if msg_counts[msg_type] <= 5:
                        print_message_details(msg, msg_counts[msg_type])

            except Exception as e:
                print(f"Error processing file {rlog_path}: {e}")
        else:
            print(f"rlog.bz2 not found in {segment_path}")
