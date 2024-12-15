import os
from openpilot.tools.lib.logreader import LogReader

import json

# Path to the database.json file
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'

# Load the JSON data
with open(database_json_path, 'r') as file:
    database = json.load(file)

import os
from openpilot.tools.lib.logreader import LogReader

# Base path where the log files are stored
base_path = r'/home/ubuntu/Documents/Github/commaCarSegments'

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    for log_file in log_files:
        log_file_path = os.path.join(base_path, log_file)
        print(f'  Reading log file: {log_file_path}')

        # Initialize LogReader with the log file
        lr = LogReader(log_file_path)

        # Iterate through messages in the log
        for msg in lr:
            # Process each message as needed
            print(msg)
