import os
import csv
from openpilot.tools.lib.logreader import LogReader

# Root directory containing all chunk directories
root_dir = "/media/ubuntu/SSD/Github/comma2k19"
output_dir = "/media/ubuntu/SSD/Github/comma2k19/Output_Logs"  # Directory to save output files

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

def process_and_write_chunk_to_csv(chunk_dir, output_csv_path):
    try:
        # Open the output CSV file for writing
        with open(output_csv_path, mode='w', newline='') as csvfile:
            fieldnames = [
                "Time Stamp", "ID", "Extended", "Dir", "Bus", "LEN",
                "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()  # Write the header row once

            # Traverse all .bz2 files in the current chunk directory
            for subdir, _, files in os.walk(chunk_dir):
                for file in files:
                    if file.endswith(".bz2"):
                        log_file_path = os.path.join(subdir, file)
                        process_and_append_to_csv(log_file_path, writer)

        print(f"Processed chunk: {chunk_dir}, output saved to {output_csv_path}")

    except Exception as e:
        print(f"Error processing chunk {chunk_dir}: {e}")

def process_and_append_to_csv(log_file, csv_writer):
    try:
        log_data = LogReader(log_file)

        for msg in log_data:
            if msg.which() == "can":
                for can_message in msg.can:
                    row = {
                        "Time Stamp": msg.logMonoTime,
                        "ID": f"{can_message.address:X}",
                        "Extended": "true",  # Assuming all messages are extended
                        "Dir": "Rx" ,#if can_message.src == 0 else "Tx",
                        "Bus": 0,
                        "LEN": len(can_message.dat),
                    }
                    for i in range(8):
                        row[f"D{i+1}"] = (
                            f"{can_message.dat[i]:02X}" if i < len(can_message.dat) else "00"
                        )
                    csv_writer.writerow(row)

    except Exception as e:
        print(f"Error processing log file {log_file}: {e}")

def generate_files_per_chunk(root_dir, output_dir):
    # Traverse the root directory for each chunk
    for chunk_name in os.listdir(root_dir):
        chunk_path = os.path.join(root_dir, chunk_name)
        if os.path.isdir(chunk_path) and chunk_name.startswith("Chunk_"):
            # Generate an output file for the current chunk
            output_csv_path = os.path.join(output_dir, f"{chunk_name}.csv")
            process_and_write_chunk_to_csv(chunk_path, output_csv_path)

# Execute the function
generate_files_per_chunk(root_dir, output_dir)
