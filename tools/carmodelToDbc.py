import json
import os
import csv
import datetime
from collections import defaultdict
import cantools
from openpilot.tools.lib.logreader import LogReader

# Paths (Update these paths accordingly)
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'
segments_directory = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'


# Load database.json
with open(database_json_path, 'r') as file:
    database = json.load(file)

# Explicit mapping from car models to DBC file paths
car_model_to_dbc = {'COMMA BODY': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/_honda_common.dbc', 'CHRYSLER PACIFICA HYBRID 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA HYBRID 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA HYBRID 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'ACURA RDX 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_rdx_2018_can.dbc', 'ACURA ILX 2016': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_ilx_2016_can.dbc', 'HONDA ODYSSEY 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_odyssey_exl_2018.dbc', 'HONDA INSIGHT 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_insight_ex_2019_can.dbc', 'HONDA PILOT 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_pilot_2023_can.dbc', 'ACURA RDX 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_rdx_2018_can.dbc', 'GENESIS GV60 ELECTRIC 1ST GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/_steering_sensors_a.dbc', 'HYUNDAI SANTA CRUZ 1ST GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE HYBRID 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SONATA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SONATA 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI TUCSON 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI TUCSON 4TH GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI PALISADE 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI IONIQ 5 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'HYUNDAI IONIQ 6 2023': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'HYUNDAI KONA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI VELOSTER 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'TOYOTA COROLLA TSS2 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/toyota_tss2_adas.dbc', 'TOYOTA PRIUS TSS2 2021': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/toyota_tss2_adas.dbc', 'NISSAN X-TRAIL 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/nissan/nissan_x_trail_2017.dbc', 'MAZDA CX-5': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA CX-9': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA 3': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA 6': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc'}


# car_model_to_dbc = {
#     'TOYOTA PRIUS 2017': r'/home/ubuntu/opendbc/toyota_prius_2017_pt_generated.dbc',
#     'HONDA ACCORD 2018 SPORT 2T': r'/home/ubuntu/opendbc/honda_accord_2018_can.dbc',
#     'HONDA CIVIC 2016 TOURING': r'/home/ubuntu/opendbc/honda_civic_touring_2016_can.dbc',
#     'TOYOTA RAV4 HYBRID 2017': r'/home/ubuntu/opendbc/toyota_rav4_hybrid_2017_pt_generated.dbc',
#     'SUBARU OUTBACK 2019 2.5I PREMIUM': r'/home/ubuntu/opendbc/subaru_outback_2019_generated.dbc',
#     'ACURA RDX 2018 ACURAWATCH PLUS': r'/home/ubuntu/opendbc/acura_rdx_2018_can.dbc',
#     'CHRYSLER PACIFICA 2018 HYBRID': r'/home/ubuntu/opendbc/chrysler_pacifica_hybrid_2018_can.dbc',
#     'KIA SOUL EV 2018': r'/home/ubuntu/opendbc/kia_soul_ev_2018.dbc',
#     'NISSAN LEAF 2018 SL': r'/home/ubuntu/opendbc/nissan_leaf_2018.dbc',
#     # Add more mappings as needed
# }

def format_timestamp(nanoseconds):
    seconds = nanoseconds / 1e9
    return datetime.datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S.%f')

for car_model, log_files in database.items():
    print(f'Processing logs for car model: {car_model}')
    # Get DBC file path from the explicit mapping
    dbc_file_path = car_model_to_dbc.get(car_model)
    if not dbc_file_path or not os.path.isfile(dbc_file_path):
        print(f"DBC file not found for car model {car_model}")
        continue
    try:
        db = cantools.database.load_file(dbc_file_path, strict = False)

        for log_file in log_files:
            # Construct the segment path
            segment_path = os.path.join(segments_directory, log_file)[:-2]
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
    except:
        continue