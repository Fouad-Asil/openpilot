import json
import os
import cantools
from openpilot.tools.lib.logreader import LogReader

# Paths (Update these paths accordingly)
database_json_path = r'/home/ubuntu/Documents/Github/commaCarSegments/database.json'
segments_directory = r'/home/ubuntu/Documents/Github/commaCarSegments/segments'
output_json_path = 'decoded_signals.json'

# Load database.json
with open(database_json_path, 'r') as file:
    database = json.load(file)

# Explicit mapping from car models to DBC file paths
car_model_to_dbc = {'COMMA BODY': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/_honda_common.dbc', 'CHRYSLER PACIFICA HYBRID 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA HYBRID 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA HYBRID 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'CHRYSLER PACIFICA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/chrysler/chrysler_pacifica_2017_hybrid.dbc', 'ACURA RDX 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_rdx_2018_can.dbc', 'ACURA ILX 2016': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_ilx_2016_can.dbc', 'HONDA ODYSSEY 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_odyssey_exl_2018.dbc', 'HONDA INSIGHT 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_insight_ex_2019_can.dbc', 'HONDA PILOT 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/honda_pilot_2023_can.dbc', 'ACURA RDX 2018': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/acura_rdx_2018_can.dbc', 'GENESIS GV60 ELECTRIC 1ST GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/honda/_steering_sensors_a.dbc', 'HYUNDAI SANTA CRUZ 1ST GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SANTA FE HYBRID 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SONATA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI SONATA 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_santafe_2007.dbc', 'HYUNDAI TUCSON 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI TUCSON 4TH GEN': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI PALISADE 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI IONIQ 5 2022': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'HYUNDAI IONIQ 6 2023': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'HYUNDAI KONA 2020': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_canfd.dbc', 'HYUNDAI VELOSTER 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/hyundai_kia_generic.dbc', 'TOYOTA COROLLA TSS2 2019': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/toyota_tss2_adas.dbc', 'TOYOTA PRIUS TSS2 2021': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/toyota_tss2_adas.dbc', 'NISSAN X-TRAIL 2017': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/generator/nissan/nissan_x_trail_2017.dbc', 'MAZDA CX-5': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA CX-9': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA 3': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc', 'MAZDA 6': r'/home/ubuntu/Documents/Github/opendbc/opendbc/dbc/mazda_rx8.dbc'}

def extract_signals(car_model, dbc_path, log_files, num_files=3):
    """Extracts a list of decoded signals for a given car model."""
    if not os.path.isfile(dbc_path):
        print(f"DBC file not found for car model {car_model}")
        return []

    try:
        db = cantools.database.load_file(dbc_path, strict=False)
        decoded_signals = set()

        for i, log_file in enumerate(log_files[:num_files]):
            segment_path = os.path.join(segments_directory, log_file)[:-2]
            rlog_path = os.path.join(segment_path, 'rlog.bz2')

            if not os.path.isfile(rlog_path):
                print(f"rlog.bz2 not found for {car_model} in {segment_path}")
                continue

            try:
                lr = LogReader(rlog_path)
                for msg in lr:
                    if msg.which() == 'can':
                        for can_msg in msg.can:
                            try:
                                # Decode the CAN message
                                decoded = db.decode_message(can_msg.address, can_msg.dat)
                                decoded_signals.update(decoded.keys())
                            except (KeyError, cantools.database.errors.Error):
                                continue
            except Exception as e:
                print(f"Error reading {rlog_path}: {e}")

        return list(decoded_signals)

    except Exception as e:
        print(f"Error loading DBC for {car_model}: {e}")
        return []

# Process each car model and extract signals
vehicle_signals = {}

for car_model, log_files in database.items():
    dbc_path = car_model_to_dbc.get(car_model)
    if not dbc_path:
        print(f"No DBC file mapping for car model {car_model}")
        continue

    print(f"Processing {car_model}")
    signals = extract_signals(car_model, dbc_path, log_files)
    if signals:
        vehicle_signals[car_model] = signals

# Save results to JSON
try:
    with open(output_json_path, 'w') as json_file:
        json.dump(vehicle_signals, json_file, indent=4)
    print(f"Decoded signals saved to {output_json_path}")
except Exception as e:
    print(f"Error saving JSON: {e}")
