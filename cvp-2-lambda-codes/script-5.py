
import boto3
import json
import logging
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Initialize logging and S3 client
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
s3_client = boto3.client('s3')

# Input and output S3 buckets
input_bucket = 'cvp-2-bucket'
output_bucket = 'cvp-2-output-2-testing'

# File paths in S3
drug_names_file = 'Input_data/Suspected_Product_Brand_Name/drug_names.txt'
report_drug_file = 'Input_data/report_id_database/report_drug.txt'
reports_file = 'Input_data/report_id_database/reports.txt'
reactions_file = 'Input_data/report_id_database/reactions.txt'
report_links_file = 'Input_data/report_id_database/report_links.txt'
report_drug_indication_file = 'Input_data/report_id_database/report_drug_indication.txt'


# Function to read files from S3
def read_s3_file(bucket, key):
    try:
        logging.info(f"Attempting to read S3 file {key} from bucket {bucket}...")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        logging.info(f"Successfully read S3 file {key} from bucket {bucket}.")
        return response['Body'].read().decode('utf-8').splitlines()
    except Exception as e:
        logging.error(f"Error reading S3 file {key} from bucket {bucket}: {e}")
        return []


#converting date format
def convert_date_format(date_str):
    try:
        # Convert the date string from 'DD-MMM-YY' to 'YYYY-MM-DD'
        date_obj = datetime.strptime(date_str, "%d-%b-%y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        # Return the original string if it doesn't match the expected format
        return date_str

#coverting 1 to yes, 2 to no
def convert_to_yes_no(value):
    if value == "1":
        return "yes"
    elif value == "2":
        return "no"
    else:
        return value  # In case there are other values, return the original value

#cleaning data
def clean_string(value):
    """Removes unwanted escape sequences and extra quotes from a JSON string."""
    return value.strip('"').replace('\\"', '')

# Step 1: Parse drug names from file
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    drug_names = [line.strip().lower() for line in file_content if line.strip()]
    logging.info(f"Parsed {len(drug_names)} drug names.")
    return drug_names



# Step 2: Locate REPORT_IDs corresponding to drug names
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()
            report_id = clean_string(fields[1]).strip()
            if any(drug_name in line.lower() for drug_name in drug_names):
                report_ids[report_id].append(fields)
    logging.info(f"Found {len(report_ids)} report IDs matching the drug names.")
    return report_ids


# Step 3: Extract data from reference files based on REPORT_ID
def extract_report_data(report_ids, reports_content, reactions_content, report_links_content,
                        report_drug_indication_content, report_drug_content):
    logging.info("Extracting report data from reference files...")
    report_data = {}

    # Read reports.txt and build report_data
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue  # Skip if the report_id is not in the report_ids
            report_data[report_id] = {
                'report_no': clean_string(fields[1]),
                'version_no': clean_string(fields[2]),
                'datreceived': convert_date_format(clean_string(fields[3])),
                'datintreceived': convert_date_format(clean_string(fields[4])),
                'mah_no': clean_string(fields[5]),
                'report_type_eng': clean_string(fields[7]),
                'gender_eng': clean_string(fields[10]),
                'age': clean_string(fields[12]),
                'age_unit_eng': clean_string(fields[14]),
                'outcome_eng': clean_string(fields[17]),
                'weight': clean_string(fields[19]),
                'weight_unit_eng': clean_string(fields[20]),
                'height': clean_string(fields[22]),
                'height_unit_eng': clean_string(fields[23]),
                'seriousness_eng': clean_string(fields[26]),
                'death': convert_to_yes_no(clean_string(fields[28])),
                'disability': convert_to_yes_no(clean_string(fields[29])),
                'congenital_anomaly': convert_to_yes_no(clean_string(fields[30])),
                'life_threatening': convert_to_yes_no(clean_string(fields[31])),
                'hospitalization': convert_to_yes_no(clean_string(fields[32])),
                'other_medically_imp_cond': convert_to_yes_no(clean_string(fields[33])),
                'reporter_type_eng': clean_string(fields[34]),
                'source_eng': clean_string(fields[37])
            }

    # Read reactions.txt
    for line in reactions_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids

        # Accumulate drug names, appending with commas if multiple drugs exist
        pt_name_eng = clean_string(fields[5])
        meddra_version = clean_string(fields[9])
        duration = clean_string(fields[2])
        duration_unit_eng = clean_string(fields[3])
        if 'pt_name_eng' in report_data[report_id]:
            report_data[report_id]['pt_name_eng'] += ', ' + pt_name_eng
            report_data[report_id]['meddra_version'] += ', ' + meddra_version
            report_data[report_id]['duration'] += ', ' + duration
            report_data[report_id]['duration_unit_eng'] += ', ' + duration_unit_eng
        else:
            report_data[report_id]['pt_name_eng'] = pt_name_eng
            report_data[report_id]['meddra_version'] = meddra_version
            report_data[report_id]['duration'] = duration
            report_data[report_id]['duration_unit_eng'] = duration_unit_eng

        # report_data[report_id]['reaction_eng'] = clean_string(fields[5])
        # report_data[report_id]['version'] = clean_string(fields[9])
        # report_data[report_id]['duration'] = clean_string(fields[2])

    # Read report_links.txt
    for line in report_links_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        record_type_eng = clean_string(fields[2]).strip()
        report_link_no = clean_string(fields[4]).strip()
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        
        if not record_type_eng:
            record_type_eng = 'No duplicate or linked report'

        if not report_link_no:  
            report_link_no = 'No duplicate or linked report'   
        
          # Update the report_data dictionary with the extracted or default values
        if report_id not in report_data:
            report_data[report_id] = {}  # Initialize the report_data entry if missing

        report_data[report_id]['record_type_eng'] = record_type_eng
        report_data[report_id]['report_link_no'] = report_link_no

        # else
        # report_data[report_id]['record_type_eng'] = clean_string(fields[2])
        # report_data[report_id]['report_link_no'] = clean_string(fields[4])

    # Read report_drug_indication.txt
    # # Read report_drug_indication.txt
    # for line in report_drug_indication_content:
    #     fields = line.split('$')
    #     report_id = fields[1].strip()
    #
    #     # Check if the report_id is in the report_ids set
    #     if report_id not in report_ids:
    #         report_data[report_id] = {'indication_eng': ''}  # Set blank if no match
    #         continue  # Skip further processing if the report_id is not in the report_ids
    #
    #     # If there's a match, process the indication
    #     report_data[report_id]['indication_eng'] = fields[4] if len(fields) > 4 else ''


    # Create a dictionary to store the drug names for each report_id as a comma-separated string
drug_names_dict = {}

# Read and process report_drug.txt to accumulate drug names and other fields
for line in report_drug_content:
    fields = line.split('$')
    if len(fields) > 1:
        report_id = clean_string(fields[1]).strip()
        
        if report_id not in report_ids:
            continue  # Skip if the report_id is not in the report_ids
        
        # Extract drug-related fields
        drug_name = clean_string(fields[3])
        drug_involvement = clean_string(fields[4])
        route_admin = clean_string(fields[6])
        unit_dose_qty = clean_string(fields[8])
        dose_unit_eng = clean_string(fields[9])
        freq_time_unit_eng = clean_string(fields[15])
        therapy_duration = clean_string(fields[17])
        therapy_duration_unit_eng = clean_string(fields[18])
        
        # Store drug names in the dictionary for each report_id as a comma-separated string
        if report_id not in drug_names_dict:
            drug_names_dict[report_id] = drug_name  # First drug name for this report_id
        else:
            # Append drug name with a comma if more than one drug name exists for this report_id
            drug_names_dict[report_id] += ', ' + drug_name

        # Accumulate fields in report_data for the given report_id
        if report_id not in report_data:
            report_data[report_id] = {}

        if 'drug_name' in report_data[report_id]:
            report_data[report_id]['drug_name'] += ', ' + drug_name
            report_data[report_id]['drug_involvement'] += ', ' + drug_involvement
            report_data[report_id]['route_admin'] += ', ' + route_admin
            report_data[report_id]['unit_dose_qty'] += ', ' + unit_dose_qty
            report_data[report_id]['dose_unit_eng'] += ', ' + dose_unit_eng
            report_data[report_id]['freq_time_unit_eng'] += ', ' + freq_time_unit_eng
            report_data[report_id]['therapy_duration'] += ', ' + therapy_duration
            report_data[report_id]['therapy_duration_unit_eng'] += ', ' + therapy_duration_unit_eng
        else:
            report_data[report_id]['drug_name'] = drug_name
            report_data[report_id]['drug_involvement'] = drug_involvement
            report_data[report_id]['route_admin'] = route_admin
            report_data[report_id]['unit_dose_qty'] = unit_dose_qty
            report_data[report_id]['dose_unit_eng'] = dose_unit_eng
            report_data[report_id]['freq_time_unit_eng'] = freq_time_unit_eng
            report_data[report_id]['therapy_duration'] = therapy_duration
            report_data[report_id]['therapy_duration_unit_eng'] = therapy_duration_unit_eng

# Read and process report_drug_indication.txt to match indications with drug names
logging.info("Reading report_drug_indication.txt...")
report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)

if report_drug_indication_content:
    for line in report_drug_indication_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            drug_name_eng = clean_string(fields[3]).strip()
            indication = clean_string(fields[4]).strip()

            # Skip if report_id is not in report_ids
            if report_id not in report_ids:
                continue

            # Retrieve drug names for the current report_id from the drug_names_dict
            drug_names_for_report = drug_names_dict.get(report_id, "").split(', ')

            # Initialize an indication_eng entry if missing
            if 'indication_eng' not in report_data[report_id]:
                report_data[report_id]['indication_eng'] = ''

            # Iterate over the drug names for the report and check if they match
            for drug_name in drug_names_for_report:
                if not drug_name.strip():
                    continue  # Skip blank drug names
                
                # Check if the drug_name matches drug_name_eng in the indication file
                if drug_name.lower() == drug_name_eng.lower():
                    if report_data[report_id]['indication_eng']:
                        report_data[report_id]['indication_eng'] += ', ' + indication
                    else:
                        report_data[report_id]['indication_eng'] = indication
else:
    logging.info("No content found in report_drug_indication.txt.")

# Fill blank or unmatched indications
for report_id in report_ids:
    if 'indication_eng' not in report_data[report_id] or not report_data[report_id]['indication_eng']:
        report_data[report_id]['indication_eng'] = 'No matching indication for the drugs'


           
    logging.info(f"Extracted data for {len(report_data)} report IDs.")
    return report_data


# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data. append({
            "report_no": data.get('report_no', ''),
            "version_no": data.get('version_no', ''),
            "datintreceived": data.get('datintreceived', ''),
            "datreceived": data.get('datreceived', ''),
            "mah_no": data.get('mah_no', ''),
            "report_type_eng": data.get('report_type_eng', ''),
            "gender_eng": data.get('gender_eng', ''),
            "age": data.get('age', ''),
            "age_unit_eng": data.get('age_unit_eng', ''),
            "outcome_eng": data.get('outcome_eng', ''),
            "weight": data.get('weight', ''),
            "weight_unit_eng": data.get('weight_unit_eng', ''),
            "height": data.get('height', ''),
            "height_unit_eng": data.get('height_unit_eng', ''),
            "seriousness_eng": data.get('seriousness_eng', ''),
            "death": data.get('death', ''),
            "disability": data.get('disability', ''),
            "congenital_anomaly": data.get('congenital_anomaly', ''),
            "life_threatening": data.get('life_threatening', ''),
            "hospitalization": data.get('hospitalization', ''),
            "other_medically_imp_cond": data.get('other_medically_imp_cond', ''),
            "reporter_type_eng": data.get('reporter_type_eng', ''),
            "source_eng": data.get('source_eng', ''),
            "pt_name_eng": data.get('pt_name_eng', ''),
            "meddra_version": data.get('meddra_version', ''),
            "duration": data.get('duration', ''),
            "duration_unit_eng": data.get('duration_unit_eng', ''),
            "record_type_eng": data.get('record_type_eng', ''),
            "report_link_no": data.get('report_link_no', ''),
            "drug_name": data.get('drug_name', ''),
            "drug_involvement": data.get('drug_involvement', ''),
            "route_admin": data.get('route_admin', ''),
            "unit_dose_qty": data.get('unit_dose_qty', ''),
            "dose_unit_eng": data.get('dose_unit_eng', ''),
            "freq_time_unit_eng": data.get('freq_time_unit_eng', ''),
            "therapy_duration": data.get('therapy_duration', ''),
            "therapy_duration_unit_eng": data.get('therapy_duration_unit_eng', ''),
            "dosage_form_eng": data.get('dosage_form_eng', ''),
            "indication_eng": data.get('indication_eng', '')
        })

    try:
        json_data = json.dumps(final_data, indent=4)
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        output_file = f"output_data_{timestamp}.json"
        s3_client.put_object(Bucket=output_bucket, Key=output_file, Body=json_data)
        logging.info(f"Successfully uploaded JSON file to S3: {output_file}")
    except Exception as e:
        logging.error(f"Error generating or uploading JSON output: {e}")


# Main function to execute all steps in parallel
def main():
    logging.info("Starting script execution...")
    start_time = time.time()

    # Read input files in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # Submit S3 read tasks
        futures = {
            'drug_names': executor.submit(read_s3_file, input_bucket, drug_names_file),
            'report_drug': executor.submit(read_s3_file, input_bucket, report_drug_file),
            'reports': executor.submit(read_s3_file, input_bucket, reports_file),
            'reactions': executor.submit(read_s3_file, input_bucket, reactions_file),
            'report_links': executor.submit(read_s3_file, input_bucket, report_links_file),
            'report_drug_indication': executor.submit(read_s3_file, input_bucket, report_drug_indication_file)
        }

        # Wait for all read tasks to finish
        data = {key: future.result() for key, future in futures.items()}

    # Step 1: Parse drug names
    logging.info("Starting parsing drugnames...")
    drug_names = parse_drug_names(data['drug_names'])

    # Step 2: Find report IDs corresponding to drug names
    report_ids = find_report_ids(drug_names, data['report_drug'])

    # Step 3: Extract data based on report IDs
    report_data = extract_report_data(report_ids, data['reports'], data['reactions'], data['report_links'],
                                      data['report_drug_indication'], data['report_drug'])

    # Step 4: Generate and save the JSON output to S3
    generate_json_output(report_data)

    logging.info(f"Script execution completed in {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
# import boto3


# import json
# import logging
# from collections import defaultdict
# import time
#
# # Initialize logging and S3 client
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# s3_client = boto3.client('s3')
#
# # Input and output S3 buckets
# input_bucket = 'cvp-2-bucket'
# output_bucket = 'cvp-2-output'
#
# # File paths in S3
# drug_names_file = 'Input_data/Suspected_Product_Brand_Name/drug_names.txt'
# report_drug_file = 'Input_data/report_id_database/report_drug.txt'
# reports_file = 'Input_data/report_id_database/reports.txt'
# reactions_file = 'Input_data/report_id_database/reactions.txt'
# report_links_file = 'Input_data/report_id_database/report_links.txt'
# report_drug_indication_file = 'Input_data/report_id_database/report_drug_indication.txt'
#
#
# # Function to read files from S3
# def read_s3_file(bucket, key):
#     try:
#         logging.info(f"Attempting to read S3 file {key} from bucket {bucket}...")
#         response = s3_client.get_object(Bucket=bucket, Key=key)
#         logging.info(f"Successfully read S3 file {key} from bucket {bucket}.")
#         return response['Body'].read().decode('utf-8').splitlines()
#     except Exception as e:
#         logging.error(f"Error reading S3 file {key} from bucket {bucket}: {e}")
#         return []
#
#
# # Step 1: Parse drug names from file
# def parse_drug_names(file_content):
#     logging.info("Parsing drug names...")
#     drug_names = []
#     for line in file_content:
#         line = line.strip().lower()  # Case-insensitive parsing
#         if line:
#             drug_names.append(line)
#     logging.info(f"Parsed {len(drug_names)} drug names.")
#     return drug_names
#
#
# # Step 2: Locate REPORT_IDs corresponding to drug names
# def find_report_ids(drug_names, report_drug_content):
#     logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
#     report_ids = defaultdict(list)
#     for line in report_drug_content:
#         fields = line.split('$')
#         if len(fields) > 1:
#             drug_name = fields[3].strip().lower()
#             report_id = fields[1].strip()
#             if any(drug_name in line.lower() for drug_name in drug_names):
#                 report_ids[report_id].append(fields)
#     logging.info(f"Found {len(report_ids)} report IDs matching the drug names.")
#     return report_ids
#
#
# # Step 3: Extract data from reference files based on REPORT_ID
# def extract_report_data(report_ids):
#     logging.info("Extracting report data from reference files...")
#     report_data = {}
#
#     # Read reports.txt
#     logging.info("Reading reports.txt...")
#     reports_content = read_s3_file(input_bucket, reports_file)
#     for line in reports_content:
#         fields = line.split('$')
#         if len(fields) > 1:
#             report_id = fields[0].strip()
#             if report_id in report_ids:
#                 report_data[report_id] = {
#                     'report_no': fields[1],
#                     'version_no': fields[2],
#                     'datintreceived': fields[4],
#                     'datreceived': fields[3],
#                     'source_eng': fields[37],
#                     'mah_no': fields[5],
#                     'report_type_eng': fields[7],
#                     'reporter_type_eng': fields[34],
#                     'seriousness_eng': fields[26],
#                     'death': fields[28],
#                     'disability': fields[29],
#                     'congenital_anomaly': fields[30],
#                     'life_threatening': fields[31],
#                     'hospitalization': fields[32],
#                     'other_medically_imp_cond': fields[33],
#                     'age': fields[12],
#                     'gender_eng': fields[10],
#                     'height': fields[22],
#                     'weight': fields[19],
#                     'outcome_eng': fields[17]
#                 }
#
#     # Read reactions.txt
#     logging.info("Reading reactions.txt...")
#     reactions_content = read_s3_file(input_bucket, reactions_file)
#     for line in reactions_content:
#         fields = line.split('$')
#         report_id = fields[1].strip()
#         if report_id in report_ids:
#             report_data[report_id]['reaction_eng'] = fields[5]
#             report_data[report_id]['version'] = fields[9]
#             report_data[report_id]['duration'] = fields[2]
#
#     # Read report_links.txt and add data to report_data
#     logging.info("Reading report_links.txt...")
#     report_links_content = read_s3_file(input_bucket, report_links_file)
#     for line in report_links_content:
#         fields = line.split('$')
#         report_id = fields[1].strip()
#         if report_id in report_ids:
#             report_data[report_id]['link_type_eng'] = fields[2]
#             report_data[report_id]['e2b_report_no'] = fields[4]
#
#     # # Read report_drug_indication.txt and add data to report_data if content exists
#     # logging.info("Reading report_drug_indication.txt...")
#     # report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)
#     # if report_drug_indication_content:  # If file has content
#     #     for line in report_drug_indication_content:
#     #         fields = line.split('$')
#     #         report_id = fields[1].strip()
#     #         if report_id in report_ids:
#     #             report_data[report_id]['indication_eng'] = fields[4]
#     # else:
#     #     logging.info("No content found in report_drug_indication.txt.")
#     # Read report_drug_indication.txt and process matching report_ids
#     logging.info("Reading report_drug_indication.txt...")
#     report_drug_indication_content = read_s3_file(input_bucket, report_drug_indication_file)
#
#     # Initialize a set to store all report_ids found in the file
#     indication_report_ids = set()
#
#     if report_drug_indication_content:  # If the file has content
#         for line in report_drug_indication_content:
#             # Split the line into fields using '$' as the delimiter
#             fields = line.split('$')
#
#             # if len(fields) > 4:  # Ensure there are enough fields to avoid index errors
#                 # Extract the report_id from field[1] and strip any whitespace
#                 report_id = fields[1].strip()
#
#                 # Add the report_id to the set of indication_report_ids
#                 indication_report_ids.add(report_id)
#             # else:
#             #     logging.warning(f"Line skipped due to insufficient fields: {line.strip()}")
#     else:
#         logging.info("No content found in report_drug_indication.txt.")
#
#     # Compare report_ids with indication_report_ids
#     logging.info("Comparing report_ids with report IDs from indication file...")
#     for report_id in report_ids:
#         if report_id in indication_report_ids:  # Proceed only if report_id exists in the indication file
#             # Get the corresponding line and extract the indication_eng field
#             for line in report_drug_indication_content:
#                 fields = line.split('$')
#                 # if len(fields) > 4 and fields[1].strip() == report_id:
#                 if fields[1].strip() == report_id:
#                     report_data[report_id]['indication_eng'] = fields[4]
#         else:
#             logging.info(f"Skipping report_id {report_id} as it is not in the indication file.")
#
#     # Read report_drug.txt and add data to report_data
#     logging.info("Reading report_drug.txt...")
#     report_drug_content = read_s3_file(input_bucket, report_drug_file)
#     for line in report_drug_content:
#         fields = line.split('$')
#         if len(fields) > 1:
#             report_id = fields[1].strip()
#             if report_id in report_ids:
#                 # Add the new data fields from report_drug.txt
#                 report_data[report_id]['drug_name_eng'] = fields[3]  # DRUG_NAME_ENG
#                 report_data[report_id]['drug_type_eng'] = fields[4]  # DRUG_TYPE_ENG
#                 report_data[report_id]['dose_unit_eng'] = fields[9]  # DOSE_UNIT_ENG
#                 report_data[report_id]['route_eng'] = fields[6]  # ROUTE_ENG
#                 report_data[report_id]['dose'] = fields[8]  # DOSE
#                 report_data[report_id]['freq'] = fields[11]  # FREQ
#                 report_data[report_id]['therapy_duration'] = fields[17]  # DURATION
#
#     logging.info(f"Extracted data for {len(report_data)} report IDs.")
#     return report_data
#
#
# # Step 4: Generate the JSON structure and save it to the output S3 bucket
# def generate_json_output(report_data):
#     logging.info("Generating JSON output...")
#     final_data = []
#     for report_id, data in report_data.items():
#         final_data.append({
#             "report_id": report_id,
#             "report_no": data.get('report_no', ''),
#             "version_no": data.get('version_no', ''),
#             "datintreceived": data.get('datintreceived', ''),
#             "datreceived": data.get('datreceived', ''),
#             "source_eng": data.get('source_eng', ''),
#             "mah_no": data.get('mah_no', ''),
#             "report_type_eng": data.get('report_type_eng', ''),
#             "reporter_type_eng": data.get('reporter_type_eng', ''),
#             "seriousness_eng": data.get('seriousness_eng', ''),
#             "death": data.get('death', ''),
#             "disability": data.get('disability', ''),
#             "congenital_anomaly": data.get('congenital_anomaly', ''),
#             "life_threatening": data.get('life_threatening', ''),
#             "hospitalization": data.get('hospitalization', ''),
#             "other_medically_imp_cond": data.get('other_medically_imp_cond', ''),
#             "age": data.get('age', ''),
#             "gender_eng": data.get('gender_eng', ''),
#             "height": data.get('height', ''),
#             "weight": data.get('weight', ''),
#             "outcome_eng": data.get('outcome_eng', ''),
#             "reaction_eng": data.get('reaction_eng', ''),
#             "version": data.get('version', ''),
#             "duration": data.get('duration', ''),
#             "link_type_eng": data.get('link_type_eng', ''),
#             "e2b_report_no": data.get('e2b_report_no', ''),
#             "drug_name_eng": data.get('drug_name_eng', ''),
#             "drug_type_eng": data.get('drug_type_eng', ''),
#             "dose_unit_eng": data.get('dose_unit_eng', ''),
#             "route_eng": data.get('route_eng', ''),
#             "dose": data.get('dose', ''),
#             "freq": data.get('freq', ''),
#             "therapy_duration": data.get('therapy_duration', ''),
#             "indication_eng": data.get('indication_eng', '')
#         })
#
#     try:
#         json_data = json.dumps(final_data, indent=4)
#         timestamp = time.strftime('%Y%m%d-%H%M%S')
#         output_file_key = f'output_{timestamp}.json'
#         logging.info(f"Saving the generated JSON data to S3 as {output_file_key}...")
#         s3_client.put_object(Bucket=output_bucket, Key=output_file_key, Body=json_data)
#         logging.info(f"Successfully saved the generated JSON data to {output_file_key}.")
#     except Exception as e:
#         logging.error(f"Error generating or saving JSON output: {e}")
#
#
# # Main execution logic
# def main():
#     logging.info("Process started...")
#
#     # Step 1: Read drug names
#     drug_names_content = read_s3_file(input_bucket, drug_names_file)
#     drug_names = parse_drug_names(drug_names_content)
#
#     # Step 2: Read report_drug.txt
#     report_drug_content = read_s3_file(input_bucket, report_drug_file)
#
#     # Step 3: Find report IDs corresponding to the drug names
#     report_ids = find_report_ids(drug_names, report_drug_content)
#
#     # Step 4: Extract data from reference files based on report IDs
#     report_data = extract_report_data(report_ids)
#
#     # Step 5: Generate JSON output and save it to S3
#     generate_json_output(report_data)
#
#     logging.info("Process completed.")
#
# if __name__ == '__main__':
#     main()
