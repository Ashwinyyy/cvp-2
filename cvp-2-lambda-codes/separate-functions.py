
import boto3
import json
import logging
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading

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

#Function to extract reports.txt
def extract_reports(report_ids, reports_content, report_data):
    logging.info("Extracting report data from reports.txt...")
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue
            report_data[report_id] = {
                'report_no': clean_string(fields[1]),
                'version_no': clean_string(fields[2]),
                'datintreceived': convert_date_format(clean_string(fields[4])),
                'datreceived': convert_date_format(clean_string(fields[3])),
                'source_eng': clean_string(fields[37]),
                'mah_no': clean_string(fields[5]),
                'report_type_eng': clean_string(fields[7]),
                'reporter_type_eng': clean_string(fields[34]),
                'seriousness_eng': clean_string(fields[26]),
                'death': convert_to_yes_no(clean_string(fields[28])),
                'disability': convert_to_yes_no(clean_string(fields[29])),
                'congenital_anomaly': convert_to_yes_no(clean_string(fields[30])),
                'life_threatening': convert_to_yes_no(clean_string(fields[31])),
                'hospitalization': convert_to_yes_no(clean_string(fields[32])),
                'other_medically_imp_cond': convert_to_yes_no(clean_string(fields[33])),
                'age': clean_string(fields[12]),
                'age_unit_eng': clean_string(fields[14]),
                'gender_eng': clean_string(fields[10]),
                'height': clean_string(fields[22]),
                'height_unit_eng': clean_string(fields[23]),
                'weight': clean_string(fields[19]),
                'weight_unit_eng': clean_string(fields[20]),
                'outcome_eng': clean_string(fields[17])
            }
    logging.info(f"Extracted {len(report_data)} reports.")



#Function to extract reportlinks.txt
def extract_report_links(report_ids, report_links_content, report_data):
    logging.info("Extracting report link data from report_links.txt...")
    for line in report_links_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue
        report_data[report_id]['record_type_eng'] = clean_string(fields[2])
        report_data[report_id]['report_link_no'] = clean_string(fields[4])
    logging.info(f"Extracted links for {len(report_data)} reports.")   


lock = threading.Lock()

def extract_drug_and_indication_data(report_ids, report_drug_content, report_drug_indication_content, report_data):
    logging.info("Extracting drug and indication data from report_drug.txt and report_drug_indication.txt...")

    # Create an indication map for quick lookup
    indications_map = {}
    if report_drug_indication_content:
        for line in report_drug_indication_content:
            fields = line.split('$')
            if len(fields) > 1:
                report_id = clean_string(fields[1]).strip()
                drug_name = clean_string(fields[3]).strip()
                indication = clean_string(fields[4]).strip()
                if report_id not in indications_map:
                    indications_map[report_id] = {}
                indications_map[report_id][drug_name] = indication

    # Process drug data and align with indications
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            if report_id not in report_ids:
                continue
            drug_name = clean_string(fields[3])
            drug_type = clean_string(fields[4])
            dose_unit = clean_string(fields[9])
            route_of_admin = clean_string(fields[6])
            dosage_form = clean_string(fields[20])
            dose_qty = clean_string(fields[8])
            frequency = clean_string(fields[15])
            therapy_time = clean_string(fields[17])
            therapy_unit = clean_string(fields[18])

            # Initialize if report_id is not already in report_data
            if report_id not in report_data:
                report_data[report_id] = {
                    'drug_name_eng': '',
                    'drug_type_eng': '',
                    'dose_unit_eng': '',
                    'route_eng': '',
                    'dosageform_eng': '',
                    'unit_dose_qty': '',
                    'freq_time_unit_eng': '',
                    'therapy_duration': '',
                    'therapy_duration_unit_eng': '',
                    'indication_eng': ''
                }

            # Thread-safe access to report_data
            with lock:
                # Append data to the report_data structure
                report_data[report_id]['drug_name_eng'] += ', ' + drug_name if report_data[report_id]['drug_name_eng'] else drug_name
                report_data[report_id]['drug_type_eng'] += ', ' + drug_type if report_data[report_id]['drug_type_eng'] else drug_type
                report_data[report_id]['dose_unit_eng'] += ', ' + dose_unit if report_data[report_id]['dose_unit_eng'] else dose_unit
                report_data[report_id]['route_eng'] += ', ' + route_of_admin if report_data[report_id]['route_eng'] else route_of_admin
                report_data[report_id]['dosageform_eng'] += ', ' + dosage_form if report_data[report_id]['dosageform_eng'] else dosage_form
                report_data[report_id]['unit_dose_qty'] += ', ' + dose_qty if report_data[report_id]['unit_dose_qty'] else dose_qty
                report_data[report_id]['freq_time_unit_eng'] += ', ' + frequency if report_data[report_id]['freq_time_unit_eng'] else frequency
                report_data[report_id]['therapy_duration'] += ', ' + therapy_time if report_data[report_id]['therapy_duration'] else therapy_time
                report_data[report_id]['therapy_duration_unit_eng'] += ', ' + therapy_unit if report_data[report_id]['therapy_duration_unit_eng'] else therapy_unit

                # Align drug names with indications
                if report_id in indications_map:
                    drug_names = report_data[report_id]['drug_name_eng'].split(', ')
                    indications = [''] * len(drug_names)
                    for i, drug_name in enumerate(drug_names):
                        if drug_name in indications_map[report_id]:
                            indications[i] = indications_map[report_id][drug_name]
                    report_data[report_id]['indication_eng'] = ', '.join(indications)

    logging.info(f"Extracted drug and indication data for {len(report_data)} reports.")


#Function to extract reactions.txt
def extract_reactions(report_ids, reactions_content, report_data):
    logging.info("Extracting reaction data from reactions.txt...")
    for line in reactions_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id not in report_ids:
            continue
        adv_reaction_eng = clean_string(fields[5])
        meddra_version = clean_string(fields[9])
        reaction_duration = clean_string(fields[2])
        reaction_duration_unit = clean_string(fields[3])
        if 'reaction_eng' in report_data[report_id]:
            report_data[report_id]['reaction_eng'] += ', ' + adv_reaction_eng
            report_data[report_id]['version'] += ', ' + meddra_version
            report_data[report_id]['duration'] += ', ' + reaction_duration
            report_data[report_id]['duration_unit_eng'] += ', ' + reaction_duration_unit
        else:
            report_data[report_id]['reaction_eng'] = adv_reaction_eng
            report_data[report_id]['version'] = meddra_version
            report_data[report_id]['duration'] = reaction_duration
            report_data[report_id]['duration_unit_eng'] = reaction_duration_unit
    logging.info(f"Extracted reactions for {len(report_data)} reports.")

# # Main function to call all the extraction functions
# def extract_report_data(report_ids, reports_content, reactions_content, report_links_content,
#                         report_drug_indication_content, report_drug_content):
#     logging.info("Extracting report data from all files...")
#     report_data = {}
    
#     # Call each function to extract the relevant data
#     extract_reports(report_ids, reports_content, report_data)
#     extract_reactions(report_ids, reactions_content, report_data)
#     extract_report_links(report_ids, report_links_content, report_data)
#     extract_drug_data(report_ids, report_drug_content, report_data)
#     extract_indications(report_ids, report_drug_indication_content, report_data)

#     logging.info(f"Extraction completed for {len(report_data)} reports.")
#     return report_data

def extract_report_data_concurrently(report_ids, reports_content, reactions_content, report_links_content,
                                     report_drug_indication_content, report_drug_content):
    logging.info("Extracting report data from all files concurrently...")

    report_data = {}

    # Using ThreadPoolExecutor for concurrent execution
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Dispatching tasks to separate threads
        future_reports = executor.submit(extract_reports, report_ids, reports_content, report_data)
        future_reactions = executor.submit(extract_reactions, report_ids, reactions_content, report_data)
        future_links = executor.submit(extract_report_links, report_ids, report_links_content, report_data)
        future_drug_data = executor.submit(extract_drug_and_indication_data, report_ids, report_drug_content, report_drug_indication_content, report_data)


        # Wait for all tasks to complete
        future_reports.result()
        future_reactions.result()
        future_links.result()
        future_drug_indication.result()
        future_drug_data.result()

    logging.info(f"Extraction completed for {len(report_data)} reports.")
    return report_data



# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data. append({
            "report_id": report_id,
            "report_no": data.get('report_no', ''),
            "version_no": data.get('version_no', ''),
            "datintreceived": data.get('datintreceived', ''),
            "datreceived": data.get('datreceived', ''),
            "source_eng": data.get('source_eng', ''),
            "mah_no": data.get('mah_no', ''),
            "report_type_eng": data.get('report_type_eng', ''),
            "reporter_type_eng": data.get('reporter_type_eng', ''),
            "seriousness_eng": data.get('seriousness_eng', ''),
            "death": data.get('death', ''),
            "disability": data.get('disability', ''),
            "congenital_anomaly": data.get('congenital_anomaly', ''),
            "life_threatening": data.get('life_threatening', ''),
            "hospitalization": data.get('hospitalization', ''),
            "other_medically_imp_cond": data.get('other_medically_imp_cond', ''),
            "age": data.get('age', ''),
            'age_unit_eng': data.get('age_unit_eng', ''),
            "gender_eng": data.get('gender_eng', ''),
            "height": data.get('height', ''),
            "height_unit_eng": data.get('height_unit_eng', ''),
            "weight": data.get('weight', ''),
            "weight_unit_eng": data.get('weight_unit_eng', ''),
            "outcome_eng": data.get('outcome_eng', ''),

            "record_type_eng": data.get('record_type_eng', ''),
            "report_link_no": data.get('report_link_no', ''),
            "drug_name_eng": data.get('drug_name_eng', ''),
            "drug_type_eng": data.get('drug_type_eng', ''),
            "route_eng": data.get('route_eng', ''),
            "dosageform_eng": data.get('dosageform_eng', ''),
            "unit_dose_qty": data.get('unit_dose_qty', ''),
            "dose_unit_eng": data.get('dose_unit_eng', ''),
            "freq_time_unit_eng": data.get('freq_time_unit_eng', ''),
            "therapy_duration": data.get('therapy_duration', ''),
            "therapy_duration_unit_eng": data.get('therapy_duration_unit_eng', ''),
            "indication_eng": data.get('indication_eng', '') , # Indication left blank if not found
            "reaction_eng": data.get('reaction_eng', ''),
            "version": data.get('version', ''),
            "duration": data.get('duration', ''),
            "duration_unit_eng": data.get('duration_unit_eng', ''),
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
    report_data = extract_report_data_concurrently(report_ids, data['reports'], data['reactions'], data['report_links'],
                                      data['report_drug_indication'], data['report_drug'])

    # Step 4: Generate and save the JSON output to S3
    generate_json_output(report_data)

    logging.info(f"Script execution completed in {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
