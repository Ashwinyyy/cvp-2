import boto3
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import asyncio
import aioboto3
import threading
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import logging
from concurrent.futures import as_completed

# Initialize logging and S3 client
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input and output S3 buckets
input_bucket = 'cvp-2-bucket'
output_bucket = 'cvp-2-output-2-testing'

# File paths in S3
files_to_read = [
    'Input_data/Suspected_Product_Brand_Name/drug_names.txt',
    'Input_data/report_id_database/report_drug.txt',
    'Input_data/report_id_database/reports.txt',
    'Input_data/report_id_database/report_drug_indication.txt',
    'Input_data/report_id_database/report_links.txt',
    'Input_data/report_id_database/reactions.txt'
    # 'Input_data/report_id_database/report_drug_indication.txt'
]


# Function to read files from S3 asynchronously using aioboto3
async def read_s3_file_async(bucket, key):
    async with aioboto3.Session().client('s3') as s3_client:
        try:
            logging.info(f"Attempting to read S3 file {key} from bucket {bucket} asynchronously...")
            response = await s3_client.get_object(Bucket=bucket, Key=key)
            content = await response['Body'].read()
            logging.info(f"Successfully read S3 file {key}.")
            return content.decode('utf-8').splitlines()
        except Exception as e:
            logging.error(f"Error reading S3 file {key} from bucket {bucket}: {e}")
            return []


# Main function to read all files concurrently
async def read_all_files():
    tasks = [read_s3_file_async(input_bucket, file) for file in files_to_read]
    file_contents = await asyncio.gather(*tasks)
    return file_contents


# Converting date format
def convert_date_format(date_str):
    try:
        # Convert the date string from 'DD-MMM-YY' to 'YYYY-MM-DD'
        date_obj = datetime.strptime(date_str, "%d-%b-%y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        # Return the original string if it doesn't match the expected format
        return date_str


# Converting 1 to yes, 2 to no
def convert_to_yes_no(value):
    if value == "1":
        return "yes"
    elif value == "2":
        return "no"
    else:
        return value  # In case there are other values, return the original value


# Cleaning data
def clean_string(value):
    """Removes unwanted escape sequences and extra quotes from a JSON string."""
    return value.strip('"').replace('\\"', '')


# Step 1: Parse drug names from file
def parse_drug_names(file_content):
    logging.info("Parsing drug names...")
    drug_names = [line.strip().lower() for line in file_content if line.strip()]
    logging.info(f"Parsed {len(drug_names)} drug names.")
    return drug_names



# to rmeove duplicaates from drugnames list
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)  # Change to list for handling multiple fields
    drug_names_set = set(drug_names)

    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()
            report_id = clean_string(fields[1]).strip()
            if drug_name in drug_names_set:
                logging.debug(f"Match found for drug: {drug_name} with REPORT_ID: {report_id}")
                report_ids[report_id].append(fields)  # Append the matching fields to the list

    logging.info(f"Found {len(report_ids)} unique report IDs matching the drug names.")
    return report_ids



def extract_reports(report_ids, reports_content, report_data, lock):
    logging.info("Extracting report data from reports.txt...")
    for line in reports_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id not in report_ids:
                continue
            with lock:  # Locking to avoid concurrency issues
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

    logging.info(f"Extracted {len(report_data)} reports.")
    return report_data


import logging

def clean_string(input_string):
    # This function should clean the string as needed, e.g., stripping unnecessary whitespace, etc.
    return input_string.strip()

def extract_report_drug(report_ids, report_drug_content, report_data, lock):
    logging.info("Extracting drug data from report_drug.txt...")

    for line in report_drug_content:
        fields = line.split('$')

        # Check if there are enough fields to process
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()

            # Process the report only if the report_id exists in the report_ids list
            if report_id in report_ids:
                with lock:  # Locking to avoid concurrency issues
                    # Initialize report_data[report_id] as a dictionary with lists for each key if it doesn't exist
                    if report_id not in report_data:
                        logging.warning(f"Initializing report data for report_id: {report_id}")
                        report_data[report_id] = {
                            'drug_name': [],
                            'drug_involvement': [],
                            'route_admin': [],
                            'unit_dose_qty': [],
                            'dose_unit_eng': [],
                            'freq_time_unit_eng': [],
                            'therapy_duration': [],
                            'therapy_duration_unit_eng': []
                        }
                    else:
                        logging.info(f"Report data already initialized for report_id: {report_id}")

                    # Append the data to the respective fields
                    try:
                        # Ensure that each field exists and append the data correctly
                        report_data[report_id]['drug_name'].append(clean_string(fields[3]).strip().lower())
                        report_data[report_id]['drug_involvement'].append(clean_string(fields[4]))
                        report_data[report_id]['route_admin'].append(clean_string(fields[6]))
                        report_data[report_id]['unit_dose_qty'].append(clean_string(fields[8]))
                        report_data[report_id]['dose_unit_eng'].append(clean_string(fields[9]))
                        report_data[report_id]['freq_time_unit_eng'].append(clean_string(fields[15]))
                        report_data[report_id]['therapy_duration'].append(clean_string(fields[17]))
                        report_data[report_id]['therapy_duration_unit_eng'].append(clean_string(fields[18]))
                    except IndexError as e:
                        logging.error(f"IndexError while processing line: {line}, error: {str(e)}")
            else:
                logging.info(f"Skipping report_id {report_id} as it's not in report_ids.")

    # Convert list values to comma-separated strings
    for report_id, data in report_data.items():
        for key, value in data.items():
            if isinstance(value, list):
                report_data[report_id][key] = ', '.join(value)

    logging.info(f"Extracted drug data for {len(report_data)} reports.")
    return report_data




def extract_report_indication(report_ids, report_drug_indication_content, report_data):
    logging.info("Extracting indication data from report_drug_indication.txt...")
    indications_map = {}
    for line in report_drug_indication_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            drug_name_eng = clean_string(fields[3]).strip().lower()
            indication = clean_string(fields[4]).strip()

            if report_id in report_data:
                # with lock:  # Locking to avoid concurrency issues
                    if report_id not in indications_map:
                        indications_map[report_id] = {}
                    indications_map[report_id][drug_name_eng] = indication

    for report_id, drug_indications in indications_map.items():
        if report_id in report_data:
            drug_names = report_data[report_id].get('drug_name', '').split(', ')

            indications = []
            for drug_name in drug_names:
                drug_name = drug_name.strip().lower()
                indication = drug_indications.get(drug_name, '')
                # Append the indication or a blank if none, keeping commas for consistency
                indications.append(indication if indication else ' ')

            # with lock:  # Locking to avoid concurrency issues
                report_data[report_id]['indications'] = ', '.join(indications)

    logging.info(f"Extracted indication data for {len(report_data)} reports.")
    return report_data


def extract_report_links(report_ids, report_links_content, report_data, lock):
    logging.info("Extracting report link data from report_links.txt...")

    for line in report_links_content:
        fields = line.split('$')

        # Check if the line contains enough fields
        if len(fields) > 4:
            report_id = clean_string(fields[1]).strip()

            # Proceed only if the report_id is in the report_ids list
            if report_id in report_ids:
                record_type_eng = clean_string(fields[2]).strip()
                report_link_no = clean_string(fields[4]).strip()

                with lock:  # Locking to avoid concurrency issues
                    # If record_type_eng is empty in report_links.txt, set it to default
                    if not record_type_eng:
                        record_type_eng = 'No duplicate or linked report'

                    # If report_link_no is empty in report_links.txt, set it to default
                    if not report_link_no:
                        report_link_no = 'No duplicate or linked report'

                    # Add or update the fields in report_data
                    report_data[report_id]['record_type_eng'] = record_type_eng
                    report_data[report_id]['report_link_no'] = report_link_no

    logging.info(f"Extracted report link data for {len(report_data)} reports.")
    return report_data



def extract_reactions(report_ids, reactions_content, report_data, lock):
    logging.info("Extracting reactions data from reactions.txt...")
    for line in reactions_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[1]).strip()
            if report_id in report_ids:
                pt_name_eng = clean_string(fields[5])
                meddra_version = clean_string(fields[9])
                duration = clean_string(fields[2])
                duration_unit_eng = clean_string(fields[3])

                with lock:  # Locking to avoid concurrency issues
                    report_data[report_id].setdefault('pt_name_eng', []).append(pt_name_eng)
                    report_data[report_id].setdefault('meddra_version', []).append(meddra_version)
                    report_data[report_id].setdefault('duration', []).append(duration)
                    report_data[report_id].setdefault('duration_unit_eng', []).append(duration_unit_eng)

    # Convert list values to comma-separated strings after the loop
    for report_id, data in report_data.items():
        for key, value in data.items():
            if isinstance(value, list):
                with lock:  # Locking to avoid concurrency issues
                    report_data[report_id][key] = ','.join(value)

    logging.info(f"Extracted reactions for {len(report_data)} reports.")
    return report_data


# def extract_report_data_concurrently(report_ids, report_drug_content, reports_content, report_drug_indication_content,
#                                      report_links_content, reactions_content):
#     logging.info("Extracting report data concurrently...")
#     report_data = defaultdict(dict)
#     lock = threading.Lock()
#
#     with ThreadPoolExecutor(max_workers=5) as executor:
#         executor.submit(extract_reports, report_ids, reports_content, report_data, lock)
#         executor.submit(extract_report_drug, report_ids, report_drug_content, report_data, lock)
#         executor.submit(extract_report_indication, report_ids, report_drug_indication_content, report_data, lock)
#         executor.submit(extract_report_links, report_ids, report_links_content, report_data, lock)
#         executor.submit(extract_reactions, report_ids, reactions_content, report_data, lock)
#
#     logging.info(f"Completed extraction for {len(report_data)} reports.")
#     return report_data
# Extracting report data concurrently using ThreadPoolExecutor and handling thread safety
def extract_report_data_concurrently(report_ids, report_drug_content, reports_content, report_links_content, reactions_content):
    logging.info("Extracting report data concurrently...")
    report_data = defaultdict(dict)
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        futures.append(executor.submit(extract_reports, report_ids, reports_content, report_data, lock))
        futures.append(executor.submit(extract_report_drug, report_ids, report_drug_content, report_data, lock))
        # futures.append(executor.submit(extract_report_indication, report_ids, report_drug_indication_content, report_data, lock))
        futures.append(executor.submit(extract_report_links, report_ids, report_links_content, report_data, lock))
        futures.append(executor.submit(extract_reactions, report_ids, reactions_content, report_data, lock))

        # Waiting for all threads to complete and processing results
        for future in as_completed(futures):
            future.result()  # Ensure any exception is raised here if occurs

    logging.info(f"Completed extraction for {len(report_data)} reports.")
    return report_data

# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data.append({
            # "report_id": report_id,
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
    return final_data

    # try:
    #     json_data = json.dumps(final_data, indent=4)
    #     timestamp = time.strftime('%Y%m%d-%H%M%S')
    #     output_file = f"output_data_{timestamp}.json"
    #     s3_client.put_object(Bucket=output_bucket, Key=output_file, Body=json_data)
    #     logging.info(f"Successfully uploaded JSON file to S3: {output_file}")
    # except Exception as e:
    #     logging.error(f"Error generating or uploading JSON output: {e}")
    # return final_data


# # Save the output to S3
# def save_output_to_s3(output_data):
#     logging.info("Saving JSON output to S3...")
#     output_key = f"Output/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_output.json"
#     try:
#         s3_client = boto3.client('s3')
#         s3_client.put_object(
#             Bucket=output_bucket,
#             Key=output_key,
#             Body=json.dumps(output_data)
#         )
#         logging.info(f"Output saved to S3 at {output_key}.")
#     except Exception as e:
#         logging.error(f"Failed to save output to S3: {e}")
# Save the output to S3
def save_output_to_s3(output_data):
    logging.info("Saving JSON output to S3...")

    # Format timestamp for file name
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    output_file = f"Output/output_data_{timestamp}.json"

    try:
        # Initialize the S3 client
        s3_client = boto3.client('s3')

        # Convert the output data to JSON with indentation
        json_data = json.dumps(output_data, indent=4)

        # Upload the JSON data to S3
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_file,
            Body=json_data
        )

        logging.info(f"Successfully uploaded JSON file to S3: {output_file}")

    except Exception as e:
        logging.error(f"Error generating or uploading JSON output: {e}")

    return output_data


# Main function
async def main():
    # def main():
    logging.info("Starting report extraction process...")
    # loop = asyncio.get_event_loop()
    logging.info("Starting to read all files concurrently from S3...")
    file_contents = await read_all_files()

    drug_names_content, report_drug_content, reports_content, report_drug_indication_content, report_links_content, reactions_content = file_contents

    logging.info("Parsing drug names...")
    drug_names = parse_drug_names(drug_names_content)

    logging.info("Finding report IDs...")
    report_ids = find_report_ids(drug_names, report_drug_content)

    logging.info("Extracting report data concurrently...")
    report_data = extract_report_data_concurrently(report_ids, report_drug_content, reports_content, report_links_content,
                                                   reactions_content)


    logging.info("Processing indications...")
    report_data = extract_report_indication(report_ids, report_drug_indication_content, report_data)

    # Generate JSON output (you may already have this function for generating the JSON)
    generated_json_data = generate_json_output(report_data)

    # Call the function to save the output to S3
    logging.info("Saving JSON output to S3...")
    save_output_to_s3(generated_json_data)  # Call the function here

    logging.info(f"Completed report extraction for {len(report_data)} reports.")


if __name__ == "__main__":
    asyncio.run(main())  # Use asyncio.run() to run the async main function
