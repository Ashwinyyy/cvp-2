import boto3
import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import asyncio
import aioboto3
import threading

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
    'Input_data/report_id_database/report_drug_indication.txt'
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

# Step 2: Locate REPORT_IDs corresponding to drug names
def find_report_ids(drug_names, report_drug_content):
    logging.info(f"Finding REPORT_IDs for {len(drug_names)} drug names...")
    report_ids = defaultdict(list)
    drug_names_set = set(drug_names)  # Create a set for faster lookup
    for line in report_drug_content:
        fields = line.split('$')
        if len(fields) > 1:
            drug_name = clean_string(fields[3]).strip().lower()
            report_id = clean_string(fields[1]).strip()
            if drug_name in drug_names_set:  # Exact match
                logging.debug(f"Match found for drug: {drug_name} with REPORT_ID: {report_id}")
                report_ids[report_id].append(fields)
    logging.info(f"Found {len(report_ids)} unique report IDs matching the drug names.")
    return report_ids

# Function to extract reports.txt
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

# Function to extract report links data
def extract_report_links(report_ids, report_links_content, report_data):
    logging.info("Extracting report link data from report_links.txt...")
    for line in report_links_content:
        fields = line.split('$')
        report_id = clean_string(fields[1]).strip()
        if report_id in report_ids:
            report_data[report_id]['link'] = clean_string(fields[3])
    logging.info(f"Extracted {len(report_data)} report links.")

# Function to extract drug and indication data
def extract_drug_and_indication_data(report_ids, report_drug_indication_content, report_data, lock):
    logging.info("Extracting drug and indication data...")
    indications_map = {}  # Initialize the indications map
    with lock:
        for line in report_drug_indication_content:
            fields = line.split('$')
            if len(fields) > 1:
                report_id = clean_string(fields[0]).strip()
                indication = clean_string(fields[1]).strip()
                drug_name_eng = clean_string(fields[3]).strip().lower()

                if report_id not in report_data:
                    continue

                if report_id not in indications_map:
                    indications_map[report_id] = {}

                indications_map[report_id][drug_name_eng] = indication

        # Now update the report_data with the indications
        for report_id, drug_indications in indications_map.items():
            if report_id in report_data:
                drug_names = report_data[report_id].get('drug_name_eng', '').split(', ')
                indications = [drug_indications.get(drug_name, '') for drug_name in drug_names]
                report_data[report_id]['indication_eng'] = ', '.join(indications)
    logging.info(f"Extracted drug and indication data for {len(report_data)} reports.")

# Function to extract reactions data
def extract_reactions(report_ids, reactions_content, report_data):
    logging.info("Extracting reactions data from reactions.txt...")
    for line in reactions_content:
        fields = line.split('$')
        if len(fields) > 1:
            report_id = clean_string(fields[0]).strip()
            if report_id in report_ids:
                report_data[report_id]['adverse_reaction_eng'] = clean_string(fields[5])
    logging.info(f"Extracted reactions for {len(report_data)} reports.")

# Function to extract report data concurrently
def extract_report_data_concurrently(report_ids, report_drug_content, reports_content, report_drug_indication_content, report_links_content, reactions_content):
    logging.info("Extracting report data concurrently...")
    report_data = defaultdict(dict)
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.submit(extract_reports, report_ids, report_drug_content, report_data)
        executor.submit(extract_reports, report_ids, reports_content, report_data)
        executor.submit(extract_report_links, report_ids, report_links_content, report_data)
        executor.submit(extract_drug_and_indication_data, report_ids, report_drug_indication_content, report_data, lock)
        executor.submit(extract_reactions, report_ids, reactions_content, report_data)

    logging.info(f"Completed extraction for {len(report_data)} reports.")
    return report_data

# Step 4: Generate the JSON structure and save it to the output S3 bucket
def generate_json_output(report_data):
    logging.info("Generating JSON output...")
    final_data = []
    for report_id, data in report_data.items():
        final_data.append({
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
            "age_unit_eng": data.get('age_unit_eng', ''),
            "gender_eng": data.get('gender_eng', ''),
            "height": data.get('height', ''),
            "height_unit_eng": data.get('height_unit_eng', ''),
            "weight": data.get('weight', ''),
            "weight_unit_eng": data.get('weight_unit_eng', ''),
            "outcome_eng": data.get('outcome_eng', ''),
            "indication_eng": data.get('indication_eng', ''),
            "adverse_reaction_eng": data.get('adverse_reaction_eng', ''),
            "link": data.get('link', '')
        })
    return final_data

# Save the output to S3
# Save the output to S3
def save_output_to_s3(output_data):
    logging.info("Saving JSON output to S3...")
    output_key = f"Output/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_output.json"
    
    try:
        # Create an S3 client
        s3_client = boto3.client('s3')
        
        # Put object in the specified S3 bucket
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=json.dumps(output_data),
            ContentType='application/json'  # Optional, but sets the MIME type as JSON
        )
        
        logging.info(f"Output saved to S3 at {output_key}.")
    
    except Exception as e:
        logging.error(f"Failed to save output to S3: {e}")
        # Optionally re-raise the error if you want to handle it upstream
        raise

# Main function to orchestrate everything
def main():
    logging.info("Starting the process of reading files and generating report...")
    loop = asyncio.get_event_loop()
    file_contents = loop.run_until_complete(read_all_files())
    drug_names, report_drug_content, reports_content, report_drug_indication_content, report_links_content, reactions_content = file_contents

    # Step 1: Parse drug names
    drug_names_parsed = parse_drug_names(drug_names)

    # Step 2: Find report IDs
    report_ids = find_report_ids(drug_names_parsed, report_drug_content)

    # Step 3: Extract report data concurrently
    report_data = extract_report_data_concurrently(report_ids, report_drug_content, reports_content, report_drug_indication_content, report_links_content, reactions_content)

    # Step 4: Generate JSON output
    final_output = generate_json_output(report_data)

    # Step 5: Save the output to S3
    save_output_to_s3(final_output)
