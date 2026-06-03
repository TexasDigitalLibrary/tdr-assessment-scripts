import csv
import json
import os
import pandas as pd
import re
import requests
import sys
import time
from datetime import datetime, timedelta
from utils import assign_size_bins, extract_max_version, retrieve_all_institutions

###########################################
####           Workflow set-up         ####
###########################################

# Read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

# toggle for test environment (incomplete run, faster to complete)
test = config['TOGGLES']['test']
# toggle to only look at your/one institution in TDR
only_my_institution = config['TOGGLES']['only_my_institution'] 
# toggle for stage 3 retrieval
versions_API = config['TOGGLES']['versions_api']
# toggle for retrieving metrics from DataCite
metrics_dc = config['TOGGLES']['metrics_dc']
# toggle for retrieving metrics from Dataverse
metrics_dv = config['TOGGLES']['metrics_dv']
# toggle for excluding unpublished
exclude_drafts = config['TOGGLES']['exclude_drafts']
# toggle for excluding deaccessioned
exclude_deaccessioned = False
## this will change the query filter used in the Search API call for datasets and the filename from outputs
if exclude_drafts:
    status = 'publicationStatus:Published'
    status_filename = 'PUBLISHED'
else:
    status = ''
    status_filename = 'ALL'
# toggle to split dataset_entries_native by institution (IN DEVELOPMENT)
split_institution_output = config['TOGGLES']['split_outputs']

# setting timestamp at start of script to calculate run time
start_time = datetime.now() 
# creating variable with current date for appending to filenames
today = datetime.now().strftime('%Y%m%d') 
# cutoff for checking recency of data dump from TDL
cutoff_months = 6

# filename version of your institution's name
my_institution_filename = config['INSTITUTION']['filename']
## condition what goes in the filename based on toggle for which institution(s) to ping
if only_my_institution:
    institution_filename = my_institution_filename
else:
    institution_filename = 'all-institutions'
# short-hand version of your institution's name
my_institution_short_name = config['INSTITUTION']['myInstitution']
# root of your institution's dataverse
subtree = config['INSTITUTION']['subtree']

#######################################################################
####           Read in primary funder and affiliation maps         ####
#######################################################################

script_dir = os.getcwd()

affiliation_path = f'{script_dir}/affiliation-map-primary.csv'
if os.path.exists(affiliation_path):
    ror_map = pd.read_csv(affiliation_path)
    print('ROR affiliation map exists and has been loaded.\n')
else:
    print('ROR affiliation map does not exist.\n')

funder_path = f'{script_dir}/funder-map-primary.csv'
if os.path.exists(funder_path):
    funder_ror_map = pd.read_csv(funder_path)
    print('ROR funder map exists and has been loaded..\n')
else:
    funder_ror_map = None
    print('ROR funder map does not exist.\n')

################################################
####           Import TDL data dump         ####
################################################

# Conditionally import TDL data dump
def find_latest_folder(base_path=".", pattern="dataverse-reports-"):
    matching_folders = []
    
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        
        if os.path.isdir(folder_path) and folder.startswith(pattern):
            # Assumes this format: dataverse-reports-YYYYMMDD
            date_match = re.search(r'(\d{8})$', folder)
            
            if date_match:
                date_str = date_match.group(1)
                try:
                    folder_date = datetime.strptime(date_str, "%Y%m%d")
                    matching_folders.append((folder_path, folder_date, folder))
                except ValueError:
                    continue
    
    if not matching_folders:
        return None, None
    
    # Sort by date and return the most recent
    latest = sorted(matching_folders, key=lambda x: x[1], reverse=True)[0]
    return latest[0], latest[1]

# Identify folder and extract date
dv_report, folder_date = find_latest_folder()

if dv_report is None:
    print("No 'dataverse-reports-YYYYMMDD' folder found. Exiting script")
    sys.exit()
else:
    print(f'Found folder: {os.path.basename(dv_report)}')
    print(f'Folder date: {folder_date.strftime('%Y-%m-%d')}')
    
    # Check if folder is recent enough (within 6 months)
    cutoff_date = start_time - timedelta(days=(cutoff_months*30))
    if folder_date < cutoff_date:
        days_old = (start_time - folder_date).days
        print(f"\n WARNING: Folder is {days_old} days old (more than {cutoff_months*30} months).")
    else:
        print(f'Folder is recent (within {cutoff_months*30} months)\n')
        
    try:
        combined_datasets_df = pd.read_csv(os.path.join(dv_report, 'datasets-concatenated.csv'))
        print('Loaded existing concatenated datasets file.\n')
    except FileNotFoundError:
        # Get list of Excel files
        excel_files = [f for f in os.listdir(dv_report) if f.endswith(".xlsx")]
        datasets_list = []
        
        for file in excel_files:
            file_path = os.path.join(dv_report, file)
            
            try:
                df = pd.read_excel(file_path, sheet_name="datasets")
                institution = file.split("-")[0]
                df["institution"] = institution
                datasets_list.append(df)
                print(f'{file} - {len(df)} rows')
            except Exception as e:
                print(f'Error reading {file}: {e}')
        
        if datasets_list:
            combined_datasets_df = pd.concat(datasets_list, ignore_index=True)
            output_path = os.path.join(dv_report, "datasets-concatenated.csv")
            combined_datasets_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"Saved: {output_path} ({len(combined_datasets_df)} total rows)\n")
    
    # Process dataverses
    try:
        combined_dataverses_df = pd.read_csv(os.path.join(dv_report, 'dataverses-concatenated.csv'))
        print('Loaded existing concatenated dataverses file.\n')
    except FileNotFoundError: 
        print("Processing dataverses...")
        dataverses_list = []
        
        for file in excel_files:
            file_path = os.path.join(dv_report, file)
            
            try:
                df = pd.read_excel(file_path, sheet_name="dataverses")
                institution = file.split("-")[0]
                df["institution"] = institution
                dataverses_list.append(df)
                print(f"{file} - {len(df)} rows")
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        if dataverses_list:
            combined_dataverses_df = pd.concat(dataverses_list, ignore_index=True)
            output_path = os.path.join(dv_report, "dataverses-concatenated.csv")
            combined_dataverses_df.to_csv(output_path, index=False, encoding="utf-8-sig")
            print(f"✓ Saved: {output_path} ({len(combined_dataverses_df)} total rows)")

    # Generate pruned versions for merging
    cols_datasets_tdl_dump = ['persistentUrl', 'identifier', 'publicationDate', 'versionState', 'createTime', 'contentSize (MB)', 'totalFiles']
    combined_datasets_pruned_df = combined_datasets_df[cols_datasets_tdl_dump]
    ## Label deaccessioned datasets
    combined_datasets_pruned_df['versionState'] = combined_datasets_pruned_df['versionState'].fillna('DEACCESSIONED')


    cols_dataverses_tdl_dump = ['name', 'alias', 'id', 'dataverseType', 'contactIdentifier', 'contentSize (MB)', 'released', 'institution']
    combined_dataverses_pruned_df = combined_dataverses_df[cols_dataverses_tdl_dump]

# Check for directories, create if non-existent
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate\n')
    else:
        os.mkdir('test')
        print('test directory has been created\n')
    test_dir = os.path.join(script_dir, 'test')
    os.chdir('test')
    if os.path.isdir('outputs'):
        print('test outputs directory found - no need to recreate\n')
    else:
        os.mkdir('outputs')
        print('test outputs directory has been created\n')
    outputs_dir = os.path.join(test_dir, 'outputs')
    if os.path.isdir('logs'):
        print('test logs directory found - no need to recreate\n')
    else:
        os.mkdir('logs')
        print('test logs directory has been created\n')
    logs_dir = os.path.join(test_dir, 'logs')
else:
    if os.path.isdir('outputs'):
        print('outputs directory found - no need to recreate\n')
    else:
        os.mkdir('outputs')
        print('outputs directory has been created\n')
    outputs_dir = os.path.join(script_dir, 'outputs')
    if os.path.isdir('logs'):
        print('logs directory found - no need to recreate\n')
    else:
        os.mkdir('logs')
        print('logs directory has been created\n')
    logs_dir = os.path.join(script_dir, 'logs')

#############################################
####           Search API set-up         ####
#############################################

print('Beginning to define API call parameters.')
url_tdr = 'https://dataverse.tdl.org/api/search/'

# Variable params for test environment
if test and only_my_institution:
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] 
elif test and not only_my_institution: 
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] // 2 #halve page size if retrieving all institutions
elif not test:
    page_limit_dataset = config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size_dataset = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']

print(f'Retrieving {page_size_dataset} records per page over {page_limit_dataset} pages.')

query = '*'
page_start_dataset = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment_dataset = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0

headers_tdr = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}

params_tdr_ut_austin = {
    'q': query,
    'fq': status,
    'subtree': 'utexas',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_baylor = {
    'q': query,
    'fq': status,
    'subtree': 'baylor',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_smu = {
    'q': query,
    'fq': status,
    'subtree': 'smu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_tamu = {
    'q': query,
    'fq': status,
    'subtree': 'tamu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_txst = {
    'q': query,
    'fq': status,
    'subtree': 'txst',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_ttu = {
    'q': query,
    'fq': status,
    'subtree': 'ttu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_houston = {
    'q': query,
    'fq': status,
    'subtree': 'uh',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_hscfw = {
    'q': query,
    'fq': status,
    'subtree': 'unthsc',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_tamug = {
    'q': query,
    'fq': status,
    'subtree': 'tamug',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_tamui = {
    'q': query,
    'fq': status,
    'subtree': 'tamiu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_utsah = {
    'q': query,
    'fq': status,
    'subtree': 'uthscsa',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}
params_tdr_utswm = {
    'q': query,
    'fq': status,
    'subtree': 'utswmed',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_uta = {
    'q': query,
    'fq': status,
    'subtree': 'uta',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_twu = {
    'q': query,
    'fq': status,
    'subtree': 'twu',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

params_tdr_lamar = {
    'q': query,
    'fq': status,
    'subtree': 'lamar',
    'type': 'dataset',
    'start': page_start_dataset,
    'page': page_increment_dataset,
    'per_page': page_limit_dataset
}

all_params_datasets = {
        'UT Austin': params_tdr_ut_austin,
        'Baylor': params_tdr_baylor,
        'SMU': params_tdr_smu,
        'TAMU': params_tdr_tamu,
        'Texas State': params_tdr_txst,
        'Texas Tech': params_tdr_ttu,
        'Houston': params_tdr_houston,
        'HSC Fort Worth': params_tdr_hscfw,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui,
        'UT San Antonio Health': params_tdr_utsah,
        'UT Southwestern Medical': params_tdr_utswm,
        'Lamar': params_tdr_lamar,
        'UT Arlington': params_tdr_uta,
        "Texas Woman's University": params_tdr_twu
    }

# TAMU system-specific
tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

if only_my_institution:
    if my_institution_short_name == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_short_name: all_params_datasets[my_institution_short_name]
        }
else:
    params_list = all_params_datasets

print('Starting TDR retrieval.\n')
all_data = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataset, page_size_dataset, page_limit_dataset)

print('Starting TDR filtering.\n')
dataset_entries = []
for item in all_data:
    id = item.get('global_id', '')
    type = item.get('type', '')
    institution = item.get('institution','')
    status = item.get('versionState', '')
    first_created = item.get('createdAt', '')
    description = item.get('description', '')
    keywords = item.get('keywords', '')
    subjects = item.get('subjects', '')
    name = item.get('name', '')
    dataverse = item.get('name_of_dataverse', '')
    dataverse_code = item.get('identifier_of_dataverse', '')
    majorV = item.get('majorVersion', 0)
    minorV = item.get('minorVersion', 0)
    comboV = f'{majorV}.{minorV}'
    version_id = item.get('versionId', '')
    dataset_entries.append({
        'institution': institution, 
        'doi': id,
        # 'type': type,
        # 'description': description,
        'subjects': subjects,
        # 'keywords': keywords,
        # 'status': status,
        'created_original': first_created,
        'dataset_title': name,
        'dataverse': dataverse,
        'alias': dataverse_code,
        # 'major_version': majorV,
        # 'minor_version': minorV,
        'total_version': comboV,
        'version_id': version_id
    })

df_dataset_entries = pd.DataFrame(dataset_entries)

#####################################################
####          Process Search API dataset_entries_native         ####
#####################################################

# Ensuring full version (float not integer)
df_dataset_entries['total_version'] = df_dataset_entries['total_version'].apply(extract_max_version)
# Add Boolean for versioned
df_dataset_entries['versioned'] = df_dataset_entries.apply(lambda row: 'Versioned' if (row['total_version'] > 1.0) else 'Not versioned', axis=1)
# Clean up DOI field
df_dataset_entries['doi'] = df_dataset_entries['doi'].str.replace('doi:', '')
# Create PURL link to align with data dump files
df_dataset_entries['persistentUrl'] = 'https://doi.org/' + df_dataset_entries['doi']
# Combine with dataset-level data dump df if it exists
## Right now, the script will not reach this point if the data dump doesn't exist anyway
if combined_datasets_pruned_df is not None:
   df_dataset_entries = pd.merge(combined_datasets_pruned_df, df_dataset_entries, on='persistentUrl', how='left')

#sort on status, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED'
# df_dataset_entries = df_dataset_entries.sort_values(by='status', ascending=False)
df_dataset_entries.to_csv(f'outputs/{today}_{institution_filename}_all-deposits.csv')
# filtered_tdr_deduplicated = df_dataset_entries.drop_duplicates(subset=['identifier'], keep='first')
# filtered_tdr_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-deposits-deduplicated.csv', index=False, encoding='utf-8-sig')

# #create df of published datasets with draft version (retains both entries)
# commonColumns = ['identifier', 'dataset_title']
# duplicates = df_dataset_entries.duplicated(subset=commonColumns, keep=False)
# dual_status_datasets = df_dataset_entries[duplicates]
# dual_status_datasets.to_csv(f'outputs/{today}_{institution_filename}_dual-status-datasets.csv', index=False, encoding='utf-8-sig')

#############################################
####           Native API set-up         ####
#############################################

print('Starting Native API call')
url_tdr_native = 'https://dataverse.tdl.org/api/datasets/'
# Only retrieve DOIs that exist and can be retrieved by a liaison
## A superuser could get information on unpublished datasets, but a liaison can only get this for their institution
df_datasets_published = df_dataset_entries.dropna(subset=['doi'])
print(f'Total datasets to be analyzed: {len(df_datasets_published)}.\n')

dataset_entries_native = []
first_timeouts = []
second_timeouts = []
final_timeouts = []
for doi in df_datasets_published['doi']:
    try:
        response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving metadata for: {doi}\n')
            dataset_entries_native.append(response.json())
            time.sleep(0.2)
        else:
            final_timeouts.append({"doi": doi, "reason": f"Status {response.status_code}"})
    except requests.exceptions.Timeout:
        first_timeouts.append(doi)
    except requests.exceptions.RequestException as e:
        final_timeouts.append({"doi": doi, "reason": str(e)})

if first_timeouts:
    print(f"\n--- Retrying {len(first_timeouts)} timeouts with 5s limit ---\n")
    time.sleep(2) 
    for doi in first_timeouts:
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
            if response.status_code == 200:
                print(f'Retrying call for: {doi}\n')
                dataset_entries_native.append(response.json())
                time.sleep(0.2)
            else:
                final_timeouts.append({"doi": doi, "reason": f"Status {response.status_code}"})
        except requests.exceptions.Timeout:
            second_timeouts.append(doi)
        except requests.exceptions.RequestException as e:
            final_timeouts.append({"doi": doi, "reason": str(e)})

if second_timeouts:
    print(f"\n--- Retrying {len(second_timeouts)} repeat timeouts with 10s limit ---\n")
    time.sleep(2) 
    for doi in second_timeouts:
        try:
            response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=10)
            if response.status_code == 200:
                print(f'Retrying call for {doi} again\n')
                dataset_entries_native.append(response.json())
                time.sleep(0.2)
            else:
                final_timeouts.append({"doi": doi, "reason": f"Retry Status {response.status_code}"})
        except Exception as e:
            final_timeouts.append({"doi": doi, "reason": f"Persistent Timeout/Error: {e}"})

print('Done retrieving dataset_entries_native\n')

data_tdr_native = {
    'datasets': dataset_entries_native
}

print(f"INITIALLY FAILED: {len(first_timeouts)}\n")
print(f"TOTAL FAILED: {len(final_timeouts)}\n")
if len(final_timeouts) > 0:
    print(final_timeouts)

# Saving failed retrievals
with open(f'{logs_dir}/{today}_failed-retrievals.csv', 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['Date', 'DOI', 'Error Message']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    
    writer.writeheader()
    for item in final_timeouts:
        writer.writerow({
            'Date': today,
            'DOI': item['doi'],
            'Error Message': item['reason']
        })

print('Beginning dataframe subsetting\n')
file_entries = [] 
author_entries = []

for item in data_tdr_native['datasets']:
    data = item.get('data', '')
    dataset_id = data.get('id', '')
    pubDate = data.get('publicationDate', '')
    latest = data.get('latestVersion', {})
    status = latest.get('versionState', '')
    status2 = latest.get('latestVersionPublishingState', '')
    doi = latest.get('datasetPersistentId', '')
    updateDate = latest.get('lastUpdateTime', '')
    createDate = latest.get('createTime', '')
    releaseDate = latest.get('releaseTime', '')
    license = latest.get('license', {})
    licenseName = license.get('name', None)
    terms = latest.get('termsOfUse', None)
    usage = licenseName if licenseName is not None else terms
    confidentiality = latest.get('confidentialityDeclaration', None)
    permission = latest.get('specialPermissions', None)
    restrictions = latest.get('restrictions', None)
    requirements = latest.get('depositorRequirements', None)
    conditions = latest.get('conditions', None)
    disclaimer = latest.get('disclaimer', None)
    terms_access = latest.get('termsOfAccess', None)
    data_access_place = latest.get('dataAccessPlace', None)
    availability = latest.get('availabilityStatus', None)
    contact_access = latest.get('contactForAccess', None)
    files = latest.get('files', [])
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    fields = citation.get('fields', [])
    grant_agencies = 'No funding listed'
    keywords = None
    notes = None
    depositor = 'None listed'
    contacts = 'None listed'
    contact_emails = 'None listed'
    for field in fields:
        if field['typeName'] == 'grantNumber':
            grant_agencies = []
            for grant in field.get('value', []):
                grant_number_agency = grant.get('grantNumberAgency', {}).get('value', '')
                grant_agencies.append(grant_number_agency)
            grant_agencies = '; '.join(grant_agencies)
        if field['typeName'] == 'subject':
            subjects = field.get('value', [])
        if field['typeName'] == 'notesText':
            notes = field.get('value', '')
        if field['typeName'] == 'keyword':
            keywords = []
            for keyword_dict in field.get('value', []):
                keyword_value = keyword_dict.get('keywordValue', {}).get('value', '')
                if keyword_value:
                    keywords.append(keyword_value)
            keywords_str = '; '.join(keywords)
        if field['typeName'] == 'datasetContact':
            contacts = []
            contact_emails = []
            for contact in field.get('value', []):
                contact_value = contact.get('datasetContactName', {}).get('value', '')
                contact_email_value = contact.get('datasetContactEmail', {}).get('value', '')
                if contact_value:
                    contacts.append(contact_value)
                if contact_email_value:
                    contact_emails.append(contact_email_value)
            contacts = '; '.join(contacts) 
            contact_emails = '; '.join(contact_emails)
        if field['typeName'] == 'depositor':
            depositor = field.get('value', '')
    num_authors = 0
    for field in fields:
        if field['typeName'] == 'author':
            for position, author in enumerate(field.get('value', []), start=1):
                num_authors += 1
    total_filesize = 0
    unique_content_types = set()
    fileCount = len(files)
    base_entry = {
        'dataset_id': dataset_id,
        'doi': doi,
        # 'notes': notes,
        'funders': grant_agencies,
        'dataset_contact': contacts,
        'dataset_email': contact_emails,
        'dataset_depositor': depositor,
        # 'current_status': status2,
        # 'reuse_requirements': usage,
        'license': licenseName,
        # 'confidentiality': confidentiality,
        # 'permission': permission,
        # 'restrictions': restrictions,
        # 'requirements': requirements,
        # 'conditions': conditions,
        # 'disclaimer': disclaimer,
        # 'terms_access': terms_access,
        # 'data_access_place': data_access_place,
        # 'availability': availability,
        # 'contact_access': contact_access
    }
    if files:
        for file in files:
            file_info = file.get('dataFile', {})
            file_entry = base_entry.copy()
            file_entry.update({
                'file_id': file_info.get('id', ''),
                'filename': file_info.get('filename', ''),
                # 'mime_type': file_info.get('contentType', ''),
                # 'friendly_type': file_info.get('friendlyType', ''),
                'original_mime_type': file_info.get('originalFileFormat', file_info.get('contentType', '')),
                # 'original_friendly_type': file_info.get('originalFormatLabel', file_info.get('friendlyType', '')),
                # 'tabular': file_info.get('tabularData', ''),
                'file_size': file_info.get('filesize', 0),
                # 'original_file_size': file_info.get('originalFileSize', 0),
                'storage_identifier': file_info.get('storageIdentifier', ''),
                'file_creation_date': file_info.get('creationDate', ''),
                'file_publication_date': file_info.get('publicationDate', ''),
                'restricted': file.get('restricted', ''),
            })
            file_entries.append(file_entry)
    else:
        file_entry = base_entry.copy()
        file_entry.update({
            'file_id': 'NO FILES',
            'filename': 'NO FILES',
            # 'mime_type': 'NO FILES',
            # 'friendly_type': 'NO FILES',
            'original_mime_type': 'NO FILES',
            # 'original_friendly_type': 'NO FILES',
            # 'tabular': 'NO FILES',
            'file_size': 0,
            # 'original_file_size': 'NO FILES',
            'storage_identifier': 'NO FILES',
            'file_creation_date': None,
            'file_publication_date': None,
            'restricted': 'NO FILES',
        })
        file_entries.append(file_entry)

# df with entries for individual authors
for item in data_tdr_native['datasets']:
    data = item.get('data', {})
    latest = data.get('latestVersion', {})
    doi = latest.get('datasetPersistentId', '')
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    status2 = latest.get('latestVersionPublishingState', '')
    fields = citation.get('fields', [])
    for field in fields:
        if field['typeName'] == 'author':
            num_authors = len(field.get('value', []))
            for position, author in enumerate(field.get('value', []), start=1):
                name = author.get('authorName', {}).get('value', '')
                affiliation = author.get('authorAffiliation', {}).get('value', '')
                identifier = author.get('authorIdentifier', {}).get('value', '')
                scheme = author.get('authorIdentifierScheme', {}).get('value', '')
                affiliation_expanded = author.get('authorAffiliation', {}).get('expandedvalue', {}).get('termName', '')
                identifier_expanded = author.get('authorIdentifier', {}).get('expandedvalue', {}).get('@id', '')

                affiliationName = affiliation_expanded if affiliation_expanded else affiliation
                affiliation_ror = affiliation if affiliation_expanded else None

                author_entry = {
                    'doi': doi,
                    'current_status': status2,
                    'author_name': name,
                    'author_affiliation': affiliationName,
                    'ror_id': affiliation_ror,
                    'author_identifier': identifier,
                    'author_identifier_expanded': identifier_expanded,
                    'author_identifier_scheme': scheme,
                    'author_count': num_authors,
                    'author_position': position
                }
                author_entries.append(author_entry)

df_file_entries = pd.json_normalize(file_entries)
df_author_entries = pd.json_normalize(author_entries)

#####################################################
####          Process Native API dataset_entries_native         ####
#####################################################

# Clean up DOI field
df_file_entries['doi'] = df_file_entries['doi'].str.replace('doi:', '')
df_author_entries['doi'] = df_author_entries['doi'].str.replace('doi:', '')
# Reformatting dates
df_file_entries['file_creation_date'] = pd.to_datetime(df_file_entries['file_creation_date'])
df_file_entries['file_creation_year'] = pd.to_datetime(df_file_entries['file_creation_date'], format='%Y-%m-%dT%H:%M:%SZ').dt.year
df_file_entries['file_publication_year'] = pd.to_datetime(df_file_entries['file_publication_date'], format='%Y-%m-%d').dt.year
df_file_entries['category_mime_type'] = df_file_entries['original_mime_type'].str.extract(r'(\w+)/')

df_file_entries = assign_size_bins(df_file_entries, column='file_size', new_column='file_size_bin')
df_files_datasets = pd.merge(df_dataset_entries, df_file_entries, on='doi', how='right')

# Group columns for pruning dfs
core_dataset_cols = ['institution', 'dataverse', 'alias', 'dataset_title', 'doi', 'dataset_id', 'dataset_contact','dataset_email','dataset_depositor','license', 'subjects', 'funders', 'total_version']
core_file_cols = ['filename', 'file_id', 'original_mime_type', 'file_size', 'storage_identifier', 'file_creation_date', 'file_publication_date', 'created_original', 'file_size_bin', 'restricted']
accessory_dataset_cols = ['notes', 'keywords', 'current_status']
accessory_file_cols = ['reuse_requirements', 'confidentiality', 'permission', 'restrictions', 'conditions', 'disclaimer', 'terms_access', 'data_access_place', 'availability', 'contact_access']

#####################################################
####          Preparing for Versions API         ####
#####################################################

if versions_API:
    # Subset to datasets that are less than version 2.0 (no major update = no file additions)
    ## Note that superusers can overwrite an existing version, so this is not foolproof
    df_files_datasets_majorVersion = df_files_datasets[df_files_datasets['major_version'] > 1]
    # Remove datasets that have never been published (will not return any info for this endpoint)
    df_files_datasets_published = df_files_datasets_majorVersion[df_files_datasets_majorVersion['publication_date'].notnull()]
    # Deduplicate on dataset_id
    df_files_datasets_published_dedup = df_files_datasets_published.drop_duplicates(subset='dataset_id', keep='first')

    dataset_results_versions = []
    print('Beginning Version API query\n')
    for dataset_id in df_files_datasets_published_dedup['dataset_id']:
        try:
            response = requests.get(f'{url_tdr_native}{dataset_id}/versions')
            if response.status_code == 200:
                print(f'Retrieving versions of dataset #{dataset_id}')
                print()
                dataset_results_versions.append(response.json())
                time.sleep(0.2)
            else:
                print(f'Error retrieving dataset #{dataset_id}: {response.status_code}, {response.text}')
        except requests.exceptions.RequestException as e:
            print(f'Timeout error on DOI {doi}: {e}')

    data_tdr_versions = {
        'datasets': dataset_results_versions
    }
    print('Beginning dataframe subsetting\n')

    dataset_entries_versions = [] 
    for dataset in data_tdr_versions['datasets']:
        data = dataset.get('data', [])
        for item in data:
            doi = item.get('datasetPersistentId', '')
            id = item.get('id', '')
            datasetid = item.get('datasetId', '')
            majorV = str(item.get('versionNumber', 0))
            minorV = str(item.get('versionMinorNumber', 0))
            status2 = latest.get('latestVersionPublishingState', '')
            comboV = f'{majorV}.{minorV}'
            status = item.get('versionState', '')
            license = item.get('license', {})
            licenseName = license.get('name', None)
            terms = item.get('termsOfUse', None)
            confidentiality = item.get('confidentialityDeclaration', None)
            permission = item.get('specialPermissions', None)
            restrictions = item.get('restrictions', None)
            requirements = item.get('depositorRequirements', None)
            conditions = item.get('conditions', None)
            disclaimer = item.get('disclaimer', None)
            terms_access = item.get('termsOfAccess', None)
            data_access_place = item.get('dataAccessPlace', None)
            availability = item.get('availabilityStatus', None)
            contact_access = item.get('contactForAccess', None)
            usage = licenseName if licenseName is not None else terms
            citation = latest.get('metadataBlocks', {}).get('citation', {})
            files = item.get('files', [])
            keywords = None
            notes = None
            fields = citation.get('fields', [])
            for field in fields:
                if field['typeName'] == 'subject':
                    subjects = field.get('value', [])
                if field['typeName'] == 'notesText':
                    notes = field.get('value', [])
                if field['typeName'] == 'keyword':
                    keywords = []
                    for keyword_dict in field.get('value', []):
                        keyword_value = keyword_dict.get('keywordValue', {}).get('value', '')
                        if keyword_value:
                            keywords.append(keyword_value)
                    keywords_str = ';'.join(keywords)
                if field['typeName'] == 'datasetContact':
                    contacts = []
                    for contact in field.get('value', []):
                        contact_value = contact.get('datasetContactName', {}).get('value', '')
                        if contact_value:
                            contacts.append(contact_value)
                    contacts = ';'.join(contacts)
            if files:
                for file in files:
                    file_info = file['dataFile']
                    unique_content_types.add(file_info['contentType'])
                    file_entry = {
                        'dataset_id': dataset_id,
                        'doi': doi,
                        'notes': notes,
                        'dataset_contact': contacts,
                        'dataset_email': contact_emails,
                        'dataset_depositor': depositor,
                        #'status': status,
                        'current_status': status2,
                        'reuse_requirements': usage,
                        # 'keywords': keywords,
                        #'fileCount': fileCount,
                        #'unique_content_types': list(unique_content_types),
                        'file_id': file_info.get('id', ''),
                        'public': file_info.get('restricted', ''),
                        'filename': file_info.get('filename', ''),
                        'mime_type': file_info.get('contentType', ''),
                        'friendly_type': file_info.get('friendlyType', ''),
                        'original_mime_type': file_info.get('originalFileFormat', file_info.get('contentType', '')), #falls back to contentType if already original
                        'original_friendly_type': file_info.get('originalFormatLabel', file_info.get('friendlyType', '')), #falls back to friendlyType if already original
                        'tabular': file_info.get('tabularData', ''),
                        'file_size': file_info.get('filesize', 0),
                        'original_file_size': file_info.get('originalFileSize', 0),
                        'storage_identifier': file_info.get('storageIdentifier', ''),
                        'creation_date': file_info.get('creationDate', ''),
                        'publication_date': file_info.get('publicationDate', ''),
                        # 'publication_day': get_day_of_week(pubDate),
                        # 'is_holiday': is_us_federal_holiday(pubDate),
                        'restricted': file.get('restricted', ''),
                        'license': licenseName,
                        'confidentiality': confidentiality,
                        'permission': permission,
                        'restrictions': restrictions,
                        'requirements': requirements,
                        'conditions': conditions,
                        'disclaimer': disclaimer,
                        'terms_access': terms_access,
                        'data_access_place': data_access_place,
                        'availability': availability,
                        'contact_access': contact_access
                    }
                    dataset_entries_versions.append(file_entry)
            else:
                file_entry = {
                    'dataset_id': dataset_id,
                    'doi': doi,
                    'dataset_contact': contacts,
                    'dataset_email': contact_emails,
                    'dataset_depositor': depositor,
                    'current_status': status2,
                    'reuse_requirements': usage,
                    'file_id': 'NO FILES',
                    'public': 'NO FILES',
                    'filename': 'NO FILES',
                    'mime_type': 'NO FILES',
                    'friendly_type': 'NO FILES',
                    'original_mime_type': 'NO FILES',
                    'original_friendly_type': 'NO FILES',
                    'tabular': 'NO FILES',
                    'file_size': 'NO FILES',
                    'original_file_size': 'NO FILES',
                    'storage_identifier': 'NO FILES',
                    'creation_date': None,
                    'publication_date': None,
                    'restricted': 'NO FILES',
                    'license': licenseName,
                    'confidentiality': confidentiality,
                    'permission': permission,
                    'restrictions': restrictions,
                    'requirements': requirements,
                    'conditions': conditions,
                    'disclaimer': disclaimer,
                    'terms_access': terms_access,
                    'data_access_place': data_access_place,
                    'availability': availability,
                    'contact_access': contact_access
                }
                dataset_entries_versions.append(file_entry)
            
    #getting dataframe with entries for individual authors
    author_entries_versions = []
    for dataset in data_tdr_versions['datasets']:
        data = dataset.get('data', [])
        for item in data:
            doi = item.get('datasetPersistentId', '')
            id = item.get('id', '')
            status2 = item.get('latestVersionPublishingState', '')
            datasetid = item.get('datasetId', '')
            citation = item.get('metadataBlocks', {}).get('citation', {})
            fields = citation.get('fields', [])
            for field in fields:
                if field['typeName'] == 'author':
                    for author in field.get('value', []):
                        name = author.get('authorName', {}).get('value', '')
                        affiliation = author.get('authorAffiliation', {}).get('value', '')
                        identifier = author.get('authorIdentifier', {}).get('value', '')
                        scheme = author.get('authorIdentifierScheme', {}).get('value', '')
                        affiliation_expanded = author.get('authorAffiliation', {}).get('expandedvalue', {}).get('termName', '')
                        identifier_expanded = author.get('authorIdentifier', {}).get('expandedvalue', {}).get('@id', '')

                        affiliationName = affiliation_expanded if affiliation_expanded else affiliation
                        affiliation_ror = affiliation if affiliation_expanded else None

                        author_entry = {
                            'doi': doi,
                            'current_status': status2,
                            'author_name': name,
                            'author_affiliation': affiliationName,
                            'ror_id': affiliation_ror,
                            'author_identifier': identifier,
                            'author_identifier_expanded': identifier_expanded,
                            'author_identifier_scheme': scheme
                        }
                        author_entries_versions.append(author_entry)

    df_file_entries_versions = pd.json_normalize(dataset_entries_versions)
    df_author_entries_versions = pd.json_normalize(author_entries_versions)

    #######################################################
    ####          Process Versions API dataset_entries_native         ####
    #######################################################

    # Clean up DOI field
    df_file_entries_versions['doi'] = df_file_entries_versions['doi'].str.replace('doi:', '')
    df_author_entries_versions['doi'] = df_author_entries_versions['doi'].str.replace('doi:', '')
    # Removing duplicate entries for a given file that has not changed across multiple versions
    df_file_entries_versions['total_version'] = df_file_entries_versions['total_version'].astype(float)
    df_file_entries_versions['total_version'] = df_file_entries_versions['total_version'].apply(extract_max_version)
    df_file_entries_versions = df_file_entries_versions.sort_values(by='total_version')
    df_file_entries_versions_deduplicated = df_file_entries_versions.drop_duplicates(subset=['dataset_id', 'storage_identifier'], keep='first')

    df_file_entries_versions_deduplicated = assign_size_bins(df_file_entries_versions_deduplicated, column='file_size', new_column='file_size_bin')

    # df_select_versions_concatenated_released = pd.merge(df_file_entries_versions_deduplicated, df_dataset_entries, on='doi', how='left')

    df_file_entries_versions_pruned = df_file_entries_versions_deduplicated[core_file_cols]
    df_file_entries_versions_pruned = df_file_entries_versions_pruned.rename(columns={'total_version_x': 'total_version', 'filename_x': 'filename', 'file_size_x': 'file_size', 'storage_identifier_x': 'storage_identifier', 'creation_date_x': 'creation_date', 'publication_date_x':'publication_date'})
    df_file_entries_versions_pruned['creation_year'] = pd.to_datetime(df_file_entries_versions_pruned['creation_date'], format='%Y-%m-%d').dt.year
    df_file_entries_versions_pruned['publication_year'] = pd.to_datetime(df_file_entries_versions_pruned['publication_date'], format='%Y-%m-%d').dt.year

    df_file_entries_combined = pd.concat([df_file_entries_versions_pruned, df_files_datasets], ignore_index=True)
    df_file_entries_combined = df_file_entries_combined.rename(columns={'title': 'dataset_title'})

    #deduplicate
    ##create fake versionID for drafts to ensure proper sorting and deduplicating
    df_file_entries_combined['version_id'] = df_file_entries_combined['version_id'].fillna(9999999)
    df_file_entries_combined['version_id'] = pd.to_numeric(df_file_entries_combined['version_id'], errors='coerce')
    df_file_entries_combined = df_file_entries_combined.sort_values(by='version_id')
    df_file_entries_combined_deduplicated = df_file_entries_combined.drop_duplicates(subset=['doi', 'storage_identifier'], keep='first')
    df_file_entries_combined_deduplicated = df_file_entries_combined_deduplicated.copy()
    df_file_entries_combined_deduplicated['version_id'] = df_file_entries_combined_deduplicated['version_id'].replace(9999999, None)
    df_author_entries_combined = pd.concat([df_author_entries, df_author_entries_versions], ignore_index=True)
    df_author_entries_combined_deduplicated = df_author_entries_combined.drop_duplicates(subset=['doi', 'author_name', 'author_affiliation', 'current_status'], keep='first')
else:
    #sort on status and then total version, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED' and then to keep the earliest version
    df_files_datasets = df_files_datasets.sort_values(by=['versionState', 'total_version'], ascending=[False, True])
    df_file_entries_combined_deduplicated = df_files_datasets.drop_duplicates(subset=['doi', 'storage_identifier'], keep='first')
    df_author_entries_combined_deduplicated = df_author_entries.drop_duplicates(subset=['doi', 'author_name', 'author_affiliation', 'current_status'], keep='first')

#######################################################
####          Process file-level output df         ####
#######################################################

# File assessment
## Three kinds of documentation
df_file_entries_combined_deduplicated.loc[:,'contains_readme'] = df_file_entries_combined_deduplicated['filename'].str.contains('readme|read_me', case=False)
df_file_entries_combined_deduplicated.loc[:,'contains_codebook'] = df_file_entries_combined_deduplicated['filename'].str.contains('codebook', case=False)
df_file_entries_combined_deduplicated.loc[:,'contains_data_dictionary'] = df_file_entries_combined_deduplicated['filename'].str.contains('dictionary', case=False) 
## Create Boolean for documentation
### Need a mask to handle when there are blanks (either due to testing or to unpublished datasets)
mask = (
    df_file_entries_combined_deduplicated['contains_readme'].notna() &
    df_file_entries_combined_deduplicated['contains_codebook'].notna() &
    df_file_entries_combined_deduplicated['contains_data_dictionary'].notna()
)

df_file_entries_combined_deduplicated.loc[mask, 'has_documentation'] = (
    ~df_file_entries_combined_deduplicated.loc[mask, 'contains_readme'] & 
    ~df_file_entries_combined_deduplicated.loc[mask, 'contains_codebook'] &
    ~df_file_entries_combined_deduplicated.loc[mask, 'contains_data_dictionary']
)

## Create new friendlyFormat column from manually created map
formatMap = config['FORMAT_MAP']
df_file_entries_combined_deduplicated.loc[:,'friendly_format_manual'] = df_file_entries_combined_deduplicated['original_mime_type'].apply(
    lambda x: formatMap.get(x.strip(), x.strip()) if isinstance(x, str) and x != 'no match found' else 'no files'
)
## Export CSV with list of files that didn't match
df_new_formats = df_file_entries_combined_deduplicated[df_file_entries_combined_deduplicated['friendly_format_manual'].str.contains('/')]
df_new_formats.to_csv('new-file-formats.csv')
print(f'There are {len(df_new_formats)} mimetypes that need to be matched.\n')

## Identify certain file formats
software_formats = set(config['SOFTWARE_FORMATS'].keys())
compressed_formats = set(config['COMPRESSED_FORMATS'].keys())
microsoft_formats = set(config['MICROSOFT_FORMATS'].keys())
## Create Boolean for these categories
df_file_entries_combined_deduplicated.loc[:,'contains_software'] = df_file_entries_combined_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in software_formats for part in x.split(';')) if isinstance(x, str) else False
)
df_file_entries_combined_deduplicated.loc[:,'contains_compressed'] = df_file_entries_combined_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in compressed_formats for part in x.split(';')) if isinstance(x, str) else False
)
df_file_entries_combined_deduplicated.loc[:,'contains_microsoft_office'] = df_file_entries_combined_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in microsoft_formats for part in x.split(';')) if isinstance(x, str) else False
)

# ## Manual file extension extraction
# df_file_entries_combined_deduplicated['extension_minimum'] = df_file_entries_combined_deduplicated['filename'].str.extract(r'(\.[^.]+)$')
# df_file_entries_combined_deduplicated['extension_maximum'] = df_file_entries_combined_deduplicated['filename'].str.extract(r'(\..*)')

# Prune out unnecessary columns
file_cols = ['institution', 'doi', 'total_version', 'filename', 'file_id', 'original_mime_type', 'original_friendly_type', 'file_size', 'storage_identifier', 'creation_date', 'publication_date', 'created_original', 'file_size_bin', 'restricted', 'creation_year', 'publication_year', 'contains_readme', 'contains_codebook', 'contains_data_dictionary', 'has_documentation', 'friendly_format_manual', 'contains_software', 'contains_compressed', 'contains_microsoft_office']

df_file_entries_combined_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-files-deduplicated-{status_filename}.csv', index=False, encoding='utf-8-sig')

# Combining files to dataset-level records
## Define column to add
sum_columns = ['file_size']

def agg_func(column_name):
    if column_name in sum_columns:
        return 'sum'
    else:
        return lambda x: sorted(set(map(str, x)))

agg_funcs = {col: agg_func(col)for col in df_file_entries_combined_deduplicated.columns if col != 'dataset_id'}

df_file_entries_aggregated = df_file_entries_combined_deduplicated.groupby('dataset_id').agg(agg_funcs).reset_index()
# Convert all list-type columns to comma-separated strings
for col in df_file_entries_aggregated.columns:
    if df_file_entries_aggregated[col].apply(lambda x: isinstance(x, list)).any():
        df_file_entries_aggregated[col] = df_file_entries_aggregated[col].apply(lambda x: '; '.join(map(str, x)))
df_dataset_entries_aggregated = df_file_entries_aggregated.drop_duplicates(subset='dataset_id', keep='first')

# Standardize entries where aggregation returned a mixed 'False;True'
## May want to retain mixed strings in some cases; not all possible columns listed
def normalize_boolean_column(col):
    return col.apply(lambda x: True if isinstance(x, str) and 'true' in x.lower() else False)
bool_columns = ['contains_readme', 'contains_codebook', 'contains_data_dictionary', 'contains_software', 'contains_compressed', 'contains_microsoft_office', 'has_documentation']
df_dataset_entries_aggregated = df_dataset_entries_aggregated.copy()
for col in bool_columns:
    df_dataset_entries_aggregated[col] = normalize_boolean_column(df_dataset_entries_aggregated[col])
df_dataset_entries_aggregated = df_dataset_entries_aggregated.rename(columns={'file_size': 'dataset_size'})
# Bin datasets by manually calculated size
df_dataset_entries_aggregated = assign_size_bins(df_dataset_entries_aggregated, column='dataset_size', new_column='dataset_size_bin')

df_dataset_entries_aggregated.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-{status_filename}.csv', index=False, encoding='utf-8-sig')
cols_files_aggregated = ['filename', 'file_id', 'original_mime_type', 'dataset_size', 'dataset_size_bin', 'storage_identifier', 'file_creation_date', 'file_publication_date', 'created_original', 'file_size_bin', 'restricted']
df_dataset_entries_aggregated_pruned = df_dataset_entries_aggregated[core_dataset_cols+cols_files_aggregated+['dataset_size_bin', 'persistentUrl']]

df_dataset_entries_enriched = pd.merge(df_dataset_entries, df_dataset_entries_aggregated_pruned, on='persistentUrl', how='left')

#Dropping newly aggregated strings that are no longer useful
df_dataset_entries_enriched = df_dataset_entries_enriched.drop(columns=['file_id', 'filename', 'original_mime_type', 'storage_identifier', 'file_creation_date', 'file_publication_date', 'created_original_y', 'file_size_bin', 'doi_y', 'dataverse_y', 'alias_y'])
## Rename deduplicated cols
df_dataset_entries_enriched = df_dataset_entries_enriched.rename(columns={'created_original_x': 'created_original', 'doi_x': 'doi', 'dataverse_x': 'dataverse', 'alias_x': 'alias'})

# Splitting by institution, only writes published datasets
if split_institution_output and not only_my_institution:
    column = 'institution'
    output_dir = 'by-institution'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for unique_value, df in df_dataset_entries_aggregated.groupby(column):
        filename = f"{output_dir}/{unique_value.replace(' ', '_')}_datasets-combined-PUBLISHED.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Saved {filename}")

#######################################################
####          Search API (for collections)         ####
#######################################################

print('Beginning to define API call parameters.')

##(re)set API-specific params
page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] if test else config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size_dataverse = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']
print(f'Retrieving {page_size_dataverse} dataverses per page over {page_limit_dataverse} pages.\n')

###for TDR, affiliation is not reliable for returning all relevant dataset_entries_native
query = '*'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment_dataverse = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0

query = '*'
params_tdr_ut_austin = {
    'q': query,
    'subtree': 'utexas',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'subtree': 'baylor',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'subtree': 'smu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'subtree': 'tamu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'subtree': 'txst',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'subtree': 'ttu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'subtree': 'uh',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'subtree': 'unthsc',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'subtree': 'tamug',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'subtree': 'tamiu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'subtree': 'uthscsa',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'subtree': 'utswmed',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'subtree': 'uta',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'subtree': 'twu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment_dataverse,
    'per_page': page_limit_dataverse
}

all_params_dataverses = {
        'UT Austin': params_tdr_ut_austin,
        'Baylor': params_tdr_baylor,
        'SMU': params_tdr_smu,
        'TAMU': params_tdr_tamu,
        'Texas State': params_tdr_txst,
        'Texas Tech': params_tdr_ttu,
        'Houston': params_tdr_houston,
        'HSC Fort Worth': params_tdr_hscfw,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui,
        'UT San Antonio Health': params_tdr_utsah,
        'UT Southwestern Medical': params_tdr_utswm,
        'UT Arlington': params_tdr_uta,
        "Texas Woman's University": params_tdr_twu
    }

tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

#substitute for your institution
if only_my_institution:
    if my_institution_short_name == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_short_name: all_params_dataverses[my_institution_short_name]
        }
else:
    params_list = all_params_dataverses

print('Starting TDR retrieval.\n')
all_dataverses = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataverse, page_size_dataverse, page_limit_dataverse)

print('Starting TDR filtering.\n')
dataverses_select_tdr = []
for item in all_dataverses:
    name = item.get('name', '')
    url = item.get('url','')
    identifier = item.get('identifier', '')
    description = item.get('description', '')
    published = item.get('published_at', '')
    status = item.get('publicationStatuses', '')
    affiliation = item.get('affiliation', '')
    parent_dataverse_name = item.get('parentDataverseName', '')
    parent_dataverse_id = item.get('parentDataverseIdentifier', '')
    institution = item.get('institution', '')  
    dataverses_select_tdr.append({
        # 'institution': institution,
        'dataverse_name': name, 
        'url': url,
        'identifier': identifier,
        # 'status': status,
        # 'description': description,
        'published': published,
        # 'affiliation': affiliation,
        'parent_dataverse_name': parent_dataverse_name,
        'parent_dataverse_id': parent_dataverse_id
    })

df_dataverses_select_tdr = pd.DataFrame(dataverses_select_tdr)

#######################################################
####          Native API (for collections)         ####
#######################################################

print('Starting Native API call\n')
url_tdr_native = 'https://dataverse.tdl.org/api/dataverses/'

dataverse_entries = []
initial_timeouts_dv = []
final_timeouts_dv = []
for identifier in df_dataverses_select_tdr['identifier']:
    try:
        response = requests.get(f'{url_tdr_native}/{identifier}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving dataverse: {identifier}\n')
            dataverse_entries.append(response.json())
            time.sleep(0.2)
        else:
            final_timeouts_dv.append({"dataverse": identifier, "reason": f"Status {response.status_code}"})
    except requests.exceptions.Timeout:
        initial_timeouts_dv.append(identifier)
    except requests.exceptions.RequestException as e:
        final_timeouts_dv.append({"identifier": identifier, "reason": str(e)})

if initial_timeouts_dv:
    print(f"\n--- Retrying {len(initial_timeouts_dv)} timeouts with 10s limit ---\n")
    time.sleep(2) 
    
    for identifier in initial_timeouts_dv:
        try:
            response = requests.get(f'{url_tdr_native}/{identifier}', headers=headers_tdr, timeout=10 )
            if response.status_code == 200:
                print(f'Retrying dataverse: {identifier}\n')
                dataset_entries_native.append(response.json())
                time.sleep(0.2)
            else:
                final_timeouts_dv.append({"identifier": identifier, "reason": f"Retry Status {response.status_code}"})
        except Exception as e:
            final_timeouts_dv.append({"identifier": identifier, "reason": f"Persistent Timeout/Error: {e}"})

dataverse_entries_native = {
    'dataverses': dataverse_entries
}

print(f"INITIALLY FAILED: {len(initial_timeouts_dv)}\n")
print(f"TOTAL FAILED: {len(final_timeouts_dv)}\n")
print(final_timeouts_dv)

print('Beginning dataframe subsetting\n')
collection_entries = [] 
for item in dataverse_entries_native['dataverses']:
    data = item.get('data')
    id = data.get('id', '')
    contacts = data.get('dataverseContacts', [])
    dataverse_contact = [contact.get('contactEmail', None) for contact in contacts if isinstance(contact, dict)]
    contactsCount = len(dataverse_contact)
    permission = data.get('permissionRoot', '')
    dataverse_type = data.get('dataverseType', '')
    metadata_block = data.get('isMetadataBlockRoot', '')
    facet_root = data.get('isFacetRoot', '')
    owner = data.get('ownerId', '')
    created = data.get('creationDate', '')
    identifier = data.get('alias', '')
    released = data.get('isReleased', '')
    collection_entries.append({
        'id': id,
        'dataverse_contact': dataverse_contact,
        # 'contact_count': contactsCount, 
        'owner': owner,
        # 'dataverse_type': dataverse_type,
        # 'permission': permission,
        # 'metadata_block': metadata_block,
        # 'facet_root': facet_root,
        'dataverse_creation_date': created,
        'identifier': identifier,
        # 'released': released
    })

df_collection_entries = pd.json_normalize(collection_entries)

##################################################
####          Contents of collections         ####
##################################################

url_contents = 'https://dataverse.tdl.org/api/dataverses/{}/contents'
url_storagesize = 'https://dataverse.tdl.org/api/dataverses/{}/storagesize'

contents_results = []
storagesize_results = []

print(f'Retrieving additional information on {len(df_dataverses_select_tdr)} dataverses.\n')

for identifier in df_dataverses_select_tdr['identifier']:
    try:
        response = requests.get(url_contents.format(identifier), headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving contents for {identifier}\n')
            contents_results.append(response.json())
            time.sleep(0.2)
        else:
            print(f'Error retrieving contents for {identifier}: {response.status_code}, {response.text}\n')
            contents_results.append(None)
    except requests.exceptions.RequestException as e:
        print(f'Timeout error on contents {identifier}: {e}\n')
        contents_results.append(None)
    
    # # Get storagesize
    # try:
    #     response = requests.get(url_storagesize.format(identifier), headers=headers_tdr, timeout=5)
    #     if response.status_code == 200:
    #         storagesize_results.append(response.json())
    #     else:
    #         print(f'Error retrieving storagesize for {identifier}: {response.status_code}, {response.text}')
    #         storagesize_results.append(None)
    # except requests.exceptions.RequestException as e:
    #     print(f'Timeout error on storagesize {identifier}: {e}')
    #     storagesize_results.append(None)

# df_files_datasets['contents'] = contents_results
# df_files_datasets['storagesize'] = storagesize_results

# Number of datasets and dataverses
## Count dataverses
num_dataverses = [
    sum(1 for item in (contents.get('data', []) if isinstance(contents, dict) else []) 
        if item.get('type') == 'dataverse')
    for contents in contents_results
]

## Count datasets
num_datasets = [
    sum(1 for item in (contents.get('data', []) if isinstance(contents, dict) else []) 
        if item.get('type') == 'dataset')
    for contents in contents_results
]

## Get dataset DOIs
dataset_dois = [
    '; '.join(
        item.get('persistentUrl', '').replace('https://doi.org/', '')
        for item in (contents.get('data', []) if isinstance(contents, dict) else [])
        if item.get('type') == 'dataset' and 'persistentUrl' in item
    )
    for contents in contents_results
]

## Add to DataFrame
df_dataverses_select_tdr['num_dataverses'] = num_dataverses
df_dataverses_select_tdr['num_datasets'] = num_datasets
df_dataverses_select_tdr['dataset_dois'] = dataset_dois

df_collection_entries_expanded = pd.merge(df_dataverses_select_tdr, df_collection_entries, on='identifier', how='left')

df_collection_entries_expanded = pd.merge(combined_dataverses_pruned_df, df_collection_entries_expanded, on='id', how='left')
df_collection_entries_expanded.to_csv(f'outputs/{today}_{institution_filename}_all-dataverses.csv', index=False, encoding='utf-8-sig')

df_collection_entries_expanded_pruned = df_collection_entries_expanded[cols_dataverses_tdl_dump+['dataverse_name', 'url', 'identifier', 'parent_dataverse_name', 'parent_dataverse_id', 'dataverse_contact', 'owner', 'dataset_dois']]
# rename identifier column to avoid conflict with dataset 'identifier'
df_collection_entries_expanded_pruned = df_collection_entries_expanded_pruned.rename(columns={'identifier': 'dataverse_code'})

###############################################
####          Combining dataframes         ####
###############################################

# When doing testing, you need to split dfs based on whether there are blanks or not, otherwise it will create a gigantic df by merging on all blanks
## Rename some columns to avoid conflicts when re-concatenating
df_dataset_entries_enriched = df_dataset_entries_enriched.rename(columns={'contentSize (MB)': 'dataset_size_MB', 'institution': 'institution_name'})
# Separate filled and blank rows
df_dataset_filled = df_dataset_entries_enriched[df_dataset_entries_enriched['dataverse'].notna() & (df_dataset_entries_enriched['dataverse'] != '')]
df_dataset_blank = df_dataset_entries_enriched[df_dataset_entries_enriched['dataverse'].isna() | (df_dataset_entries_enriched['dataverse'] == '')]

df_collection_filled = df_collection_entries_expanded_pruned[df_collection_entries_expanded_pruned['dataverse_name'].notna() & (df_collection_entries_expanded_pruned['dataverse_name'] != '')]
df_collection_blank = df_collection_entries_expanded_pruned[df_collection_entries_expanded_pruned['dataverse_name'].isna() | (df_collection_entries_expanded_pruned['dataverse_name'] == '')]

# Merge filled rows and clean up
merged = pd.merge(df_dataset_filled, df_collection_filled, 
                  on='alias', how='left')
merged = merged.loc[:, ~merged.columns.duplicated(keep='first')]

# Align blank DataFrames to merged columns and concatenate
all_columns = merged.columns.tolist()
df_dataset_blank = df_dataset_blank.reindex(columns=all_columns)
df_collection_blank = df_collection_blank.reindex(columns=all_columns)

dataset_dataverse_merged = pd.concat([merged, df_dataset_blank, df_collection_blank], ignore_index=True)

## Need to clean up on back end
dataset_dataverse_merged = dataset_dataverse_merged.drop(columns=['institution_y', 'subjects_y', 'dataset_title_y', 'total_version_y'])
## Rename deduplicated cols
dataset_dataverse_merged = dataset_dataverse_merged.rename(columns={'institution_x': 'institution', 'subjects_x': 'subjects', 'dataset_title_x': 'dataset_title', 'total_version_x':'total_version'})

# dataset_dataverse_merged = dataset_dataverse_merged.fillna({'dataverse_name': 'Default institutional dataverse', 'parent_dataverse_name': 'None', 'parent_dataverse_id': 'None', 'dataverse_contact': 'None', 'owner': -999, 'id': -999, 'dataset_dois': 'Not applicable'})
# dataset_dataverse_merged_dedup = dataset_dataverse_merged.drop_duplicates(subset=['doi', 'dataset_id'], keep='first')

dataset_dataverse_merged = dataset_dataverse_merged.dropna(subset=['persistentUrl'])
dataset_dataverse_merged_dedup = dataset_dataverse_merged.dropna(subset=['doi'])
# dataset_dataverse_merged.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses-{status_filename}.csv', index=False, encoding='utf-8-sig')
dataset_dataverse_merged.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses.csv', index=False, encoding='utf-8-sig')

# ## Combining dataset-dataverse and author dfs
# authors_dataset_dataverse_merged = pd.merge(
#         df_author_entries_combined_deduplicated,
#         dataset_dataverse_merged_dedup,
#         on='doi',
#         how='left'
#     )
# authors_dataset_dataverse_merged.to_csv(f'outputs/{today}_{institution_filename}_all-authors-datasets-dataverses-{status_filename}.csv', index=False, encoding='utf-8-sig')

###################################################
####          Metrics (in development)         ####
###################################################
if metrics_dv:
    url_tdr_native = 'https://dataverse.tdl.org/api/datasets/'
    metrics = []

    endpoints = {
    'viewsUnique': 'makeDataCount/viewsUnique',
    'downloadsUnique': 'makeDataCount/downloadsUnique',
    'downloadsTotal': 'makeDataCount/downloadsTotal',
    'citations': 'makeDataCount/citations',
    'viewsTotal': 'makeDataCount/viewsTotal'
    }
        
    for doi in dataset_dataverse_merged_dedup['doi']:
        try:
            metric_entry = {'doi': doi}
            
            # Query each endpoint
            for metric_name, endpoint in endpoints.items():
                try:
                    response = requests.get(f'{url_tdr_native}:persistentId/{endpoint}?persistentId=doi:{doi}', headers=headers_tdr, timeout=10)
                    
                    if response.status_code == 200:
                        full_response = response.json()
                        data = full_response.get('data', {})
                        
                        # Handle text-based citations as list
                        if metric_name == 'citations':
                            citations_list = data if isinstance(data, list) else []
                            metric_entry['citationCount'] = len(citations_list)
                            metric_entry['citations'] = citations_list
                        else:
                            # All others are numeric
                            value = data.get(metric_name, None)
                            metric_entry[metric_name] = value
                        
                        print(f'Retrieved {metric_name} ({value}) for: {doi}')
                    else:
                        print(f'Failed to retrieve {metric_name} for {doi}: {response.status_code}')
                        if metric_name == 'citations':
                            metric_entry['citationCount'] = None
                            metric_entry['citations'] = None
                        else:
                            metric_entry[metric_name] = None
                        
                except requests.exceptions.Timeout:
                    print(f'Timed out on {metric_name} for: {doi}')
                    if metric_name == 'citations':
                        metric_entry['citationCount'] = None
                        metric_entry['citations'] = None
                    else:
                        metric_entry[metric_name] = None
                except requests.exceptions.RequestException as e:
                    print(f'Failed to retrieve {metric_name} for {doi}: {str(e)}')
                    if metric_name == 'citations':
                        metric_entry['citationCount'] = None
                        metric_entry['citations'] = None
                    else:
                        metric_entry[metric_name] = None
            
            metrics.append(metric_entry)
            time.sleep(0.6)
            
        except Exception as e:
            print(f'Error processing {doi}: {str(e)}')

    # Convert to DataFrame
    metrics_df_dv = pd.DataFrame(metrics)
    metrics_df_dv = metrics_df_dv.rename(columns={'viewsTotal': 'views_total_dv', 'viewUnique': 'views_unique_dv', 'downloadsTotal': 'downloads_total_dv', 'downloadsUnique': 'downloads_unique_dv', 'citationCount': 'citations_dv'})
    metrics_df_dv.to_csv(f'outputs/{today}_dataset-metrics_Dataverse_{institution_filename}.csv', index=False)

if metrics_dc:
    url_datacite = 'https://api.datacite.org/dois/'
    dataset_entries_native = []
    # Drop blanks (unpublished)
    dataset_dataverse_merged_dedup_clean = dataset_dataverse_merged_dedup.dropna(subset=['doi'])
    for doi in dataset_dataverse_merged_dedup_clean['doi']:
        try:
            response = requests.get(f'{url_datacite}{doi}')
            print(f'Retrieving metrics from DataCite for: {doi}.\n')
            if response.status_code == 200:
                dataset_entries_native.append(response.json())
        except Exception as e:
            print(f'Error processing {doi}: {str(e)}')

    datacite_metrics = {
        'datasets': dataset_entries_native
        }

    datacite_select = [] 
    datasets = datacite_metrics.get('datasets', []) 
    for item in datasets:
        data = item.get('data', {})
        attributes = data.get('attributes', {})
        doi = attributes.get('doi', '')
        views = attributes.get('viewCount', None)
        downloads = attributes.get('downloadCount', None)
        citations = attributes.get('citationCount', None)
        base_entry = {
            'doi': doi,
            'views_dc': views,
            'downloads_dc': downloads,
            'citations_dc': citations
            }
        datacite_select.append(base_entry)

    df_datacite_select = pd.json_normalize(datacite_select)
    df_datacite_select['doi'] = df_datacite_select['doi'].str.upper()
    # df_datacite_select.to_csv(f'outputs/{today}_dataset-metrics_DataCite_{institution_filename}.csv', index=False)

# if only_my_institution:
#     unique_downloads = requests.get(f'https://dataverse.tdl.org/api/info/metrics/uniquedownloads?parentAlias={subtree}')
#     with open(f'outputs/{today}_unique_downloads_{institution_filename}.csv', 'w') as f:
#         f.write(unique_downloads.text)
# else:
#     unique_downloads = requests.get(f'https://dataverse.tdl.org/api/info/metrics/uniquedownloads')
#     with open(f'outputs/{today}_unique_downloads_{institution_filename}.csv', 'w') as f:
#         f.write(unique_downloads.text)

if metrics_dv:
    dataset_dataverse_merged = pd.merge(dataset_dataverse_merged,metrics_df_dv,left_on='doi',right_on='doi', how='left')
    dataset_dataverse_merged_dedup = pd.merge(dataset_dataverse_merged_dedup,metrics_df_dv,left_on='doi',right_on='doi', how='left')

if metrics_dc:
    dataset_dataverse_merged = pd.merge(dataset_dataverse_merged,df_datacite_select,left_on='doi',right_on='doi', how='left')
    dataset_dataverse_merged_dedup = pd.merge(dataset_dataverse_merged_dedup,df_datacite_select,left_on='doi',right_on='doi', how='left')

if metrics_dv or metrics_dc:
    dataset_dataverse_merged = dataset_dataverse_merged.dropna(subset=['persistentUrl'])
    dataset_dataverse_merged.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses.csv', index=False, encoding='utf-8-sig')
    dataset_dataverse_merged_dedup.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses-{status_filename}.csv', index=False, encoding='utf-8-sig')

#########################################################
####          Process author-level output df         ####
#########################################################

df_author_entries_combined_deduplicated = df_author_entries_combined_deduplicated.sort_values(by='author_name')
# df_author_entries_combined_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-authors-{status_filename}.csv', index=False, encoding='utf-8-sig')

df_all_affiliations_dedup = df_author_entries_combined_deduplicated.drop_duplicates(subset=['author_affiliation'], keep='first')
df_all_affiliations_dedup = df_all_affiliations_dedup.rename(columns={'author_affiliation': 'affiliation'})

if ror_map is None: #create primary file if it doesn't exist yet
    print('No existing primary file found, creating new one.\n')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup) - 1}\n')
    df_all_affiliations_dedup.to_csv(f'{script_dir}/affiliation-map-primary.csv', index=False, encoding='utf-8-sig')
else: #concat primary file with new list of unique affiliations, drop duplicates (keep first will retain existing matches)
    print('Found existing primary file, adding and deduplicating.\n')
    df_all_affiliations_dedup_expanded = pd.concat([ror_map, df_all_affiliations_dedup])
    print(f'Total affiliations: {len(df_all_affiliations_dedup_expanded)}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded.drop_duplicates(subset=['affiliation'], keep='first')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup_expanded_pruned) - 1}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned[['affiliation', 'ror', 'official_name']]
    ## remove blanks
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned.dropna(subset=['affiliation'])
    ## if the ROR plug-in is not enabled, it will return ROR strings
    mask = ~df_all_affiliations_dedup_expanded_pruned['affiliation'].str.contains('https://ror.org/', case=False, na=False) # Case-insensitive and handles potential NaN values
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned[mask]
    df_all_affiliations_dedup_expanded_pruned.to_csv(f'{script_dir}/affiliation-map-primary-TEMP.csv', index=False, encoding='utf-8-sig')

df_funders = df_dataset_entries_aggregated[['funders']].copy()

df_funders['funders'] = df_funders['funders'].str.split('; ')
df_funders_exploded = df_funders.explode('funders')
df_funders_exploded = df_funders_exploded[ (df_funders_exploded['funders'].notna()) & (df_funders_exploded['funders'] != '')]
df_funders_exploded = df_funders_exploded.reset_index(drop=True)
df_funders_unique = df_funders_exploded.drop_duplicates( subset=['funders'],  keep='first')
df_funders_unique = df_funders_unique.sort_values('funders').reset_index(drop=True)

if funder_ror_map is None: #create primary file if it doesn't exist yet
    print('No existing primary map file found, creating new one.\n')
    print(f'Total unique funders: {len(df_funders_unique) - 1}\n')
    df_funders_unique.to_csv(f'{script_dir}/funder-map-primary.csv', index=False, encoding='utf-8-sig')
else: 
    # Concat existing mapping file with new list of unique affiliations, drop duplicates (keep first will retain existing matches)
    ## Requires you to have manually added a 'ror' column to original output
    print('Found existing primary map file, adding and deduplicating.\n')
    df_funders_expanded = pd.concat([funder_ror_map, df_funders_unique])
    print(f'Total funders: {len(df_funders_expanded)}\n')
    df_funders_expanded_pruned = df_funders_expanded.drop_duplicates(subset=['grant_agencies'], keep='first')
    print(f'Total unique funders: {len(df_funders_expanded_pruned) - 1}\n')
    df_funders_expanded_pruned = df_funders_expanded_pruned[['grant_agencies', 'ror', 'official_name']]
    df_funders_expanded_pruned = df_funders_expanded_pruned.dropna(subset=['grant_agencies'])
    df_funders_expanded_pruned.to_csv(f'{script_dir}/funder-map_TEMP.csv', index=False, encoding='utf-8-sig')

if ror_map is not None:
    print(f'Number of new affiliations to check: {len(df_all_affiliations_dedup_expanded_pruned) - len(ror_map)}.\n')

if funder_ror_map is not None:
    print(f'Number of new funders to check: {len(df_funders_expanded_pruned) - len(funder_ror_map)}.\n')

##############################################################
####          At-a-glance dataset-level summaries         ####
##############################################################

# Size by year summary
size_by_year = df_file_entries_combined_deduplicated.groupby('file_creation_year')['file_size'].sum().reset_index()
size_by_year['fileGB'] = size_by_year['file_size'] / 1000000000
# print('Annual size summary')
# print(size_by_year)
size_by_year.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-annual-size-{status_filename}.csv', index=False, encoding='utf-8-sig')

# File format summary
unique_datasets_per_format = df_file_entries_combined_deduplicated.groupby('friendly_format_manual')['dataset_id'].nunique()
# print('Total file format summary')
# print(unique_datasets_per_format)
unique_datasets_per_format.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-unique-format-{status_filename}.csv', index=False, encoding='utf-8-sig')

print(f'Done\n---Time to run: {datetime.now() - start_time}---\n')

if test:
    print('**REMINDER: THIS IS A TEST RUN, AND ANY RESULTS ARE NOT COMPLETE!**\n')