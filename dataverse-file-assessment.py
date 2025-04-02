import requests
import pandas as pd
import json
import numpy as np
import os
import math
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# for test environment
test = False
# to only look at your/one institution in TDR
only_my_institution = True 

startTime = datetime.now() ## setting timestamp at start of script to calculate run time
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

## read in filename version of your institution's name
my_institution = config['INSTITUTION']['filename']

#creating directories
if test:
    if os.path.isdir("test"):
        print("test directory found - no need to recreate")
    else:
        os.mkdir("test")
        print("test directory has been created")
    os.chdir('test')
    if os.path.isdir("outputs"):
        print("test outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("test outputs directory has been created")
else:
    if os.path.isdir("outputs"):
        print("outputs directory found - no need to recreate")
    else:
        os.mkdir("outputs")
        print("outputs directory has been created")

print("Beginning to define API call parameters.")

url_tdr = "https://dataverse.tdl.org/api/search/"

## set API-specific params
### Dataverse
page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] if test else config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size = config['VARIABLES']['PAGE_SIZES']['dataverse'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse']

print(f"Retrieving {page_size} records per page over {page_limit_dataverse} pages.")

### for TDR, affiliation is not reliable for returning all relevant results; the DOI prefix is used as the most generic common denominator for datasets
query = '10.18738/T8/'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0
l = 0

headers_tdr = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}

params_tdr_ut_austin = {
    'q': query,
    'subtree': 'utexas',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'subtree': 'baylor',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'subtree': 'smu',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'subtree': 'tamu',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'subtree': 'txst',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'subtree': 'ttu',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'subtree': 'uh',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'subtree': 'unthsc',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'subtree': 'tamug',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'subtree': 'tamiu',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'subtree': 'uthscsa',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'subtree': 'utswmed',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'subtree': 'uta',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'subtree': 'twu',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

#substitute for your institution
if only_my_institution:
        params_list = {
        "UT Austin": params_tdr_ut_austin
    }
else:
    params_list = {
        "UT Austin": params_tdr_ut_austin,
        "Baylor": params_tdr_baylor,
        "SMU": params_tdr_smu,
        "TAMU": params_tdr_tamu,
        "Texas State": params_tdr_txst,
        "Texas Tech": params_tdr_ttu,
        "Houston": params_tdr_houston,
        "HSC Fort Worth": params_tdr_hscfw,
        "TAMU Galveston": params_tdr_tamug,
        "TAMU International": params_tdr_tamui,
        "UT San Antonio Health": params_tdr_utsah,
        "UT Southwestern Medical": params_tdr_utswm,
        "UT Arlington": params_tdr_uta,
        "Texas Women's University": params_tdr_twu
    }

params_harvard = {
    'q': '10.7910',
    'type': 'dataset',
    'sort': 'date',
    'order': 'desc',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

# Step 2. Define global functions
def retrieve_page_dataverse(url, params=None, headers=None):
    """Fetch a single page of results."""
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise an exception for errors
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'data': {'items': [], 'total_count': 0}}

def retrieve_all_data_dataverse(url, params, headers):
    global page_limit_dataverse, k
    """Fetch all pages of data using pagination."""
    # create empty list
    all_data_tdr = []

    while True and k < page_limit_dataverse: 
        k+=1
        # Fetch a page of data
        data = retrieve_page_dataverse(url, params, headers)  
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count/page_limit_dataverse)
        print(f"Fetching Page {params['page']} of {total_pages} pages...")
        print()

        if not data['data']:
            print("No data found.")
            break
    
        all_data_tdr.extend(data['data']['items'])
        
        # Update pagination parameters
        params['start'] += page_limit_dataverse
        params['page'] += 1
        
        if params['start'] >= total_count:
            print("End of response.")
            break

    return all_data_tdr

def retrieve_all_data_for_institutions(url, params_list, headers):
    all_data = []  # Create a list to hold all entries

    for institution_name, params in params_list.items():
        global page_limit_dataverse, k
        k = 0  # Reset k for each institution

        # Fetch data for the current institution
        all_data_tdr = retrieve_all_data_dataverse(url, params, headers)

        # Add institution name to each entry
        for entry in all_data_tdr:
            entry['institution'] = institution_name  # Add the institution name
            all_data.append(entry)  # Append the entry to the list

    return all_data

# Step 3. Starting API retrieval

print("Starting TDR retrieval")
print()

all_data = retrieve_all_data_for_institutions(url_tdr, params_list, headers_tdr)

# Step 4. Starting response filtering

## for TDR

print("Starting TDR filtering.")
print()
data_select_tdr = []

for item in all_data:
    id = item.get('global_id', "")
    type = item.get('type', "")
    institution = item.get('institution',"")
    status = item.get('versionState', "")
    name = item.get('name', "")
    dataverse = item.get('name_of_dataverse', "")
    majorV = item.get('majorVersion', 0)
    minorV = item.get('minorVersion', 0)
    
    # Append a dictionary with the desired key-value pairs
    data_select_tdr.append({
        'institution': institution, 
        'doi': id,
        'type': type,
        'status': status,
        'title': name,
        'dataverse': dataverse,
        'majorVersion': majorV,
        'minorVersion': minorV
    })

df_data_select_tdr = pd.DataFrame(data_select_tdr)
#remove dataverses and files
filtered_tdr = df_data_select_tdr[df_data_select_tdr['type'] == 'dataset']
#editing DOI field
filtered_tdr['doi'] = filtered_tdr['doi'].str.replace('doi:', '')
#add column for versioned
filtered_tdr['versioned'] = filtered_tdr.apply(lambda row: 'Versioned' if (row['majorVersion'] > 1) or (row['minorVersion'] > 0) else 'Not versioned', axis=1)

#sort on status, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED'
filtered_tdr = filtered_tdr.sort_values(by='status', ascending=False)
if only_my_institution:
    filtered_tdr.to_csv(f"outputs/{todayDate}_{my_institution}_all-deposits.csv")
else:
    filtered_tdr.to_csv(f"outputs/{todayDate}_all-institutions_all-deposits.csv")
filtered_tdr_deduplicated = filtered_tdr.drop_duplicates(subset=['doi'], keep="first")
if only_my_institution:
    filtered_tdr_deduplicated.to_csv(f"outputs/{todayDate}_{my_institution}_all-deposits-deduplicated.csv")
else:
    filtered_tdr_deduplicated.to_csv(f"outputs/{todayDate}_all-institutions_all-deposits-deduplicated.csv")

#create df of published datasets with draft version (retains both entries)
commonColumns = ['doi', 'title']
duplicates = filtered_tdr.duplicated(subset=commonColumns, keep=False)
dualStatusDatasets = filtered_tdr[duplicates]
if only_my_institution:
    dualStatusDatasets.to_csv(f"outputs/{todayDate}_{my_institution}_dual-status-datasets.csv")
else:
    dualStatusDatasets.to_csv(f"outputs/{todayDate}_all-institutions_dual-status-datasets.csv")

#retrieving additional metadata for deposits by individual API call (one per DOI)
##retrieves both published and never-published draft datasets; if a published dataset is currently in DRAFT state, it will return the information for the DRAFT state
print("Starting Native API call")
url_tdr_native = "https://dataverse.tdl.org/api/datasets/"

results = []
for doi in filtered_tdr_deduplicated['doi']:
    try:
        response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f"Retrieving {doi}")
            print()
            results.append(response.json())
        else:
            print(f"Error fetching {doi}: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Timeout error on DOI {doi}: {e}")

data_tdr_native = {
    'datasets': results
}

data_select_tdr_native = [] 
for item in data_tdr_native['datasets']:
    data = item.get('data', "")
    datasetID = data.get('id', "")
    pubDate = data.get('publicationDate', "")
    latest = data.get('latestVersion', {})
    status = latest.get('versionState', "")
    status2 = latest.get('latestVersionPublishingState', "")
    doi = latest.get('datasetPersistentId', "")
    updateDate = latest.get('lastUpdateTime', "")
    createDate = latest.get('createTime', "")
    releaseDate = latest.get('releaseTime', "")
    files = latest.get('files', [])
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    fields = citation.get('fields', [])
    grantAgencies = []
    for field in fields:
        if field['typeName'] == 'grantNumber':
            for grant in field.get('value', []):
                grant_number_agency = grant.get('grantNumberAgency', {}).get('value', '')
                grantAgencies.append(grant_number_agency)
    total_filesize = 0
    unique_content_types = set()
    fileCount = len(files)
    for file_info in files:
        file_data = file_info['dataFile']
        total_filesize += file_data['filesize']
        unique_content_types.add(file_data['contentType'])
        file_entry = {
            'datasetID': datasetID,
            'doi': doi,
            'publicationDate': pubDate,
            'status': status,
            'status2': status2,
            'releaseDate': releaseDate,
            'creationDate': createDate,
            'updateDate': updateDate,
            'size': total_filesize,
            'fileCount': fileCount,
            'unique_content_types': list(unique_content_types),
            'fileID': file_data.get('id', ""),
            'filename': file_data.get('filename', ""),
            'mimeType': file_data.get('contentType', ""),
            'friendlyType': file_data.get('contentType', ""),
            'tabular': file_data.get('tabular', ""),
            'fileSize': file_data.get('filesize', 0),
            'storageIdentifier': file_data.get('storageIdentifier', ""),
            'fileCreationDate': file_data.get('creationDate', ""),
            'filePublicationDate': file_data.get('publicationDate', "")

        }
        data_select_tdr_native.append(file_entry)

df_select_tdr_native = pd.json_normalize(data_select_tdr_native)
df_select_tdr_native['doi'] = df_select_tdr_native['doi'].str.replace('doi:', '')
df_select_tdr_native['fileCreationDate'] = pd.to_datetime(df_select_tdr_native['fileCreationDate'])
df_select_tdr_native['fileCreationYear'] = df_select_tdr_native['fileCreationDate'].dt.year
df_select_tdr_native.to_csv("outputs/test-first-native.csv")

ten_kb = 1 * 1024
one_mb = 1 * 1024 * 1024
hundred_mb = 100 * 1024 * 1024
one_gb = 1 * 1024 * 1024 * 1024
ten_gb = 10 * 1024 * 1024 * 1024
fifteen_gb = 15 * 1024 * 1024 * 1024
twenty_gb = 20 * 1024 * 1024 * 1024
twentyfive_gb = 25 * 1024 * 1024 * 1024
thirty_gb = 30 * 1024 * 1024 * 1024
thirtyfive_gb = 35 * 1024 * 1024 * 1024
forty_gb = 40 * 1024 * 1024 * 1024
fifty_gb = 50 * 1024 * 1024 * 1024

df_select_tdr_native['size_bin'] = "Empty"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > 0) & (df_select_tdr_native['size'] <= ten_kb), 'size_bin'] = "0-10 kB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > ten_kb) & (df_select_tdr_native['size'] <= one_mb), 'size_bin'] = "10 kB-1 MB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > one_mb) & (df_select_tdr_native['size'] <= hundred_mb), 'size_bin'] = "1-100 MB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > hundred_mb) & (df_select_tdr_native['size'] <= one_gb), 'size_bin'] = "100 MB-1 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > one_gb) & (df_select_tdr_native['size'] <= ten_gb), 'size_bin'] = "1-10 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > ten_gb) & (df_select_tdr_native['size'] <= fifteen_gb), 'size_bin'] = "10-15 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > fifteen_gb) & (df_select_tdr_native['size'] <= twenty_gb), 'size_bin'] = "15-20 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > twenty_gb) & (df_select_tdr_native['size'] <= twentyfive_gb), 'size_bin'] = "20-25 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > twentyfive_gb) & (df_select_tdr_native['size'] <= thirty_gb), 'size_bin'] = "25-30 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > thirty_gb) & (df_select_tdr_native['size'] <= forty_gb), 'size_bin'] = "30-40 GB"
df_select_tdr_native.loc[(df_select_tdr_native['size'] > forty_gb) & (df_select_tdr_native['size'] <= fifty_gb), 'size_bin'] = "40-50 GB"
df_select_tdr_native.loc[df_select_tdr_native['size'] > fifty_gb, 'size_bin'] = ">50 GB"

df_select_concatenated = pd.merge(filtered_tdr_deduplicated, df_select_tdr_native, on='doi', how="left")
df_select_concatenated_exist = df_select_concatenated.dropna(subset=['datasetID']) #removes deaccessioned
df_select_concatenated_exist['datasetID'] = df_select_concatenated_exist['datasetID'].astype(int)
if only_my_institution:
    df_select_concatenated_exist.to_csv(f"outputs/{todayDate}_{my_institution}_all-deposits-deduplicated_expanded-metadata.csv")
else:
    df_select_concatenated_exist.to_csv(f"outputs/{todayDate}_all-institutions_all-deposits-deduplicated_expanded-metadata.csv")

#size summary
size_by_year = df_select_concatenated_exist.groupby('fileCreationYear')['fileSize'].sum().reset_index()
size_by_year['fileGB'] = size_by_year['fileSize'] / 1000000000
print(size_by_year)
if only_my_institution:
    size_by_year.to_csv(f"outputs/{todayDate}_{my_institution}_annual-size-summary.csv")
else:
    size_by_year.to_csv(f"outputs/{todayDate}_all-institutions_annual-size-summary.csv")

#file format summary
unique_datasets_per_format = df_select_concatenated_exist.groupby('mimeType')['datasetID'].nunique()
print(unique_datasets_per_format)
if only_my_institution:
    unique_datasets_per_format.to_csv(f"outputs/{todayDate}_{my_institution}_unique-format-summary.csv")
else:
    unique_datasets_per_format.to_csv(f"outputs/{todayDate}_all-institutions_unique-format-summary.csv")

print("Done.")
print(f"Time to run: {datetime.now() - startTime}")