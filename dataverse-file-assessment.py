import json
import os
import math
import pandas as pd
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs

#toggle for test environment (incomplete run, faster to complete)
test = True
#toggle to only look at your/one institution in TDR
onlyMyInstitution = True 
#toggle for stage 3 retrieval
versionsAPI = True
#toggle for excluding unpublished
excludeDrafts = True

#setting timestamp at start of script to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

##read in filename version of your institution's name
my_institution_filename = config['INSTITUTION']['filename']
###condition what goes in the filename based on toggle for which institution(s) to ping
if onlyMyInstitution:
    institutionFilename = my_institution_filename
else:
    institutionFilename = "all-institutions"
##read in short-hand version of your institution's name
my_institution_shortName = config["INSTITUTION"]["myInstitution"]

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

##set API-specific params
###Dataverse
page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] if test else config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size = config['VARIABLES']['PAGE_SIZES']['dataverse'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse']

print(f"Retrieving {page_size} records per page over {page_limit_dataverse} pages.")

###for TDR, affiliation is not reliable for returning all relevant results; the DOI prefix is used as the most generic common denominator for datasets
query = '10.18738/T8/'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0

headers_tdr = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}

params_tdr_ut_austin = {
    'q': query,
    'subtree': 'utexas',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'subtree': 'baylor',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'subtree': 'smu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'subtree': 'tamu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'subtree': 'txst',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'subtree': 'ttu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'subtree': 'uh',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'subtree': 'unthsc',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'subtree': 'tamug',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'subtree': 'tamiu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'subtree': 'uthscsa',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'subtree': 'utswmed',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'subtree': 'uta',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'subtree': 'twu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

all_params = {
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

tamu_combined_params = {
        "TAMU": params_tdr_tamu,
        "TAMU Galveston": params_tdr_tamug,
        "TAMU International": params_tdr_tamui
}

#substitute for your institution
if onlyMyInstitution:
    if my_institution_shortName == "TAMU":
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_shortName: all_params[my_institution_shortName]
        }
else:
    params_list = all_params

#define functions
##function to get single page from Dataverse API
def retrieve_page_dataverse(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error retrieving page: {e}")
        return {'data': {'items': [], 'total_count': 0}}
##function to get many pages from Dataverse API
def retrieve_all_data_dataverse(url, params, headers):
    global page_limit_dataverse, k
    # create empty list
    all_data_tdr = []

    while True and k < page_limit_dataverse: 
        k+=1
        data = retrieve_page_dataverse(url, params, headers)  
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count/page_limit_dataverse)
        print(f"Retrieving Page {params['page']} of {total_pages} pages...\n")

        if not data['data']:
            print("No data found.")
            break
    
        all_data_tdr.extend(data['data']['items'])
        
        #update pagination
        params['start'] += page_limit_dataverse
        params['page'] += 1
        
        if params['start'] >= total_count:
            print("End of response.")
            break

    return all_data_tdr

##function to retrieve many pages from many institutions
def retrieve_all_data_for_institutions(url, params_list, headers):
    all_data = []

    for institution_name, params in params_list.items():
        global page_limit_dataverse, k
        k = 0  #reset k for each institution

        all_data_tdr = retrieve_all_data_dataverse(url, params, headers)
        for entry in all_data_tdr:
            entry['institution'] = institution_name 
            all_data.append(entry)

    return all_data


##function to count descriptive words
def count_words(text):
    words = text.split()
    total_words = len(words)
    descriptive_count = sum(1 for word in words if word not in nondescriptive_words)
    return total_words, descriptive_count

##function to account for when a single word may or may not be descriptive but is certainly uninformative if in a certain combination
def adjust_descriptive_count(row):
    if ('supplemental material' in row['titleReformatted'].lower() or
            'supplementary material' in row['titleReformatted'].lower() or
            'supplementary materials' in row['titleReformatted'].lower() or
            'supplemental materials' in row['titleReformatted'].lower()):
        return max(0, row['descriptiveWordCount_title'] - 1)
    return row['descriptiveWordCount_title']

##function to assign size bins (file or dataset level)
def assign_size_bins(df, column='fileSize', new_column='fileSizeBin'):
    df=df.copy()
    bins = [
        (0, 1 * 1024, "0-10 kB"),
        (1 * 1024, 1 * 1024 * 1024, "10 kB-1 MB"),
        (1 * 1024 * 1024, 100 * 1024 * 1024, "1-100 MB"),
        (100 * 1024 * 1024, 1 * 1024 * 1024 * 1024, "100 MB-1 GB"),
        (1 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024, "1-10 GB"),
        (10 * 1024 * 1024 * 1024, 15 * 1024 * 1024 * 1024, "10-15 GB"),
        (15 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024, "15-20 GB"),
        (20 * 1024 * 1024 * 1024, 25 * 1024 * 1024 * 1024, "20-25 GB"),
        (25 * 1024 * 1024 * 1024, 30 * 1024 * 1024 * 1024, "25-30 GB"),
        (30 * 1024 * 1024 * 1024, 40 * 1024 * 1024 * 1024, "30-40 GB"),
        (40 * 1024 * 1024 * 1024, 50 * 1024 * 1024 * 1024, "40-50 GB"),
    ]

    #default set to empty
    df[new_column] = "Empty"
    for lower, upper, label in bins:
        df.loc[(df[column] > lower) & (df[column] <= upper), new_column] = label
    #maximum bin
    df.loc[df[column] > 50 * 1024 * 1024 * 1024, new_column] = ">50 GB"

    return df

print("Starting TDR retrieval.\n")
all_data = retrieve_all_data_for_institutions(url_tdr, params_list, headers_tdr)

print("Starting TDR filtering.\n")
data_select_tdr = []
for item in all_data:
    id = item.get('global_id', '')
    type = item.get('type', '')
    institution = item.get('institution','')
    status = item.get('versionState', '')
    name = item.get('name', '')
    dataverse = item.get('name_of_dataverse', '')
    majorV = item.get('majorVersion', 0)
    minorV = item.get('minorVersion', 0)
    comboV = f"{majorV}.{minorV}"
    data_select_tdr.append({
        'institution': institution, 
        'doi': id,
        'type': type,
        'status': status,
        'title': name,
        'dataverse': dataverse,
        'majorVersion': majorV,
        'minorVersion': minorV,
        'totalVersion': comboV
    })

df_data_select_tdr = pd.DataFrame(data_select_tdr)
#remove dataverses and files
filtered_tdr = df_data_select_tdr[df_data_select_tdr['type'] == 'dataset']
#editing DOI field
filtered_tdr['doi'] = filtered_tdr['doi'].str.replace('doi:', '')
#add column for versioned
filtered_tdr['versioned'] = filtered_tdr.apply(lambda row: 'Versioned' if (row['majorVersion'] > 1) or (row['minorVersion'] > 0) else 'Not versioned', axis=1)

#metadata assessments
##title
##assess 'descriptiveness of dataset title'
words = config['WORDS']
###add integers
numbers = list(map(str, range(1, 1000000)))
###combine all into a single set
nondescriptive_words = set(
    words['articles'] +
    words['conjunctions'] +
    words['prepositions'] +
    words['auxiliary_verbs'] +
    words['possessives'] +
    words['descriptors'] +
    words['order'] +
    words['version'] +
    numbers
)

filtered_tdr['titleReformatted'] = filtered_tdr['title'].str.replace('_', ' ') 
filtered_tdr['titleReformatted'] = filtered_tdr['title'].str.replace('-', ' ') #gets around text linked by underscores counting as 1 word
filtered_tdr['titleReformatted'] = filtered_tdr['titleReformatted'].str.lower()
filtered_tdr[['totalWordCount_title', 'descriptiveWordCount_title']] = filtered_tdr['titleReformatted'].apply(lambda x: pd.Series(count_words(x)))

filtered_tdr['descriptiveWordCount_title'] = filtered_tdr.apply(adjust_descriptive_count, axis=1)
filtered_tdr['nondescriptiveWordCount_title'] = filtered_tdr['totalWordCount_title'] - filtered_tdr['descriptiveWordCount_title']

#sort on status, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED'
filtered_tdr = filtered_tdr.sort_values(by='status', ascending=False)
filtered_tdr.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-deposits.csv")
filtered_tdr_deduplicated = filtered_tdr.drop_duplicates(subset=['doi'], keep="first")
filtered_tdr_deduplicated.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-deposits-deduplicated.csv")
print(f'Total datasets to be analyzed: {len(filtered_tdr_deduplicated)}.\n')

#create df of published datasets with draft version (retains both entries)
commonColumns = ['doi', 'title']
duplicates = filtered_tdr.duplicated(subset=commonColumns, keep=False)
dualStatusDatasets = filtered_tdr[duplicates]
dualStatusDatasets.to_csv(f"outputs/{todayDate}_{institutionFilename}_dual-status-datasets.csv")

#retrieving additional metadata for deposits by individual API call (one per DOI)
##retrieves both published and never-published draft datasets; if a published dataset is currently in DRAFT state, it will return the information for the DRAFT state
print("Starting Native API call")
url_tdr_native = "https://dataverse.tdl.org/api/datasets/"

results = []
for doi in filtered_tdr_deduplicated['doi']:
    try:
        response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f"Retrieving {doi}\n")
            results.append(response.json())
        else:
            print(f"Error retrieving {doi}: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Timeout error on DOI {doi}: {e}")

data_tdr_native = {
    'datasets': results
}
print("Beginning dataframe subsetting\n")
data_select_tdr_native = [] 
for item in data_tdr_native['datasets']:
    data = item.get('data', '')
    datasetID = data.get('id', '')
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
        unique_content_types.add(file_data['contentType'])
        file_entry = {
            'datasetID': datasetID,
            'doi': doi,
            #'status': status,
            'currentStatus': status2,
            'reuseRequirements': usage,
            #'fileCount': fileCount,
            #'unique_content_types': list(unique_content_types),
            'fileID': file_data.get('id', ''),
            'public': file_data.get('restricted', ''),
            'filename': file_data.get('filename', ''),
            'mimeType': file_data.get('contentType', ''),
            'friendlyType': file_data.get('friendlyType', ''),
            'tabular': file_data.get('tabularData', ''),
            'fileSize': file_data.get('filesize', 0),
            'storageIdentifier': file_data.get('storageIdentifier', ''),
            'creationDate': file_data.get('creationDate', ''),
            'publicationDate': file_data.get('publicationDate', '')
        }
        data_select_tdr_native.append(file_entry)

#getting dataframe with entries for individual authors
author_entries = []
for item in data_tdr_native['datasets']:
    data = item.get('data', {})
    latest = data.get('latestVersion', {})
    doi = latest.get('datasetPersistentId', '')
    citation = latest.get('metadataBlocks', {}).get('citation', {})
    status2 = latest.get('latestVersionPublishingState', '')
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
                    'currentStatus': status2,
                    'authorName': name,
                    'authorAffiliation': affiliationName,
                    'rorID': affiliation_ror,
                    'authorIdentifier': identifier,
                    'authorIdentifierExpanded': identifier_expanded,
                    'authorIdentifierScheme': scheme
                }
                author_entries.append(author_entry)

df_select_tdr_native = pd.json_normalize(data_select_tdr_native)
df_author_entries = pd.json_normalize(author_entries)
df_select_tdr_native['doi'] = df_select_tdr_native['doi'].str.replace('doi:', '')
df_author_entries['doi'] = df_author_entries['doi'].str.replace('doi:', '')
df_select_tdr_native['creationDate'] = pd.to_datetime(df_select_tdr_native['creationDate'])
df_select_tdr_native['fileCreationYear'] = df_select_tdr_native['creationDate'].dt.year

df_select_tdr_native = assign_size_bins(df_select_tdr_native, column='fileSize', new_column='fileSizeBin')
df_select_concatenated = pd.merge(filtered_tdr_deduplicated, df_select_tdr_native, on='doi', how="left")
df_select_concatenated_exist = df_select_concatenated.dropna(subset=['datasetID']).copy() #removes deaccessioned
df_select_concatenated_exist['datasetID'] = df_select_concatenated_exist['datasetID'].astype(int)
df_select_concatenated_exist.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-deposits-deduplicated_expanded-metadata.csv")

#subset to datasets that are less than version 2.0 (no major update, no file additions)
df_select_concatenated_exist_majorVersion = df_select_concatenated_exist[df_select_concatenated_exist['majorVersion'] > 1]


#need to use Version endpoint to get info on published version of published datasets that are currently in DRAFT status and all published versions of a dataset with multiple PUBLISHED versions. This endpoint is public and does not return any DRAFTs.
#remove datasets that have never been published (will not return any info for this endpoint)
df_select_concatenated_exist_published = df_select_concatenated_exist_majorVersion[df_select_concatenated_exist_majorVersion['publicationDate'].notnull()]
#deduplicate on datasetID
df_select_concatenated_exist_published_dedup = df_select_concatenated_exist_published.drop_duplicates(subset="datasetID", keep="first")

if versionsAPI:
    results_versions = []
    print("Beginning Version API query\n")
    for datasetID in df_select_concatenated_exist_published_dedup['datasetID']:
        try:
            response = requests.get(f'{url_tdr_native}{datasetID}/versions')
            if response.status_code == 200:
                print(f"Retrieving versions of dataset #{datasetID}")
                print()
                results_versions.append(response.json())
            else:
                print(f"Error retrieving dataset #{datasetID}: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Timeout error on DOI {doi}: {e}")

    data_tdr_versions = {
        'datasets': results_versions
    }
    print("Beginning dataframe subsetting\n")
    data_select_tdr_versions = [] 
    for dataset in data_tdr_versions['datasets']:
        data = dataset.get('data', [])
        for item in data:
            doi = item.get('datasetPersistentId', '')
            id = item.get("id", '')
            datasetid = item.get('datasetId', '')
            majorV = str(item.get('versionNumber', 0))
            minorV = str(item.get('versionMinorNumber', 0))
            status2 = latest.get('latestVersionPublishingState', '')
            comboV = f"{majorV}.{minorV}"
            status = item.get('versionState', '')
            for file in item.get('files', []):
                fileInfo = file['dataFile']
                data_select_tdr_versions.append({
                    'doi': doi,
                    'versionID': id,
                    'datasetID': datasetid,
                    #'majorVersion': majorV,
                    #'minorVersion': minorV,
                    'totalVersion': comboV,
                    'fileID': fileInfo.get('id', ''),
                    'filename': fileInfo.get('filename', ''),
                    'mimeType': fileInfo.get('contentType', ''),
                    'friendlyType': fileInfo.get('friendlyType', ''),
                    #'status': status,
                    'currentStatus': status2,
                    #'tabular': fileInfo.get('tabularData', ''),
                    'fileSize': fileInfo.get('filesize', ''),
                    'storageIdentifier': fileInfo.get('storageIdentifier', ''),
                    #'md5': fileInfo.get('md5', ''),
                    'creationDate': fileInfo.get('creationDate', ''),
                    'publicationDate': fileInfo.get('publicationDate', '')
                })
    #getting dataframe with entries for individual authors
    author_entries_versions = []
    for dataset in data_tdr_versions['datasets']:
        data = dataset.get('data', [])
        for item in data:
            doi = item.get('datasetPersistentId', '')
            id = item.get("id", '')
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
                            'currentStatus': status2,
                            'authorName': name,
                            'authorAffiliation': affiliationName,
                            'rorID': affiliation_ror,
                            'authorIdentifier': identifier,
                            'authorIdentifierExpanded': identifier_expanded,
                            'authorIdentifierScheme': scheme
                        }
                        author_entries_versions.append(author_entry)

    df_select_tdr_versions = pd.json_normalize(data_select_tdr_versions)
    df_author_entries_versions = pd.json_normalize(author_entries_versions)
    df_select_tdr_versions['doi'] = df_select_tdr_versions['doi'].str.replace('doi:', '')
    df_author_entries_versions['doi'] = df_author_entries_versions['doi'].str.replace('doi:', '')
    #removing duplicate entries for a given file that has not changed across multiple versions
    df_select_tdr_versions['totalVersion'] = df_select_tdr_versions['totalVersion'].astype(float)
    df_select_tdr_versions = df_select_tdr_versions.sort_values(by='totalVersion')
    df_select_tdr_versions_deduplicated = df_select_tdr_versions.drop_duplicates(subset=['datasetID', 'storageIdentifier'], keep='first')

    df_select_tdr_versions_deduplicated = assign_size_bins(df_select_tdr_versions_deduplicated, column='fileSize', new_column='fileSizeBin')

    df_select_versions_concatenated_released = pd.merge(df_select_tdr_versions_deduplicated, filtered_tdr_deduplicated, on='doi', how="left")

    #pruning and renaming columns in the two dataframes that collectively (should) have all of the files (from the Native and the Version endpoints)
    df_version_pruned = df_select_versions_concatenated_released[["versionID", "datasetID", "totalVersion_x", "filename", "fileID", "mimeType", "friendlyType", "fileSize", "storageIdentifier", "creationDate", "publicationDate", "institution", "doi", "fileSizeBin", "title", "dataverse"]]
    df_version_pruned = df_version_pruned.rename(columns={'totalVersion_x': 'totalVersion', 'filename_x': 'filename', 'fileSize_x': 'fileSize', 'storageIdentifier_x': 'storageIdentifier', 'creationDate_x': 'creationDate', 'publicationDate_x':'publicationDate'})
    df_version_pruned['creationYear'] = pd.to_datetime(df_version_pruned['creationDate'], format="%Y-%m-%d").dt.year
    df_version_pruned['publicationYear'] = pd.to_datetime(df_version_pruned['publicationDate'], format="%Y-%m-%d").dt.year

df_native_pruned = df_select_concatenated_exist[["datasetID", "totalVersion", "filename", "fileID", "mimeType", "friendlyType", "fileSize", "storageIdentifier", "creationDate", "publicationDate", "institution", "doi", "fileSizeBin", "title", "dataverse"]]
df_native_pruned = df_native_pruned.copy()
df_native_pruned['creationYear'] = pd.to_datetime(df_native_pruned['creationDate'], format="%Y-%m-%dT%H:%M:%SZ").dt.year
df_native_pruned['publicationYear'] = pd.to_datetime(df_native_pruned['publicationDate'], format="%Y-%m-%d").dt.year

if versionsAPI:
    df_all_files_concat = pd.concat([df_version_pruned, df_native_pruned], ignore_index=True)
    df_all_files_concat = df_all_files_concat.rename(columns={'title': 'datasetTitle'})

    #deduplicate
    ##create fake versionID for drafts to ensure proper sorting and deduplicating
    df_all_files_concat['versionID'] = df_all_files_concat['versionID'].fillna(9999999)
    df_all_files_concat = df_all_files_concat.sort_values(by='versionID')
    df_all_files_concat_deduplicated = df_all_files_concat.drop_duplicates(subset=['storageIdentifier'], keep='first')
    df_all_files_concat_deduplicated = df_all_files_concat_deduplicated.copy()
    df_all_files_concat_deduplicated['versionID'] = df_all_files_concat_deduplicated['versionID'].replace(9999999, None)
    df_all_authors_concat = pd.concat([df_author_entries, df_author_entries_versions], ignore_index=True)
    df_all_authors_concat_deduplicated = df_all_authors_concat.drop_duplicates(subset=['doi', 'authorName', 'authorAffiliation', 'currentStatus'], keep='first')
else:
    #sort on status and then total version, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED' and then to keep the earliest version
    df_native_pruned = df_native_pruned.sort_values(by=['currentStatus', 'totalVersion'], ascending=[False, True])
    df_all_files_concat_deduplicated = df_native_pruned.drop_duplicates(subset=['storageIdentifier'], keep='first')
    df_all_authors_concat_deduplicated = df_author_entries.drop_duplicates(subset=['doi', 'authorName', 'authorAffiliation', 'currentStatus'], keep='first')

#metadata assessment
##readme presence
df_all_files_concat_deduplicated.loc[:,'isREADME'] = df_all_files_concat_deduplicated['filename'].str.contains('readme', case=False)
df_all_files_concat_deduplicated.loc[:,'isCodebook'] = df_all_files_concat_deduplicated['filename'].str.contains('codebook', case=False)
df_all_files_concat_deduplicated.loc[:,'isDataDict'] = df_all_files_concat_deduplicated['filename'].str.contains('dictionary', case=False) #need to check sensitivity

##create separate friendlyFormat column
formatMap = config['FORMAT_MAP']
df_all_files_concat_deduplicated.loc[:,'friendlyFormat_manual'] = df_all_files_concat_deduplicated['mimeType'].apply(
    lambda x: formatMap.get(x.strip(), x.strip()) if isinstance(x, str) and x != "no match found" else "no files"
)
##file formats
softwareFormats = set(config['SOFTWARE_FORMATS'].keys())
compressedFormats = set(config['COMPRESSED_FORMATS'].keys())
microsoftFormats = set(config['MICROSOFT_FORMATS'].keys())
# Assume softwareFormats is a set of friendly software format names
df_all_files_concat_deduplicated.loc[:,'isSoftware'] = df_all_files_concat_deduplicated['mimeType'].apply(
    lambda x: any(part.strip() in softwareFormats for part in x.split(';')) if isinstance(x, str) else False
)
df_all_files_concat_deduplicated.loc[:,'isCompressed'] = df_all_files_concat_deduplicated['mimeType'].apply(
    lambda x: any(part.strip() in compressedFormats for part in x.split(';')) if isinstance(x, str) else False
)
df_all_files_concat_deduplicated.loc[:,'isMSOffice'] = df_all_files_concat_deduplicated['mimeType'].apply(
    lambda x: any(part.strip() in microsoftFormats for part in x.split(';')) if isinstance(x, str) else False
)

df_all_files_concat_deduplicated.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-files-deduplicated.csv")

sum_columns = ['fileSize']

def agg_func(column_name):
    if column_name in sum_columns:
        return 'sum'
    else:
        return lambda x: sorted(set(map(str, x)))

agg_funcs = {col: agg_func(col)for col in df_all_files_concat_deduplicated.columns if col != 'datasetID'}

df_tdr_all_files_combined = df_all_files_concat_deduplicated.groupby('datasetID').agg(agg_funcs).reset_index()
# Convert all list-type columns to comma-separated strings
for col in df_tdr_all_files_combined.columns:
    if df_tdr_all_files_combined[col].apply(lambda x: isinstance(x, list)).any():
        df_tdr_all_files_combined[col] = df_tdr_all_files_combined[col].apply(lambda x: '; '.join(map(str, x)))

tdr_all_datasets_deduplicated = df_tdr_all_files_combined.drop_duplicates(subset='datasetID', keep='first')
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated[["datasetID", "versionID", "totalVersion", "mimeType", "friendlyType", "fileSize", "creationDate", "publicationDate", "institution", "doi", "datasetTitle", "dataverse", "creationYear", "publicationYear", "isREADME", "isCodebook", "isDataDict", "friendlyFormat_manual", "isSoftware", "isCompressed", "isMSOffice"]]

#handles entries where aggregation returned a mixed 'False;True' value
def normalize_boolean_column(col):
    return col.apply(lambda x: True if isinstance(x, str) and 'true' in x.lower() else False)
bool_columns = ["isREADME", "isCodebook", "isDataDict", "isSoftware", "isCompressed", "isMSOffice"]
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated_pruned.copy()
for col in bool_columns:
    tdr_all_datasets_deduplicated_pruned[col] = normalize_boolean_column(tdr_all_datasets_deduplicated_pruned[col])
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated_pruned.rename(columns={'isREADME': 'containsREADME', 'isCodebook': 'containsCodebook', 'isDataDict': 'containsDataDict', 'isSoftware': 'containsSoftware', 'isCompressed': 'containsCompressed', 'isMSOffice': 'containsMSOffice', 'fileSize': 'datasetSize'})

#returns only the highest value for the version number
def extract_max_version(val):
    if isinstance(val, str):
        try:
            versions = [float(v.strip()) for v in val.split(';')]
            return max(versions)
        except ValueError:
            return val  # In case of unexpected format
    return val
tdr_all_datasets_deduplicated_pruned['totalVersion'] = tdr_all_datasets_deduplicated_pruned['totalVersion'].apply(extract_max_version)

#binning datasets by size
tdr_all_datasets_deduplicated_pruned = assign_size_bins(tdr_all_datasets_deduplicated_pruned, column='datasetSize', new_column='datasetSizeBin')

tdr_all_datasets_deduplicated_pruned.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-datasets-combined.csv")

size_by_year = df_all_files_concat_deduplicated.groupby('creationYear')['fileSize'].sum().reset_index()
size_by_year['fileGB'] = size_by_year['fileSize'] / 1000000000
print('Annual size summary')
print(size_by_year)
size_by_year.to_csv(f"outputs/{todayDate}_{institutionFilename}_annual-size-summary.csv")

#file format summary
##can substitute 'friendlyType' for 'mimeType' but will get some aggregating into 'unknown'
unique_datasets_per_format = df_all_files_concat_deduplicated.groupby('friendlyFormat_manual')['datasetID'].nunique()
print('Total file format summary')
print(unique_datasets_per_format)
unique_datasets_per_format.to_csv(f"outputs/{todayDate}_{institutionFilename}_unique-format-summary.csv")

#author assessment
##is ROR present
df_all_authors_concat_deduplicated = df_all_authors_concat_deduplicated.copy()
df_all_authors_concat_deduplicated.loc[:, 'missingROR'] = (df_all_authors_concat_deduplicated['rorID'].isna() | (df_all_authors_concat_deduplicated['rorID'] == ''))
##is any author ID system present
df_all_authors_concat_deduplicated.loc[:, 'missingAuthorScheme'] = (df_all_authors_concat_deduplicated['authorIdentifierScheme'].isna() |
    (df_all_authors_concat_deduplicated['authorIdentifierScheme'] == ''))
##ORCID present and appropriately formatted
df_all_authors_concat_deduplicated.loc[:, 'properORCID'] = (
    df_all_authors_concat_deduplicated['authorIdentifierScheme'].str.upper() == 'ORCID'
) & df_all_authors_concat_deduplicated['authorIdentifier'].str.contains('https://orcid.org/', na=False)
##is ORCID present but malformatted (not hyperlinked)
df_all_authors_concat_deduplicated.loc[:,'malformedORCID_noHyphens'] = (
    df_all_authors_concat_deduplicated['authorIdentifierScheme'].str.upper() == 'ORCID'
) & ~df_all_authors_concat_deduplicated['authorIdentifier'].str.contains('-', na=False)
##is ORCID present but malformatted (no dashes)
df_all_authors_concat_deduplicated.loc[:,'malformedORCID_noURL'] = (
    df_all_authors_concat_deduplicated['authorIdentifierScheme'].str.upper() == 'ORCID'
) & ~df_all_authors_concat_deduplicated['authorIdentifier'].str.contains('https://orcid.org/', na=False)
##is ORCID present but malformatted (single field)
df_all_authors_concat_deduplicated.loc[:,'malformedORCID_singleField'] = (
    df_all_authors_concat_deduplicated['authorIdentifierScheme'].str.upper() == 'ORCID'
) & df_all_authors_concat_deduplicated['authorIdentifierExpanded'].isna()

df_all_authors_concat_deduplicated.loc[:, 'malformedORCID_any'] = (
    df_all_authors_concat_deduplicated['malformedORCID_noHyphens'] |
    df_all_authors_concat_deduplicated['malformedORCID_noURL'] |
    df_all_authors_concat_deduplicated['malformedORCID_singleField']
)
##malformed author name (order)
df_all_authors_concat_deduplicated.loc[:, 'malformedOrder'] = (
    df_all_authors_concat_deduplicated['authorName'].str.contains(' ', na=False) & 
    ~df_all_authors_concat_deduplicated['authorName'].str.contains(',', na=False)
)
##malformed initial (standalone initial without period)
df_all_authors_concat_deduplicated.loc[:, 'malformedInitial'] = df_all_authors_concat_deduplicated['authorName'].str.contains(r'\b[A-Z]\b(?!\.)', regex=True)

df_all_authors_concat_deduplicated.loc[:, 'malformedName'] = (
    df_all_authors_concat_deduplicated['malformedOrder'] |
    df_all_authors_concat_deduplicated['malformedInitial'] 
)

df_all_authors_concat_deduplicated = df_all_authors_concat_deduplicated.sort_values(by='authorName')
df_all_authors_concat_deduplicated.to_csv(f'outputs/{todayDate}_{institutionFilename}_all-authors.csv', index=False)

if excludeDrafts:
    #authors
    df_all_authors_concat_deduplicated_published = df_all_authors_concat_deduplicated[df_all_authors_concat_deduplicated['currentStatus'] != 'DRAFT']
    df_all_authors_concat_deduplicated_published.to_csv(f'outputs/{todayDate}_{institutionFilename}_all-authors-PUBLISHED.csv', index=False)

    #datasets
    tdr_all_datasets_deduplicated_pruned_published = tdr_all_datasets_deduplicated_pruned[tdr_all_datasets_deduplicated_pruned['publicationDate'].notna() & (tdr_all_datasets_deduplicated_pruned['publicationDate'] != '')]
    tdr_all_datasets_deduplicated_pruned_published.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-datasets-combined-PUBLISHED.csv")

    #files
    df_all_files_concat_deduplicated_published = df_all_files_concat_deduplicated[df_all_files_concat_deduplicated['publicationDate'].notna() & (df_all_files_concat_deduplicated['publicationDate'] != '')]
    df_all_files_concat_deduplicated_published.to_csv(f"outputs/{todayDate}_{institutionFilename}_all-files-deduplicated-PUBLISHED.csv")

    #size summary
    size_by_year_published = df_all_files_concat_deduplicated_published.groupby('creationYear')['fileSize'].sum().reset_index()
    size_by_year_published['fileGB'] = size_by_year_published['fileSize'] / 1000000000
    size_by_year_published.to_csv(f"outputs/{todayDate}_{institutionFilename}_annual-size-summary-PUBLISHED.csv")

    #file format summary
    ##can substitute 'friendlyType' for 'mimeType' but will get some aggregating into 'unknown'
    unique_datasets_per_format_PUBLISHED = df_all_files_concat_deduplicated_published.groupby('friendlyFormat_manual')['datasetID'].nunique()
    unique_datasets_per_format_PUBLISHED.to_csv(f"outputs/{todayDate}_{institutionFilename}_unique-format-summary-PUBLISHED.csv")

print("Done.\n")
print(f"Time to run: {datetime.now() - startTime}\n")
if test:
    print("**REMINDER: THIS IS A TEST RUN, AND ANY RESULTS ARE NOT COMPLETE!**")