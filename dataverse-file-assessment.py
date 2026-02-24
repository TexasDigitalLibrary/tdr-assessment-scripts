import json
import os
import pandas as pd
import requests
import time
from datetime import datetime
from rapidfuzz import process, fuzz
from utils import adjust_descriptive_count_title, adjust_descriptive_count_description, add_final_source_column, analyze_keywords, assign_size_bins, count_words, extract_max_version, filter_sensitive_datasets, flag_sensitive_terms, get_day_of_week, is_us_federal_holiday, is_valid_orcid, is_valid_ror, retrieve_all_institutions

#### Toggles
#toggle for test environment (incomplete run, faster to complete)
test = False
#toggle to only look at your/one institution in TDR
only_my_institution = True 
#toggle for stage 3 retrieval
versions_API = False
#toggle for excluding unpublished
exclude_drafts = True
if exclude_drafts:
    status = 'publicationStatus:Published'
else:
    status = ''
#toggle to pull in biweekly DV report for institution
biweekly_report = False
#toggle to run sensitive data screening
sensitive_screen = True

#setting timestamp at start of script to calculate run time
start_time = datetime.now() 
#creating variable with current date for appending to filenames
today = datetime.now().strftime('%Y%m%d') 

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

my_institution = config['INSTITUTION']

##read in filename version of your institution's name
my_institution_filename = config['INSTITUTION']['filename']
###condition what goes in the filename based on toggle for which institution(s) to ping
if only_my_institution:
    institution_filename = my_institution_filename
else:
    institution_filename = 'all-institutions'
##read in short-hand version of your institution's name
my_institution_shortName = config['INSTITUTION']['myInstitution']

print(f'String to add to filenames: {my_institution_filename}.\n')
print(f'Short hand version of institution name: {my_institution_shortName}.\n')

words = config['WORDS']
compressed = config['COMPRESSED_FORMATS']

#read in biweekly report if you want to compare against list of registered users in institutional dataverse (only superusers can get that)
if biweekly_report:
    file_path = 'inputs/utexas-dataverse-reports.xlsx'
    sheet_name = 'users'

    tdr_users = pd.read_excel(file_path, sheet_name=sheet_name)

#getting script directory
script_directory = os.getcwd()
print(f'The script directory is {script_directory}.\n')

#creating directories
if test:
    if os.path.isdir('test'):
        print('test directory found - no need to recreate')
    else:
        os.mkdir('test')
        print('test directory has been created')
    os.chdir('test')
    if os.path.isdir('outputs'):
        print('test outputs directory found - no need to recreate')
    else:
        os.mkdir('outputs')
        print('test outputs directory has been created')
else:
    if os.path.isdir('outputs'):
        print('outputs directory found - no need to recreate')
    else:
        os.mkdir('outputs')
        print('outputs directory has been created')

print('Beginning to define API call parameters.')
url_tdr = 'https://dataverse.tdl.org/api/search/'

##set API-specific params
###Dataverse
if test and only_my_institution:
    page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] 
elif test and not only_my_institution: 
    page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] // 2 #halve page size if retrieving all institutions
elif not test:
    page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']

print(f'Retrieving {page_size} records per page over {page_limit_dataverse} pages.')

query = '*'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0

headers_tdr = {
    'X-Dataverse-key': config['KEYS']['dataverseToken']
}

params_tdr_ut_austin = {
    'q': query,
    'fq': status,
    'subtree': 'utexas',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'fq': status,
    'subtree': 'baylor',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'fq': status,
    'subtree': 'smu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'fq': status,
    'subtree': 'tamu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'fq': status,
    'subtree': 'txst',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'fq': status,
    'subtree': 'ttu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'fq': status,
    'subtree': 'uh',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'fq': status,
    'subtree': 'unthsc',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'fq': status,
    'subtree': 'tamug',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'fq': status,
    'subtree': 'tamiu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'fq': status,
    'subtree': 'uthscsa',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'fq': status,
    'subtree': 'utswmed',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'fq': status,
    'subtree': 'uta',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'fq': status,
    'subtree': 'twu',
    'type': 'dataset',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

all_params = {
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
        "Texas Women's University": params_tdr_twu
    }

tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

#substitute for your institution
if only_my_institution:
    if my_institution_shortName == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_shortName: all_params[my_institution_shortName]
        }
else:
    params_list = all_params

file_path = f'{script_directory}/tdr-affiliation-ror-matching.csv'

if os.path.exists(file_path):
    master_ror_matching = pd.read_csv(file_path)
    print(f'"{file_path}" exists and has been loaded into a DataFrame.')
else:
    print(f'"{file_path}" does not exist. DataFrame not loaded.')

print('Starting TDR retrieval.\n')
all_data = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataverse, page_size, page_limit_dataverse)

print('Starting TDR filtering.\n')
data_select_tdr = []
for item in all_data:
    id = item.get('global_id', '')
    type = item.get('type', '')
    institution = item.get('institution','')
    status = item.get('versionState', '')
    description = item.get('description', '')
    keywords = item.get('keywords', '')
    subjects = item.get('subjects', '')
    name = item.get('name', '')
    dataverse = item.get('name_of_dataverse', '')
    majorV = item.get('majorVersion', 0)
    minorV = item.get('minorVersion', 0)
    comboV = f'{majorV}.{minorV}'
    version_id = item.get('versionId', '')
    data_select_tdr.append({
        'institution': institution, 
        'doi': id,
        'type': type,
        'description': description,
        'keywords': keywords,
        'status': status,
        'dataset_title': name,
        'dataverse': dataverse,
        'major_version': majorV,
        'minor_version': minorV,
        'total_version': comboV,
        'version_id': version_id
    })

df_data_select_tdr = pd.DataFrame(data_select_tdr)

#ensuring full version
df_data_select_tdr['total_version'] = df_data_select_tdr['total_version'].apply(extract_max_version)
#remove dataverses and files
filtered_tdr = df_data_select_tdr[df_data_select_tdr['type'] == 'dataset']
#editing DOI field
filtered_tdr['doi'] = filtered_tdr['doi'].str.replace('doi:', '')
#add column for versioned
filtered_tdr['versioned'] = filtered_tdr.apply(lambda row: 'Versioned' if (row['major_version'] > 1) or (row['minor_version'] > 0) else 'Not versioned', axis=1)

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

#assessing title
filtered_tdr['title_reformatted'] = filtered_tdr['dataset_title'].str.replace('_', ' ') 
filtered_tdr['title_reformatted'] = filtered_tdr['dataset_title'].str.replace('-', ' ') #gets around text linked by underscores counting as 1 word
filtered_tdr['title_reformatted'] = filtered_tdr['title_reformatted'].str.lower()
filtered_tdr[['total_word_count_title', 'descriptive_word_count_title']] = filtered_tdr['title_reformatted'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words)))
filtered_tdr['descriptive_word_count_title'] = filtered_tdr.apply(adjust_descriptive_count_title, axis=1)
filtered_tdr['nondescriptive_word_count_title'] = filtered_tdr['total_word_count_title'] - filtered_tdr['descriptive_word_count_title']
#assessing description
filtered_tdr[['total_word_count_description', 'descriptive_word_count_description']] = filtered_tdr['description'].apply(lambda x: pd.Series(count_words(x, nondescriptive_words)))
filtered_tdr['descriptive_word_count_description'] = filtered_tdr.apply(adjust_descriptive_count_description, axis=1)
filtered_tdr['nondescriptive_word_count_description'] = filtered_tdr['total_word_count_description'] - filtered_tdr['descriptive_word_count_description']
#assessing keywords
filtered_tdr['keywords_metrics'] = filtered_tdr['keywords'].apply(lambda kw_list: analyze_keywords(kw_list, nondescriptive_words))
metrics_df = pd.DataFrame(filtered_tdr['keywords_metrics'].tolist())
filtered_tdr = pd.concat([filtered_tdr, metrics_df], axis=1)
filtered_tdr = filtered_tdr.drop(columns=['keywords_metrics'])
##flagging if insufficient descriptive terms in keywords, title, or description
filtered_tdr['nondescriptive_metadata'] = ((filtered_tdr['descriptive_word_count_title'] < 5) | (filtered_tdr['descriptive_word_count_description'] < 15) | (filtered_tdr['descriptive_keywords'] < 3))
##flagging if title ends in period
filtered_tdr['title_period'] = filtered_tdr['dataset_title'].str.endswith('.')
##flagging if title has blankspace in front or behind
filtered_tdr['title_extra_space'] = (filtered_tdr['dataset_title'].str.endswith(' ') | filtered_tdr['dataset_title'].str.startswith(' '))

#sort on status, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED'
filtered_tdr = filtered_tdr.sort_values(by='status', ascending=False)
filtered_tdr.to_csv(f'outputs/{today}_{institution_filename}_all-deposits.csv')
filtered_tdr_deduplicated = filtered_tdr.drop_duplicates(subset=['doi'], keep='first')
filtered_tdr_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-deposits-deduplicated.csv', index=False)

#create df of published datasets with draft version (retains both entries)
commonColumns = ['doi', 'dataset_title']
duplicates = filtered_tdr.duplicated(subset=commonColumns, keep=False)
dualStatusDatasets = filtered_tdr[duplicates]
dualStatusDatasets.to_csv(f'outputs/{today}_{institution_filename}_dual-status-datasets.csv', index=False)

#retrieving additional metadata for deposits by individual API call (one per DOI)
##retrieves both published and never-published draft datasets; if a published dataset is currently in DRAFT state, it will return the information for the DRAFT state
print('Starting Native API call')
url_tdr_native = 'https://dataverse.tdl.org/api/datasets/'

print(f'Total datasets to be analyzed: {len(filtered_tdr_deduplicated)}.\n')

results = []
initial_timeouts = []
final_timeouts = []
for doi in filtered_tdr_deduplicated['doi']:
    try:
        response = requests.get(f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving {doi}\n')
            results.append(response.json())
        else:
            # Non-timeout errors (like 404 or 500) go straight to permanent failures
            final_timeouts.append({"doi": doi, "reason": f"Status {response.status_code}"})
            
    except requests.exceptions.Timeout:
        initial_timeouts.append(doi)
    except requests.exceptions.RequestException as e:
        final_timeouts.append({"doi": doi, "reason": str(e)})

if initial_timeouts:
    print(f"\n--- Retrying {len(initial_timeouts)} timeouts with 10s limit ---\n")
    # Small pause to let the server recover
    time.sleep(2) 
    
    for doi in initial_timeouts:
        try:
            response = requests.get(
                f'{url_tdr_native}:persistentId/?persistentId=doi:{doi}', 
                headers=headers_tdr, 
                timeout=10 
            )
            if response.status_code == 200:
                print(f'Retrying {doi}\n')
                results.append(response.json())
            else:
                final_timeouts.append({"doi": doi, "reason": f"Retry Status {response.status_code}"})
        except Exception as e:
            # If it fails again, it's officially a permanent failure
            final_timeouts.append({"doi": doi, "reason": "Persistent Timeout/Error"})

data_tdr_native = {
    'datasets': results
}

print(f"INITIALLY FAILED: {len(initial_timeouts)}\n")
print(f"TOTAL FAILED: {len(final_timeouts)}\n")
if len(final_timeouts) > 0:
    print(final_timeouts)

print('Beginning dataframe subsetting\n')
data_select_tdr_native = [] 
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
    grantAgencies = []
    keywords = None
    notes = None
    depositor = 'None listed'
    contacts = 'None listed'
    contact_emails = 'None listed'
    for field in fields:
        if field['typeName'] == 'grantNumber':
            for grant in field.get('value', []):
                grant_number_agency = grant.get('grantNumberAgency', {}).get('value', '')
                grantAgencies.append(grant_number_agency)
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
        
    #set up counters for author
    num_authors = 0
    num_valid_orcid = 0
    num_valid_ror = 0

    for field in fields:
        if field['typeName'] == 'author':
            for position, author in enumerate(field.get('value', []), start=1):
                num_authors += 1  # Count every author

                name = author.get('authorName', {}).get('value', '')
                affiliation = author.get('authorAffiliation', {}).get('value', '')
                identifier = author.get('authorIdentifier', {}).get('value', '')
                scheme = author.get('authorIdentifierScheme', {}).get('value', '')
                affiliation_expanded = author.get('authorAffiliation', {}).get('expandedvalue', {}).get('termName', '')
                identifier_expanded = author.get('authorIdentifier', {}).get('expandedvalue', {}).get('@id', '')

                affiliationName = affiliation_expanded if affiliation_expanded else affiliation
                affiliation_ror = affiliation if affiliation_expanded else None

                # Check ORCID
                if is_valid_orcid(identifier):
                    num_valid_orcid += 1
                # Check ROR 
                if is_valid_ror(affiliation):
                    num_valid_ror += 1
    total_filesize = 0
    unique_content_types = set()
    fileCount = len(files)
    base_entry = {
    'dataset_id': dataset_id,
    'doi': doi,
    'notes': notes,
    "missing_orcid": (num_authors - num_valid_orcid) > 0,
    "missing_ror": (num_authors - num_valid_ror) > 0,
    'dataset_contact': contacts,
    'dataset_email': contact_emails,
    'dataset_depositor': depositor,
    'current_status': status2,
    'reuse_requirements': usage,
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
    if files:
        for file in files:
            file_info = file.get('dataFile', {})
            file_entry = base_entry.copy()
            file_entry.update({
                'file_id': file_info.get('id', ''),
                'public': file_info.get('restricted', ''),
                'filename': file_info.get('filename', ''),
                'mime_type': file_info.get('contentType', ''),
                'friendly_type': file_info.get('friendlyType', ''),
                'original_mime_type': file_info.get('originalFileFormat', file_info.get('contentType', '')),
                'original_friendly_type': file_info.get('originalFormatLabel', file_info.get('friendlyType', '')),
                'tabular': file_info.get('tabularData', ''),
                'file_size': file_info.get('filesize', 0),
                'original_file_size': file_info.get('originalFileSize', 0),
                'storage_identifier': file_info.get('storageIdentifier', ''),
                'creation_date': file_info.get('creationDate', ''),
                'publication_date': file_info.get('publicationDate', ''),
                'restricted': file.get('restricted', ''),
            })
            data_select_tdr_native.append(file_entry)
    else:
        file_entry = base_entry.copy()
        file_entry.update({
            'file_id': 'NO FILES',
            'public': 'NO FILES',
            'filename': 'NO FILES',
            'mime_type': 'NO FILES',
            'friendly_type': 'NO FILES',
            'original_mime_type': 'NO FILES',
            'original_friendly_type': 'NO FILES',
            'tabular': 'NO FILES',
            'file_size': 0,
            'original_file_size': 'NO FILES',
            'storage_identifier': 'NO FILES',
            'creation_date': None,
            'publication_date': None,
            'restricted': 'NO FILES',
        })
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

df_select_tdr_native = pd.json_normalize(data_select_tdr_native)
df_author_entries = pd.json_normalize(author_entries)
df_select_tdr_native['doi'] = df_select_tdr_native['doi'].str.replace('doi:', '')
df_author_entries['doi'] = df_author_entries['doi'].str.replace('doi:', '')
df_select_tdr_native['creation_date'] = pd.to_datetime(df_select_tdr_native['creation_date'])
df_select_tdr_native['file_creation_year'] = df_select_tdr_native['creation_date'].dt.year

df_select_tdr_native = assign_size_bins(df_select_tdr_native, column='file_size', new_column='file_size_bin')
df_select_concatenated = pd.merge(filtered_tdr_deduplicated, df_select_tdr_native, on='doi', how='left')
df_select_concatenated_exist = df_select_concatenated.dropna(subset=['dataset_id']).copy() #removes deaccessioned

df_select_concatenated_exist['dataset_id'] = df_select_concatenated_exist['dataset_id'].astype(int)
# As of 2025/12/05, temporarily coding out this CSV output
# df_select_concatenated_exist.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-deduplicated_expanded-metadata.csv', index=False)

#subset to datasets that are less than version 2.0 (no major update, no file additions)
df_select_concatenated_exist_majorVersion = df_select_concatenated_exist[df_select_concatenated_exist['major_version'] > 1]

#need to use Version endpoint to get info on published version of published datasets that are currently in DRAFT status and all published versions of a dataset with multiple PUBLISHED versions. This endpoint is public and does not return any DRAFTs.
#remove datasets that have never been published (will not return any info for this endpoint)
df_select_concatenated_exist_published = df_select_concatenated_exist_majorVersion[df_select_concatenated_exist_majorVersion['publication_date'].notnull()]
#deduplicate on dataset_id
df_select_concatenated_exist_published_dedup = df_select_concatenated_exist_published.drop_duplicates(subset='dataset_id', keep='first')

if versions_API:
    results_versions = []
    print('Beginning Version API query\n')
    for dataset_id in df_select_concatenated_exist_published_dedup['dataset_id']:
        try:
            response = requests.get(f'{url_tdr_native}{dataset_id}/versions')
            if response.status_code == 200:
                print(f'Retrieving versions of dataset #{dataset_id}')
                print()
                results_versions.append(response.json())
            else:
                print(f'Error retrieving dataset #{dataset_id}: {response.status_code}, {response.text}')
        except requests.exceptions.RequestException as e:
            print(f'Timeout error on DOI {doi}: {e}')

    data_tdr_versions = {
        'datasets': results_versions
    }
    print('Beginning dataframe subsetting\n')
    data_select_tdr_versions = [] 
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
                        "missing_orcid": (num_authors - num_valid_orcid) > 0,
                        "missing_ror": (num_authors - num_valid_ror) > 0,
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
                    data_select_tdr_versions.append(file_entry)
            else:
                file_entry = {
                    'dataset_id': dataset_id,
                    'doi': doi,
                    "missing_orcid": (num_authors - num_valid_orcid) > 0,
                    "missing_ror": (num_authors - num_valid_ror) > 0,
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
                data_select_tdr_versions.append(file_entry)
            
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

    df_select_tdr_versions = pd.json_normalize(data_select_tdr_versions)
    df_author_entries_versions = pd.json_normalize(author_entries_versions)
    df_select_tdr_versions['doi'] = df_select_tdr_versions['doi'].str.replace('doi:', '')
    df_author_entries_versions['doi'] = df_author_entries_versions['doi'].str.replace('doi:', '')
    #removing duplicate entries for a given file that has not changed across multiple versions
    df_select_tdr_versions['total_version'] = df_select_tdr_versions['total_version'].astype(float)
    df_select_tdr_versions['total_version'] = df_select_tdr_versions['total_version'].apply(extract_max_version)
    df_select_tdr_versions = df_select_tdr_versions.sort_values(by='total_version')
    df_select_tdr_versions_deduplicated = df_select_tdr_versions.drop_duplicates(subset=['dataset_id', 'storage_identifier'], keep='first')

    df_select_tdr_versions_deduplicated = assign_size_bins(df_select_tdr_versions_deduplicated, column='file_size', new_column='file_size_bin')

    df_select_versions_concatenated_released = pd.merge(df_select_tdr_versions_deduplicated, filtered_tdr_deduplicated, on='doi', how='left')

    #pruning and renaming columns in the two dataframes that collectively (should) have all of the files (from the Native and the Version endpoints)
    df_version_pruned = df_select_versions_concatenated_released[['version_id_x', 'dataset_id', 'dataset_contact', 'dataset_email', 'dataset_depositor', 'total_version_x', 'keywords', 'malformatted_keywords', 'total_keywords', 'descriptive_keywords', 'title_period', 'title_extra_space', 'filename', 'file_id', 'original_mime_type', 'original_friendly_type', 'file_size', 'storage_identifier', 'creation_date', 'publication_date', 'institution', 'doi', 'file_size_bin', 'dataset_title', 'dataverse', 'restricted', 'license', 'reuse_requirements', 'confidentiality', 'permission', 'restrictions', 'conditions', 'disclaimer', 'terms_access', 'data_access_place', 'availability', 'contact_access']]
    df_version_pruned = df_version_pruned.rename(columns={'total_version_x': 'total_version', 'filename_x': 'filename', 'file_size_x': 'file_size', 'storage_identifier_x': 'storage_identifier', 'creation_date_x': 'creation_date', 'publication_date_x':'publication_date', 'version_id_x': 'version_id'})
    df_version_pruned['creation_year'] = pd.to_datetime(df_version_pruned['creation_date'], format='%Y-%m-%d').dt.year
    df_version_pruned['publication_year'] = pd.to_datetime(df_version_pruned['publication_date'], format='%Y-%m-%d').dt.year

df_native_pruned = df_select_concatenated_exist[['dataset_id', 'dataset_title', 'description', 'notes', 'dataset_contact', 'dataset_email', 'dataset_depositor','version_id', 'current_status', 'total_version', 'keywords', 'malformatted_keywords', 'title_period', 'title_extra_space', 'descriptive_word_count_title', 'descriptive_word_count_description', 'descriptive_keywords', 'nondescriptive_metadata','missing_orcid', 'missing_ror', 'filename', 'file_id', 'original_mime_type', 'original_friendly_type', 'file_size', 'storage_identifier', 'creation_date', 'publication_date', 'institution', 'doi', 'file_size_bin', 'dataverse', 'restricted', 'license', 'reuse_requirements', 'confidentiality', 'permission', 'restrictions', 'conditions', 'disclaimer', 'terms_access', 'data_access_place', 'availability', 'contact_access']]

df_native_pruned = df_native_pruned.copy()
df_native_pruned['creation_year'] = pd.to_datetime(df_native_pruned['creation_date'], format='%Y-%m-%dT%H:%M:%SZ').dt.year
df_native_pruned['publication_year'] = pd.to_datetime(df_native_pruned['publication_date'], format='%Y-%m-%d').dt.year

if versions_API:
    df_all_files_concat = pd.concat([df_version_pruned, df_native_pruned], ignore_index=True)
    df_all_files_concat = df_all_files_concat.rename(columns={'title': 'dataset_title'})

    #deduplicate
    ##create fake versionID for drafts to ensure proper sorting and deduplicating
    df_all_files_concat['version_id'] = df_all_files_concat['version_id'].fillna(9999999)
    df_all_files_concat['version_id'] = pd.to_numeric(df_all_files_concat['version_id'], errors='coerce')
    df_all_files_concat = df_all_files_concat.sort_values(by='version_id')
    df_all_files_concat_deduplicated = df_all_files_concat.drop_duplicates(subset=['doi', 'storage_identifier'], keep='first')
    df_all_files_concat_deduplicated = df_all_files_concat_deduplicated.copy()
    df_all_files_concat_deduplicated['version_id'] = df_all_files_concat_deduplicated['version_id'].replace(9999999, None)
    df_all_authors_concat = pd.concat([df_author_entries, df_author_entries_versions], ignore_index=True)
    df_all_authors_concat_deduplicated = df_all_authors_concat.drop_duplicates(subset=['doi', 'author_name', 'author_affiliation', 'current_status'], keep='first')
else:
    #sort on status and then total version, setting 'DRAFT' at bottom to remove this version for published datasets that are in draft state, retain entry of 'PUBLISHED' and then to keep the earliest version
    df_native_pruned = df_native_pruned.sort_values(by=['current_status', 'total_version'], ascending=[False, True])
    df_all_files_concat_deduplicated = df_native_pruned.drop_duplicates(subset=['doi', 'storage_identifier'], keep='first')
    df_all_authors_concat_deduplicated = df_author_entries.drop_duplicates(subset=['doi', 'author_name', 'author_affiliation', 'current_status'], keep='first')

#metadata assessment
##documentation presence
df_all_files_concat_deduplicated.loc[:,'is_readme'] = df_all_files_concat_deduplicated['filename'].str.contains('readme|read_me', case=False)
df_all_files_concat_deduplicated.loc[:,'is_codebook'] = df_all_files_concat_deduplicated['filename'].str.contains('codebook', case=False)
df_all_files_concat_deduplicated.loc[:,'is_data_dictionary'] = df_all_files_concat_deduplicated['filename'].str.contains('dictionary', case=False) #need to check sensitivity
##if no documentation found
df_all_files_concat_deduplicated['has_documentation'] = (~df_all_files_concat_deduplicated['is_readme'] &~df_all_files_concat_deduplicated['is_codebook'] &~df_all_files_concat_deduplicated['is_data_dictionary'])

##create separate friendlyFormat column
formatMap = config['FORMAT_MAP']
df_all_files_concat_deduplicated.loc[:,'friendly_format_manual'] = df_all_files_concat_deduplicated['original_mime_type'].apply(
    lambda x: formatMap.get(x.strip(), x.strip()) if isinstance(x, str) and x != 'no match found' else 'no files'
)
##file formats
softwareFormats = set(config['SOFTWARE_FORMATS'].keys())
compressedFormats = set(config['COMPRESSED_FORMATS'].keys())
microsoftFormats = set(config['MICROSOFT_FORMATS'].keys())
# Assume softwareFormats is a set of friendly software format names
df_all_files_concat_deduplicated.loc[:,'is_software'] = df_all_files_concat_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in softwareFormats for part in x.split(';')) if isinstance(x, str) else False
)
df_all_files_concat_deduplicated.loc[:,'is_compressed'] = df_all_files_concat_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in compressedFormats for part in x.split(';')) if isinstance(x, str) else False
)
df_all_files_concat_deduplicated.loc[:,'is_microsoft_office'] = df_all_files_concat_deduplicated['original_mime_type'].apply(
    lambda x: any(part.strip() in microsoftFormats for part in x.split(';')) if isinstance(x, str) else False
)

# Manual file extension grabbing
df_all_files_concat_deduplicated['extension_minimum'] = df_all_files_concat_deduplicated['filename'].str.extract(r'(\.[^.]+)$')
df_all_files_concat_deduplicated['extension_maximum'] = df_all_files_concat_deduplicated['filename'].str.extract(r'(\..*)')

if exclude_drafts:
    df_all_files_concat_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-files-deduplicated-PUBLISHED.csv', index=False)
else:
    df_all_files_concat_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-files-deduplicated-ALL.csv', index=False)

#date modifications
df_all_files_concat_deduplicated['publication_day'] = df_all_files_concat_deduplicated['publication_date'].apply(get_day_of_week)
weekend_days = {'Saturday', 'Sunday'}
df_all_files_concat_deduplicated['is_weekend'] = df_all_files_concat_deduplicated['publication_day'].isin(weekend_days)
df_all_files_concat_deduplicated['is_holiday'] = df_all_files_concat_deduplicated['publication_date'].apply(is_us_federal_holiday)
break_ranges = [ #Sunday to Saturday of a given week for full-week holidays
    ('2023-11-19', '2023-11-25'),
    ('2023-12-24', '2024-01-07'),
    ('2024-03-10', '2024-03-16'),
    ('2024-11-24', '2024-11-28'),
    ('2024-12-22', '2025-01-05'),
    ('2025-03-16', '2025-03-22'),
    ('2025-11-23', '2025-11-29'),
    ('2025-12-21', '2026-01-04'),
    # Add more as needed
]
df_all_files_concat_deduplicated['publication_date'] = pd.to_datetime(df_all_files_concat_deduplicated['publication_date'])
def is_in_break(date, ranges):
    for start, end in ranges:
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        if start_dt <= date <= end_dt:
            return True
    return False

df_all_files_concat_deduplicated['during_break'] = df_all_files_concat_deduplicated['publication_date'].apply(lambda x: is_in_break(x, break_ranges))

sum_columns = ['file_size']

def agg_func(column_name):
    if column_name in sum_columns:
        return 'sum'
    else:
        return lambda x: sorted(set(map(str, x)))

agg_funcs = {col: agg_func(col)for col in df_all_files_concat_deduplicated.columns if col != 'dataset_id'}

df_tdr_all_files_combined = df_all_files_concat_deduplicated.groupby('dataset_id').agg(agg_funcs).reset_index()
# Convert all list-type columns to comma-separated strings
for col in df_tdr_all_files_combined.columns:
    if df_tdr_all_files_combined[col].apply(lambda x: isinstance(x, list)).any():
        df_tdr_all_files_combined[col] = df_tdr_all_files_combined[col].apply(lambda x: '; '.join(map(str, x)))

tdr_all_datasets_deduplicated = df_tdr_all_files_combined.drop_duplicates(subset='dataset_id', keep='first')
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated[['dataset_id', 'description', 'notes', 'dataset_contact', 'dataset_email','dataset_depositor','version_id', 'total_version', 'keywords', 'malformatted_keywords', 'title_period', 'title_extra_space', 'descriptive_word_count_title', 'descriptive_word_count_description', 'descriptive_keywords', 'nondescriptive_metadata','missing_orcid', 'missing_ror', 'original_mime_type', 'original_friendly_type', 'file_size', 'creation_date', 'publication_date', 'is_holiday', 'is_weekend', 'institution', 'doi', 'dataset_title', 'dataverse', 'creation_year', 'publication_year', 'restricted', 'license', 'reuse_requirements', 'confidentiality', 'permission', 'restrictions', 'conditions', 'disclaimer', 'terms_access', 'data_access_place', 'availability', 'contact_access', 'is_readme', 'is_codebook', 'is_data_dictionary', 'has_documentation', 'friendly_format_manual', 'is_software', 'is_compressed', 'is_microsoft_office']]

#handles entries where aggregation returned a mixed 'False;True' value
def normalize_boolean_column(col):
    return col.apply(lambda x: True if isinstance(x, str) and 'true' in x.lower() else False)
bool_columns = ['is_readme', 'is_codebook', 'is_data_dictionary', 'is_software', 'is_compressed', 'is_microsoft_office', 'has_documentation', 'is_holiday', 'is_weekend']
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated_pruned.copy()
for col in bool_columns:
    tdr_all_datasets_deduplicated_pruned[col] = normalize_boolean_column(tdr_all_datasets_deduplicated_pruned[col])
tdr_all_datasets_deduplicated_pruned = tdr_all_datasets_deduplicated_pruned.rename(columns={'is_readme': 'contains_readme', 'is_codebook': 'contains_codebook', 'is_data_dictionary': 'contains_data_dictionary', 'is_software': 'contains_software', 'is_compressed': 'contains_compressed', 'is_microsoft_office': 'contains_microsoft_office', 'file_size': 'dataset_size'})

tdr_all_datasets_deduplicated_pruned['total_version'] = tdr_all_datasets_deduplicated_pruned['total_version'].apply(extract_max_version)

#binning datasets by size
tdr_all_datasets_deduplicated_pruned = assign_size_bins(tdr_all_datasets_deduplicated_pruned, column='dataset_size', new_column='dataset_size_bin')

if exclude_drafts:
    tdr_all_datasets_deduplicated_pruned.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-PUBLISHED.csv', index=False)
else:
    tdr_all_datasets_deduplicated_pruned.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-ALL.csv', index=False)

##creating flagged dataset list
if biweekly_report:
    if exclude_drafts:
        tdr_all_datasets_deduplicated_pruned['missing_orcid'] = (tdr_all_datasets_deduplicated_pruned['missing_orcid'].astype(str).str.upper() == "TRUE")
        tdr_all_datasets_deduplicated_pruned['missing_ror'] = (tdr_all_datasets_deduplicated_pruned['missing_ror'].astype(str).str.upper() == "TRUE")
        flagged_datasets = tdr_all_datasets_deduplicated_pruned[tdr_all_datasets_deduplicated_pruned['missing_orcid'] |tdr_all_datasets_deduplicated_pruned['missing_ror']]
        flagged_datasets.to_csv(f'outputs/{today}_{institution_filename}_flagged-datasets-PUBLISHED.csv', index=False)
        flagged_datasets['dataset_email'] = flagged_datasets['dataset_email'].str.split('; ')
        flagged_contacts = flagged_datasets.explode('dataset_email')
        flagged_contacts.to_csv(f'outputs/{today}_{institution_filename}_flagged-contacts-PUBLISHED.csv', index=False)
        flagged_contacts_dedup = flagged_contacts.drop_duplicates(subset=['dataset_email'], keep='first')
        flagged_contacts_dedup = flagged_contacts_dedup[['dataset_id', 'dataset_contact', 'dataset_email', 'dataset_depositor', 'doi', 'dataset_title']]
        flagged_contacts_dedup.to_csv(f'outputs/{today}_{institution_filename}_flagged-contacts-PUBLISHED-dedup.csv', index=False)
        flagged_contacts_identified = pd.merge(
            flagged_contacts_dedup,
            tdr_users,
            left_on='dataset_email',
            right_on='email',
            how='left'
        )
        flagged_contacts_identified.to_csv(f'outputs/{today}_{institution_filename}_flagged-contacts-PUBLISHED-dedup-identified.csv', index=False)

    else:
        flagged_datasets = tdr_all_datasets_deduplicated_pruned[(tdr_all_datasets_deduplicated_pruned['missing_orcid'] == True) | (tdr_all_datasets_deduplicated_pruned['missing_ror'] == True)]
        flagged_datasets.to_csv(f'outputs/{today}_{institution_filename}_flagged-datasets-ALL.csv', index=False)

#size summary
size_by_year = df_all_files_concat_deduplicated.groupby('creation_year')['file_size'].sum().reset_index()
size_by_year['fileGB'] = size_by_year['file_size'] / 1000000000
# print('Annual size summary')
# print(size_by_year)
if exclude_drafts:
    size_by_year.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-annual-size-PUBLISHED.csv', index=False)
else:
    size_by_year.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-annual-size-ALL.csv', index=False)

#file format summary
##can substitute 'friendly_type' for 'original_mime_type' but will get some aggregating into 'unknown'
unique_datasets_per_format = df_all_files_concat_deduplicated.groupby('friendly_format_manual')['dataset_id'].nunique()
# print('Total file format summary')
# print(unique_datasets_per_format)
if exclude_drafts:
    unique_datasets_per_format.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-unique-format-PUBLISHED.csv', index=False)
else:
    unique_datasets_per_format.to_csv(f'outputs/{today}_{institution_filename}_SUMMARY-unique-format-ALL.csv', index=False)

#author assessment
##fuzzy matching author names
###sorting by length to get it to retain a longer, more detailed name (e.g., with middle initial vs. without)
unique_names = sorted(
    df_all_authors_concat_deduplicated['author_name'].unique(), 
    key=len, 
    reverse=True
)
standardized_names = {}

for name in unique_names:
    if not standardized_names:
        # 100 is maximum score
        standardized_names[name] = (name, 100.0)
        continue

    result = process.extractOne(
        name, 
        standardized_names.keys(), 
        scorer=fuzz.token_sort_ratio
    )
    
    if result:
        match, score, _ = result
        if score > 80:
            standardized_names[name] = (match, score)
        else:
            standardized_names[name] = (name, 100.0)
    else:
        standardized_names[name] = (name, 100.0)

results_map = df_all_authors_concat_deduplicated['author_name'].map(standardized_names)

df_all_authors_concat_deduplicated['author_name_standardized'] = results_map.apply(lambda x: x[0])
df_all_authors_concat_deduplicated['match_score'] = results_map.apply(lambda x: x[1])

##is ROR present
df_all_authors_concat_deduplicated = df_all_authors_concat_deduplicated.copy()
df_all_authors_concat_deduplicated.loc[:, 'missing_ror'] = (df_all_authors_concat_deduplicated['ror_id'].isna() | (df_all_authors_concat_deduplicated['ror_id'] == ''))
##is any author ID system present
df_all_authors_concat_deduplicated.loc[:, 'missing_author_scheme'] = (df_all_authors_concat_deduplicated['author_identifier_scheme'].isna() |
    (df_all_authors_concat_deduplicated['author_identifier_scheme'] == ''))
##ROR present and appropriately formatted
df_all_authors_concat_deduplicated.loc[:, 'proper_ror'] = df_all_authors_concat_deduplicated['ror_id'].str.contains('https://ror.org/', na=False)
##ORCID present and appropriately formatted
df_all_authors_concat_deduplicated.loc[:, 'proper_orcid'] = (
    df_all_authors_concat_deduplicated['author_identifier_scheme'].str.upper() == 'ORCID'
) & df_all_authors_concat_deduplicated['author_identifier'].str.contains('https://orcid.org/00', na=False)
##is ORCID present but malformatted (not hyperlinked)
df_all_authors_concat_deduplicated.loc[:,'malformed_orcid_no_hyphens'] = (
    df_all_authors_concat_deduplicated['author_identifier_scheme'].str.upper() == 'ORCID'
) & ~df_all_authors_concat_deduplicated['author_identifier'].str.contains('-', na=False)
##is ORCID present but malformatted (no dashes)
df_all_authors_concat_deduplicated.loc[:,'malformed_orcid_no_url'] = (
    df_all_authors_concat_deduplicated['author_identifier_scheme'].str.upper() == 'ORCID'
) & ~df_all_authors_concat_deduplicated['author_identifier'].str.contains('https://orcid.org/00', na=False)
##is ORCID present but malformatted (space between shoulder and identifier)
df_all_authors_concat_deduplicated.loc[:,'malformed_orcid_space'] = (
    df_all_authors_concat_deduplicated['author_identifier_scheme'].str.upper() == 'ORCID'
) & ~df_all_authors_concat_deduplicated['author_identifier'].str.contains('https://orcid.org/ 00', na=False)
##is ORCID present but malformatted (single field)
df_all_authors_concat_deduplicated.loc[:,'malformed_orcid_single_field'] = (
    df_all_authors_concat_deduplicated['author_identifier_scheme'].str.upper() == 'ORCID'
) & df_all_authors_concat_deduplicated['author_identifier_expanded'].isna()

df_all_authors_concat_deduplicated.loc[:, 'malformed_orcid_any'] = (
    df_all_authors_concat_deduplicated['malformed_orcid_no_hyphens'] |
    df_all_authors_concat_deduplicated['malformed_orcid_no_url'] |
    df_all_authors_concat_deduplicated['malformed_orcid_space'] |
    df_all_authors_concat_deduplicated['malformed_orcid_single_field']
)
##malformed author name (order)
df_all_authors_concat_deduplicated.loc[:, 'malformed_order'] = (
    df_all_authors_concat_deduplicated['author_name'].str.contains(' ', na=False) & 
    ~df_all_authors_concat_deduplicated['author_name'].str.contains(',', na=False)
)
##malformed initial (standalone initial without period)
df_all_authors_concat_deduplicated.loc[:, 'malformed_initial'] = df_all_authors_concat_deduplicated['author_name'].str.contains(r'\b[A-Z]\b(?!\.)', regex=True)

df_all_authors_concat_deduplicated.loc[:, 'malformed_name'] = (
    df_all_authors_concat_deduplicated['malformed_order'] |
    df_all_authors_concat_deduplicated['malformed_initial'] 
)

if exclude_drafts:
    df_all_authors_concat_deduplicated = df_all_authors_concat_deduplicated.sort_values(by='author_name')
    df_all_authors_concat_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-authors-PUBLISHED.csv', index=False)
else:
    df_all_authors_concat_deduplicated = df_all_authors_concat_deduplicated.sort_values(by='author_name')
    df_all_authors_concat_deduplicated.to_csv(f'outputs/{today}_{institution_filename}_all-authors-ALL.csv', index=False)

df_all_affiliations_dedup = df_all_authors_concat_deduplicated.drop_duplicates(subset=['author_affiliation'], keep='first')
df_all_affiliations_dedup = df_all_affiliations_dedup.rename(columns={'author_affiliation': 'affiliation'})

if master_ror_matching is None: #create master file if it doesn't exist yet
    print('No existing master file found, creating new one.\n')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup) - 1}\n')
    df_all_affiliations_dedup.to_csv(f'{script_directory}/tdr-affiliation-ror-matching.csv', index=False)
else: #concat master file with new list of unique affiliations, drop duplicates (keep first will retain existing matches)
    print('Found existing master file, adding and deduplicating.\n')
    df_all_affiliations_dedup_expanded = pd.concat([master_ror_matching, df_all_affiliations_dedup])
    print(f'Total affiliations: {len(df_all_affiliations_dedup_expanded)}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded.drop_duplicates(subset=['affiliation'], keep='first')
    print(f'Total unique affiliations: {len(df_all_affiliations_dedup_expanded_pruned) - 1}\n')
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned[['affiliation', 'ror', 'flag-generic']]
    ##remove blanks
    df_all_affiliations_dedup_expanded_pruned = df_all_affiliations_dedup_expanded_pruned.dropna(subset=['affiliation'])
    df_all_affiliations_dedup_expanded_pruned.to_csv(f'{script_directory}/tdr-affiliation-ror-matching-TEMP.csv', index=False)

#dataverse-level summary
query = '*'
params_tdr_ut_austin = {
    'q': query,
    'fq': status,
    'subtree': 'utexas',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_baylor = {
    'q': query,
    'fq': status,
    'subtree': 'baylor',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_smu = {
    'q': query,
    'fq': status,
    'subtree': 'smu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamu = {
    'q': query,
    'fq': status,
    'subtree': 'tamu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_txst = {
    'q': query,
    'fq': status,
    'subtree': 'txst',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_ttu = {
    'q': query,
    'fq': status,
    'subtree': 'ttu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_houston = {
    'q': query,
    'fq': status,
    'subtree': 'uh',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_hscfw = {
    'q': query,
    'fq': status,
    'subtree': 'unthsc',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_tamug = {
    'q': query,
    'fq': status,
    'subtree': 'tamug',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_tamui = {
    'q': query,
    'fq': status,
    'subtree': 'tamiu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utsah = {
    'q': query,
    'fq': status,
    'subtree': 'uthscsa',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}
params_tdr_utswm = {
    'q': query,
    'fq': status,
    'subtree': 'utswmed',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_uta = {
    'q': query,
    'fq': status,
    'subtree': 'uta',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

params_tdr_twu = {
    'q': query,
    'fq': status,
    'subtree': 'twu',
    'type': 'dataverse',
    'start': page_start_dataverse,
    'page': page_increment,
    'per_page': page_limit_dataverse
}

all_params = {
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
        "Texas Women's University": params_tdr_twu
    }

tamu_combined_params = {
        'TAMU': params_tdr_tamu,
        'TAMU Galveston': params_tdr_tamug,
        'TAMU International': params_tdr_tamui
}

#substitute for your institution
if only_my_institution:
    if my_institution_shortName == 'TAMU':
        params_list = tamu_combined_params
    else:
        params_list = {
            my_institution_shortName: all_params[my_institution_shortName]
        }
else:
    params_list = all_params

print('Beginning to define API call parameters.')

##(re)set API-specific params
page_limit_dataverse = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] if test else config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size = config['VARIABLES']['PAGE_SIZES']['dataverse_test'] if test else config['VARIABLES']['PAGE_SIZES']['dataverse_prod']
print(f'Retrieving {page_size} records per page over {page_limit_dataverse} pages.')

###for TDR, affiliation is not reliable for returning all relevant results; the DOI prefix is used as the most generic common denominator for datasets
query = '10.18738/T8/'
page_start_dataverse = config['VARIABLES']['PAGE_STARTS']['dataverse']
page_increment = config['VARIABLES']['PAGE_INCREMENTS']['dataverse']
k = 0

print('Starting TDR retrieval.\n')
all_dataverses = retrieve_all_institutions(url_tdr, params_list, headers_tdr, page_start_dataverse, page_size, page_limit_dataverse)

print('Starting TDR filtering.\n')
dataverses_select_tdr = []
for item in all_dataverses:
    name = item.get('name', '')
    type = item.get('type', '')
    url = item.get('url','')
    identifier = item.get('identifier', '')
    description = item.get('description', '')
    published = item.get('published_at', '')
    status = item.get('publicationStatuses', '')
    affiliation = item.get('affiliation', '')
    parent_dataverse_name = item.get('parentDataverseName', '')
    parent_dataverse_id = item.get('parentDataverseIdentifier', '')
    dataverses_select_tdr.append({
        'dataverse_name': name, 
        'url': url,
        'identifier': identifier,
        'type': type,
        'status': status,
        'description': description,
        'published': published,
        'affiliation': affiliation,
        'parent_dataverse_name': parent_dataverse_name,
        'parent_dataverse_id': parent_dataverse_id
    })

df_dataverses_select_tdr = pd.DataFrame(dataverses_select_tdr)

print('Starting Native API call\n')
url_tdr_native = 'https://dataverse.tdl.org/api/dataverses/'

results = []
initial_timeouts_dv = []
final_timeouts_dv = []
for identifier in df_dataverses_select_tdr['identifier']:
    try:
        response = requests.get(f'{url_tdr_native}/{identifier}', headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving dataverse: {identifier}\n')
            results.append(response.json())
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
                results.append(response.json())
            else:
                final_timeouts_dv.append({"identifier": identifier, "reason": f"Retry Status {response.status_code}"})
        except Exception as e:
            final_timeouts_dv.append({"identifier": identifier, "reason": "Persistent Timeout/Error"})

data_tdr_native = {
    'datasets': results
}

print(f"INITIALLY FAILED: {len(initial_timeouts_dv)}\n")
print(f"TOTAL FAILED: {len(final_timeouts_dv)}\n")
print(final_timeouts_dv)

data_tdr_native = {
    'dataverses': results
}

print('Beginning dataframe subsetting\n')
data_select_tdr_native = [] 
for item in data_tdr_native['dataverses']:
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
    data_select_tdr_native.append({
        'id': id,
        'dataverse_contact': dataverse_contact,
        'contact_count': contactsCount, 
        'owner': owner,
        'dataverse_type': dataverse_type,
        'permission': permission,
        'metadata_block': metadata_block,
        'facet_root': facet_root,
        'created': created,
        'identifier': identifier,
        'released': released
    })

df_select_tdr_native = pd.json_normalize(data_select_tdr_native)

#merge dfs
df_select_concatenated = pd.merge(df_dataverses_select_tdr, df_select_tdr_native, on='identifier', how='left')

url_contents = 'https://dataverse.tdl.org/api/dataverses/{}/contents'
url_storagesize = 'https://dataverse.tdl.org/api/dataverses/{}/storagesize'

contents_results = []
storagesize_results = []

print(f'Retrieving dataverse information on {len(df_dataverses_select_tdr)} dataverses.\n')

for identifier in df_dataverses_select_tdr['identifier']:
    # Fetch contents
    try:
        response = requests.get(url_contents.format(identifier), headers=headers_tdr, timeout=5)
        if response.status_code == 200:
            print(f'Retrieving contents for {identifier}\n')
            contents_results.append(response.json())
        else:
            print(f'Error retrieving contents for {identifier}: {response.status_code}, {response.text}\n')
            contents_results.append(None)
    except requests.exceptions.RequestException as e:
        print(f'Timeout error on contents {identifier}: {e}\n')
        contents_results.append(None)
    
    # # Fetch storagesize
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

# df_select_concatenated['contents'] = contents_results
# df_select_concatenated['storagesize'] = storagesize_results

#reporting number of datasets and dataverses
# Count dataverses
num_dataverses = [
    sum(1 for item in (contents.get('data', []) if isinstance(contents, dict) else []) 
        if item.get('type') == 'dataverse')
    for contents in contents_results
]

# Count datasets
num_datasets = [
    sum(1 for item in (contents.get('data', []) if isinstance(contents, dict) else []) 
        if item.get('type') == 'dataset')
    for contents in contents_results
]

# Get dataset DOIs
dataset_dois = [
    '; '.join(
        item.get('persistentUrl', '').replace('https://doi.org/', '')
        for item in (contents.get('data', []) if isinstance(contents, dict) else [])
        if item.get('type') == 'dataset' and 'persistentUrl' in item
    )
    for contents in contents_results
]

# Add to DataFrame
df_select_concatenated['num_dataverses'] = num_dataverses
df_select_concatenated['num_datasets'] = num_datasets
df_select_concatenated['dataset_dois'] = dataset_dois

df_select_concatenated.to_csv(f'outputs/{today}_{institution_filename}_all-dataverses.csv', index=False)
df_select_concatenated_pruned = df_select_concatenated[['dataverse_name', 'url', 'identifier', 'parent_dataverse_name', 'parent_dataverse_id', 'id', 'dataverse_contact', 'owner', 'dataset_dois']]

# Combining dataset and dataverse dfs
dataverse_dataset_merged = pd.merge(
        tdr_all_datasets_deduplicated_pruned,
        df_select_concatenated_pruned,
        left_on='dataverse',
        right_on='dataverse_name',
        how='left'
    )
dataverse_dataset_merged = dataverse_dataset_merged.fillna({'dataverse_name': 'Default institutional dataverse', 'parent_dataverse_name': 'None', 'parent_dataverse_id': 0, 'dataverse_contact': 'None', 'owner': 0, 'id': 0, 'dataset_dois': 'Not applicable'})
if exclude_drafts:  
    dataverse_dataset_merged.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses-PUBLISHED.csv', index=False)
else:
    dataverse_dataset_merged.to_csv(f'outputs/{today}_{institution_filename}_all-datasets-combined-with-dataverses-ALL.csv', index=False)

## Combining dataset-dataverse and author dfs
authors_dataverse_dataset_merged = pd.merge(
        df_all_authors_concat_deduplicated,
        dataverse_dataset_merged,
        left_on='doi',
        right_on='doi',
        how='left'
    )
if exclude_drafts:
    authors_dataverse_dataset_merged.to_csv(f'outputs/{today}_{institution_filename}_all-authors-datasets-dataverses-PUBLISHED.csv', index=False)
else:
    authors_dataverse_dataset_merged.to_csv(f'outputs/{today}_{institution_filename}_all-authors-datasets-dataverses.csv', index=False)

if sensitive_screen:
 
    columns_to_scan = ['dataset_title', 'dataverse', 'description', 'notes', 'keywords']
    # using incomplete terms in some cases to handle variation (e.g., sensitiv for sensitivity vs. sensitive)
    sensitive_terms = [
        'transcript', 'survey', 'interview', 'video', 'recording', 'photograph', 'demographic', 'clinical', 'medical', 
        'trial', 'human', 'people', 'participant', 'patient', 'student', 'children', 'youth', 'famil', 'household', 'indigenous', 
        'tribal', 'ethnic', 'racial', 'race', 'gender', 'sensitiv', 'deidentified', 'de-identified', 
        'anonymized', 'masked', 'obfuscated', 'redacted', 'codebook', 'qualtrics', 'redcap', 'nvivo'
    ]

    df_flagged = flag_sensitive_terms(dataverse_dataset_merged, sensitive_terms, columns_to_scan)
    df_sensitive = filter_sensitive_datasets(df_flagged)
    df_final = add_final_source_column(df_sensitive)
    df_final_filtered = df_final[df_final['final_source'].str.strip() != '']
    df_final_filtered = df_final_filtered.drop_duplicates(subset=['doi'], keep='first')
    df_final_filtered = df_final_filtered[['institution', 'dataverse', 'doi', 'dataset_title', 'dataset_id', 'description', 'notes', 'keywords', 'dataset_contact', 'dataset_email', 'creation_date', 'publication_date', 'original_mime_type', 'original_friendly_type', 'restricted', 'license', 'flags', 'source', 'final_source']]

    df_final_filtered.to_csv(f'outputs/{today}_{institution_filename}_sensitive-data-flagged.csv', index=False)

    print(f'Number of flagged datasets: {len(df_final_filtered)} out of {len(dataverse_dataset_merged)} total datasets.\n')

# print('Done.\n')
if master_ror_matching is not None:
    print(f'Number of new affiliations to check: {len(df_all_affiliations_dedup_expanded_pruned) - len(master_ror_matching)}.\n')
print(f'Done\n---Time to run: {datetime.now() - start_time}---\n')
if test:
    print('**REMINDER: THIS IS A TEST RUN, AND ANY RESULTS ARE NOT COMPLETE!**\n')