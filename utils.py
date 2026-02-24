import holidays
import math
import numpy as np
import os
import pandas as pd
import re
import requests
from datetime import datetime
from urllib.parse import urlparse, parse_qs

### API retrieval functions ###

# Retrieves single page of Dryad results
def retrieve_page_dryad(url, params):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'_embedded': {'stash:datasets': []}, 'total': {}}
# Retrieves all pages of Dryad results
def retrieve_dryad(url, params, page_start, per_page):
    all_data_dryad = []
    params = params.copy()
    params['page'] = page_start
    params['per_page'] = per_page

    data = retrieve_page_dryad(url, params)
    total_count = data.get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1

    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while True:
        print(f'Retrieving page {params["page"]} of {total_pages} from Dryad...\n')
        data = retrieve_page_dryad(url, params)

        if not data.get('_embedded'):
            print('No data found.')
            return all_data_dryad

        datasets = data['_embedded'].get('stash:datasets', [])
        all_data_dryad.extend(datasets)

        params['page'] += 1

        if not datasets:
            print('End of Dryad response.\n')
            break

    return all_data_dryad

# Retrieves single page of DataCite results
def retrieve_page_datacite(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': [], 'links': {}}
# Retrieves all pages of DataCite results
def retrieve_datacite(url, params, page_start, page_limit, per_page):
    all_data_datacite = []
    current_page = page_start

    data = retrieve_page_datacite(url, params)
    if not data['data']:
        print('No data found.')
        return all_data_datacite

    all_data_datacite.extend(data['data'])

    total_count = data.get('meta', {}).get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1

    current_url = data.get('links', {}).get('next', None)

    while current_url and current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from DataCite...\n')
        data = retrieve_page_datacite(current_url)
        if not data['data']:
            print('End of response.')
            break
        all_data_datacite.extend(data['data'])
        current_url = data.get('links', {}).get('next', None)

    return all_data_datacite
# Retrieves all pages of DataCite aggregate metadata
def retrieve_datacite_summary(url, params, publisher, affiliated, institution):
    all_resource_types = []
    all_licenses = []

    data = retrieve_page_datacite(url, params)
    if affiliated:
        print(f'Retrieving data for {publisher} for all deposits ({institution} only).\n')
    else:
        print(f'Retrieving data for {publisher} for all deposits.\n')

    if not data['meta']:
        print('No metadata found.')
        return all_resource_types, all_licenses

    resource_types = data['meta'].get('resourceTypes', [])
    licenses = data['meta'].get('licenses', [])
    
    for resource in resource_types:
        resource['publisher'] = publisher
    for license in licenses:
        license['publisher'] = publisher

    all_resource_types.extend(resource_types)
    all_licenses.extend(licenses)

    return all_resource_types, all_licenses

# Retrieves single page of Dataverse results
def retrieve_page_dataverse(url, params=None, headers=None):
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'data': {'items': [], 'total_count': 0}}
# Retrieves all pages of DataCite results
def retrieve_dataverse(url, params, headers, page_start, per_page, page_limit=None):
    all_data_dataverse = []
    params = params.copy()
    current_page = 0
    params['start'] = page_start
    params['per_page'] = per_page

    while True:
        data = retrieve_page_dataverse(url, params, headers)
        total_count = data['data']['total_count']
        total_pages = math.ceil(total_count / per_page) if per_page else 1
        print(f'Retrieving page {current_page} of {total_pages} pages...\n')

        if not data['data']:
            print('No data found.')
            break

        all_data_dataverse.extend(data['data']['items'])

        # Pagination logic
        current_page += 1
        params['start'] += per_page

        if params['start'] >= total_count:
            print('End of response.')
            break
        if page_limit and current_page >= page_limit:
            print('Reached page limit.')
            break

    return all_data_dataverse

# Retrieves single page of Zenodo results
def retrieve_page_zenodo(url, params=None):
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'hits': {'hits': [], 'total': {}}, 'links': {}}
# Retrieves page number in Zenodo query
def extract_page_number(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get('page', [None])[0]
# Retrieves all pages of Zenodo results
def retrieve_zenodo(url, params, page_start, page_limit, per_page):
    all_data_zenodo = []
    current_page = page_start
    params = params.copy()
    params['page'] = current_page
    params['size'] = per_page

    data = retrieve_page_zenodo(url, params)
    if not data['hits']['hits']:
        print('No data found.')
        return all_data_zenodo

    all_data_zenodo.extend(data['hits']['hits'])

    current_url = data.get('links', {}).get('self', None)
    total_count = data.get('hits', {}).get('total', 0)
    total_pages = math.ceil(total_count / per_page) if per_page else 1
    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while current_url and current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from Zenodo...\n')
        data = retrieve_page_zenodo(current_url)
        if not data['hits']['hits']:
            print('End of Zenodo response.\n')
            break

        all_data_zenodo.extend(data['hits']['hits'])
        current_url = data.get('links', {}).get('next', None)

    return all_data_zenodo

# Retrieves single page of OpenAlex results
def retrieve_page_openalex(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'results': [], 'meta': {}}
# Retrieves all pages of OpenAlex results
def retrieve_openalex(url, params, page_limit):
    all_data_openalex = []
    params = params.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    current_page = 0

    data = retrieve_page_openalex(url, params)
    if not data['results']:
        print('No data found.')
        return all_data_openalex

    all_data_openalex.extend(data['results'])

    total_count = data.get('meta', {}).get('count', 0)
    per_page = data.get('meta', {}).get('per_page', 1)
    total_pages = math.ceil(total_count / per_page) + 1

    print(f'Total: {total_count} entries over {total_pages} pages\n')

    while current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} of {total_pages} from OpenAlex...\n')
        data = retrieve_page_openalex(url, params)
        next_cursor = data.get('meta', {}).get('next_cursor', None)

        if next_cursor == previous_cursor:
            print('Cursor did not change. Ending loop to avoid infinite loop.')
            break

        if not data['results']:
            print('End of OpenAlex response.\n')
            break

        all_data_openalex.extend(data['results'])

        previous_cursor = next_cursor
        params['cursor'] = next_cursor

    return all_data_openalex

# Retrieves single page of Crossref results
def retrieve_page_crossref(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f'Error retrieving page: {e}')
        return {'message': {'items': [], 'total-results': {}}}
# Retrieves all pages of Crossref results
def retrieve_crossref(url, params, page_limit):
    all_data_crossref = []
    params = params.copy()
    params['cursor'] = '*'
    next_cursor = '*'
    previous_cursor = None
    current_page = 1

    data = retrieve_page_crossref(url, params)
    if not data['message']['items']:
        print('No data found.')
        return all_data_crossref

    all_data_crossref.extend(data['message']['items'])

    while current_page < page_limit:
        current_page += 1
        print(f'Retrieving page {current_page} from CrossRef...\n')
        data = retrieve_page_crossref(url, params)
        next_cursor = data.get('message', {}).get('next-cursor', None)

        if not data['message']['items']:
            print('Finished retrieval.\n')
            break

        all_data_crossref.extend(data['message']['items'])

        previous_cursor = next_cursor
        params['cursor'] = next_cursor

    return all_data_crossref
# Retrieves results for specified journals in Crossref API
def retrieve_all_journals(url_template, journal_list, params_crossref_journal, page_limit_crossref, retrieve_crossref_func):
    all_data = []
    for journal_name, issn in journal_list.items():
        print(f'Retrieving data from {journal_name} (ISSN: {issn})')
        custom_url = url_template.format(issn=issn)
        params = params_crossref_journal.copy()
        params['filter'] += f',issn:{issn}'
        journal_data = retrieve_crossref_func(custom_url, params, page_limit_crossref)
        all_data.extend(journal_data)
    return all_data

## Retrieves many pages from many institutions
def retrieve_all_institutions(url, params_list, headers, page_start, per_page, page_limit = None):
    all_data = []

    for institution_name, params in params_list.items():
        # Reset k for each institution if needed (but is k still used?)
        all_data_tdr = retrieve_dataverse(url, params, headers, page_start, per_page, page_limit)
        for entry in all_data_tdr:
            entry['institution'] = institution_name 
            all_data.append(entry)

    return all_data

### Metadata cleaning / assessment functions ###

# Determines which author (first vs. last or both) is affiliated
def determine_affiliation(row, ut_variations):
    if row['first_author'] == row['last_author']:
        return 'single author'

    first_affiliated = any(variation in (row['first_affiliation'] or '') for variation in ut_variations)
    last_affiliated = any(variation in (row['last_affiliation'] or '') for variation in ut_variations)

    if first_affiliated and last_affiliated:
        return 'both lead and senior'
    elif first_affiliated and not last_affiliated:
        return 'only lead'
    elif last_affiliated and not first_affiliated:
        return 'only senior'
    else:
        return 'neither lead nor senior'

# Standard function to look for file with specified pattern in name in specified directory
def load_most_recent_file(outputs_dir, pattern):
    files = os.listdir(outputs_dir)
    files.sort(reverse=True)

    latest_file = None
    for file in files:
        if pattern in file:
            latest_file = file
            break

    if not latest_file:
        print(f"No file with '{pattern}' was found in the directory '{outputs_dir}'.")
        return None
    else:
        file_path = os.path.join(outputs_dir, latest_file)
        df = pd.read_csv(file_path)
        print(f"The most recent file '{latest_file}' has been loaded successfully.")
        return df

# Standard function to find internal shared drive that may be mapped to a different drive on other computers
def find_mapped_drive(directory):
    target = os.path.normpath(directory)
    for drive_letter in 'DEFGHIJKLMNOPQRSTUVWXYZ':  
        drive_path = f"{drive_letter}:\\"
        if os.path.exists(drive_path):
            potential_path = os.path.join(drive_path, target)
            if os.path.exists(potential_path):
                return potential_path
    return None

# Checks if hypothetical DOI exists (for PLOS SI workflow)
def check_link(doi):
    url = f'https://doi.org/{doi}'
    response = requests.head(url, allow_redirects=True)
    return response.status_code == 200

# Validate formatting of ORCID and ROR in metadata
def is_valid_orcid(orcid):
    # ORCID must be a URL not just the string
    return isinstance(orcid, str) and orcid.startswith("https://orcid.org/")
def is_valid_ror(ror):
    return isinstance(ror, str) and ror.startswith("https://ror.org/")

# Retrieves day of week from ISO date
def get_day_of_week(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%A") 
    except Exception:
        return None
# Checks if U.S. holiday
us_holidays = holidays.US()
def is_us_federal_holiday(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt in us_holidays
    except Exception:
        return False
# Checks whether date is within prescribed ranges
def is_in_break(date, ranges):
    for start, end in ranges:
        start_dt = pd.to_datetime(start)
        end_dt = pd.to_datetime(end)
        if start_dt <= date <= end_dt:
            return True
    return False

# Return only the highest value for the version number in a Dataverse retrieval
def extract_max_version(val):
    if isinstance(val, str):
        try:
            versions = [float(v.strip()) for v in val.split(';')]
            return max(versions)
        except ValueError:
            return val  # In case of unexpected format
    return val

# Counts descriptive words in text field
def count_words(text, nondescriptive_words):
    if not isinstance(text, str) or text.strip() == '':
        return 0, 0
    words = text.split()
    total_words = len(words)
    descriptive_count = sum(1 for word in words if word.lower() not in nondescriptive_words)
    return total_words, descriptive_count

# Analyzes keywords for malformatted entry and counts descriptive words in keywords field
def analyze_keywords(keywords_list, nondescriptive_words):
    # Flag if any keyword entry contains ',' or ';'
    has_problematic = any(',' in kw or ';' in kw for kw in keywords_list)
    total_words = len(keywords_list)

    # Descriptive count
    descriptive_count = sum(
        1 for kw in keywords_list
        if kw.strip() and kw.lower() not in nondescriptive_words
    )

    return {
        'malformatted_keywords': has_problematic,
        'total_keywords': total_words,
        'descriptive_keywords': descriptive_count
    }

## Adjust for specific phrases in descriptive word counting
def adjust_descriptive_count_title(row):
    title = row.get('title_reformatted')
    desc_count = row.get('descriptive_word_count_title', 0)
    
    if not isinstance(title, str):
        return desc_count
    if not isinstance(desc_count, int):
        try:
            desc_count = int(desc_count)
        except (ValueError, TypeError):
            desc_count = 0

    keywords = ['supplemental material','supplementary material','supplementary materials','supplemental materials','supporting materials']

    if any(keyword in title.lower() for keyword in keywords):
        return max(0, desc_count - 1)
    return desc_count
def adjust_descriptive_count_description(row):
    title = row.get('description')
    desc_count = row.get('descriptive_word_count_description', 0)
    
    if not isinstance(title, str):
        return desc_count
    if not isinstance(desc_count, int):
        try:
            desc_count = int(desc_count)
        except (ValueError, TypeError):
            desc_count = 0

    keywords = ['supplemental material','supplementary material','supplementary materials','supplemental materials','supporting materials']

    if any(keyword in title.lower() for keyword in keywords):
        return max(0, desc_count - 1)
    return desc_count


def safe_split(val):
    # If it's already a list/array, return as is
    if isinstance(val, list) or isinstance(val, np.ndarray):
        return val
    # If it's NaN, return [np.nan]
    if pd.isna(val):
        return [np.nan]
    # Otherwise, split the string
    return str(val).split('; ')

# Function to standardize registrant name in RDS event data
def standardize_registrant_name(row):
    # Strip whitespace and check for non-empty values
    first = str(row['first']).strip() if pd.notna(row['first']) else ''
    last = str(row['last']).strip() if pd.notna(row['last']) else ''
    if first and last:
        # Combine and convert to title case
        return f"{first} {last}".title()
    else:
        # Use 'name' if first or last is missing/empty, convert to title case
        name = str(row['name']).strip() if pd.notna(row['name']) else ''
        return name.title() if name else pd.NA
    
# Function to assign size bins (file or dataset level) for TDR datasets
def assign_size_bins(df, column='file_size', new_column='file_size_bin'):
    df=df.copy()
    bins = [
        (1, 1 * 1024, '0-10 kB'), #technically not 0, 'Empty' is separate
        (1 * 1024, 1 * 1024 * 1024, '10 kB-1 MB'),
        (1 * 1024 * 1024, 100 * 1024 * 1024, '1-100 MB'),
        (100 * 1024 * 1024, 1 * 1024 * 1024 * 1024, '100 MB-1 GB'),
        (1 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024, '1-10 GB'),
        (10 * 1024 * 1024 * 1024, 15 * 1024 * 1024 * 1024, '10-15 GB'),
        (15 * 1024 * 1024 * 1024, 20 * 1024 * 1024 * 1024, '15-20 GB'),
        (20 * 1024 * 1024 * 1024, 25 * 1024 * 1024 * 1024, '20-25 GB'),
        (25 * 1024 * 1024 * 1024, 30 * 1024 * 1024 * 1024, '25-30 GB'),
        (30 * 1024 * 1024 * 1024, 40 * 1024 * 1024 * 1024, '30-40 GB'),
        (40 * 1024 * 1024 * 1024, 50 * 1024 * 1024 * 1024, '40-50 GB'),
    ]

    #default set to empty
    df[new_column] = 'Empty'
    for lower, upper, label in bins:
        df.loc[(df[column] > lower) & (df[column] <= upper), new_column] = label
    #maximum bin
    df.loc[df[column] > 50 * 1024 * 1024 * 1024, new_column] = '>50 GB'

    return df



## Sensitive data screening
   
def flag_sensitive_terms(df, terms, columns):
    # Compile regex patterns for each term (case-insensitive, partial match)
    patterns = [re.compile(rf'\b{re.escape(term)}\w*\b', re.IGNORECASE) for term in terms]

    flags_list = []
    sources_list = []

    for idx, row in df.iterrows():
        matched_terms = []
        matched_sources = []

        for col in columns:
            text = row[col]
            if pd.isnull(text) or text == '':
                continue
            col_matches = []
            for pattern in patterns:
                # Find all matches for each pattern
                matches = pattern.findall(text)
                if matches:
                    col_matches.extend(matches)
            if col_matches:
                matched_terms.extend(col_matches)
                matched_sources.append(col)

        # Remove duplicates, but keep all variants found in original order
        flags = '; '.join(sorted(set(matched_terms), key=matched_terms.index))
        sources = '; '.join(sorted(set(matched_sources), key=matched_sources.index))
        flags_list.append(flags)
        sources_list.append(sources)

    df['metadata_flags'] = flags_list
    df['metadata_source'] = sources_list
    return df

def filter_sensitive_datasets(df):
    # Criterion 1: flags is not empty
    flagged = df['metadata_flags'].str.strip() != ''
    
    # Criterion 2: Restricted is True or contains 'True' (in a 'True;False' string)
    restricted = (
        (df['restricted'] == True) | 
        (df['restricted'].astype(str).str.contains('True', case=False))
    )
    
    # Criterion 3: mimetype contains audio/, video/, or image/
    mimetype_sensitive = df['original_mime_type'].astype(str).str.contains(r'(audio/|video/)', case=False, regex=True)
    
    # Criterion 4: license != CC0 1.0
    not_cc0 = df['license'] != 'CC0 1.0'

    # If any one of these four occur, return it
    subset = df[flagged | restricted | mimetype_sensitive | not_cc0]
    return subset

def add_final_source_column(df):
    final_sources = []

    for idx, row in df.iterrows():
        sources = []
        # Criterion 1: flagged
        if row.get('metadata_flags', '').strip() != '':
            sources.append('metadata')
        # Criterion 2: restricted
        restricted_val = row.get('restricted', '')
        if restricted_val is True or ('True' in str(restricted_val)):
            sources.append('restricted')
        # Criterion 3: mimetype
        mimetype_val = str(row.get('original_mime_type', ''))
        if any(mt in mimetype_val.lower() for mt in ['audio/', 'video/']):
            sources.append('file format')
        # Criterion 4: license
        license_val = row.get('license', '')
        if license_val != 'CC0 1.0':
            sources.append('license')
        final_sources.append('; '.join(sources))

    df['flags_source'] = final_sources
    return df

### Logging functions ###
# Function to indent text in summary text file
def single_tab(text, indent="   "):
    return '\n'.join([indent + line for line in text.split('\n')])
def double_tab(text):
    indent = "\t\t"
    return '\n'.join([indent + line for line in text.split('\n')])