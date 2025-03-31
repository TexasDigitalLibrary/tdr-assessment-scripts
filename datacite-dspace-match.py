import json
import math
import numpy as np
import os
import pandas as pd
import re
import requests
from datetime import datetime

#setting timestamp at start of script to calculate run time
startTime = datetime.now() 
#creating variable with current date for appending to filenames
todayDate = datetime.now().strftime("%Y%m%d") 

#toggle to run with subset of total results
test = False

#year cutoff for DSpace
##cutoff is semi-arbitrary and intended to omit historical documents uploaded to a DSpace
cutoff = 2012

#read in config file
with open('config.json', 'r') as file:
    config = json.load(file)

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

#using prefix for TDR (works for all institutions)
url_datacite_tdr = "https://api.datacite.org/dois?prefix=10.18738"
#using prefix for Texas ScholarWorks (UT Austin-specific DSpace)
url_datacite_dspace = "https://api.datacite.org/dois?prefix=10.26153"

page_limit_datacite = config['VARIABLES']['PAGE_LIMITS']['datacite_test'] if test else config['VARIABLES']['PAGE_LIMITS']['datacite_prod']
page_limit_dspace = config['VARIABLES']['PAGE_LIMITS']['dspace_test'] if test else config['VARIABLES']['PAGE_LIMITS']['dspace_prod']
page_limit_tdr = config['VARIABLES']['PAGE_LIMITS']['tdr_test'] if test else config['VARIABLES']['PAGE_LIMITS']['tdr_prod']
page_size = config['VARIABLES']['PAGE_SIZES']['datacite_test'] if test else config['VARIABLES']['PAGE_SIZES']['datacite_prod']
page_start_datacite = config['VARIABLES']['PAGE_STARTS']['datacite']

#even if you are not using an affiliation filter/query, you need to keep 'affiliation': 'true' in the params to return affiliation metadata
params_datacite_dspace = {
    'affiliation': 'true',
    'query': 'publisher:(The University of Texas at Austin)',
    'page[size]': page_size,
    'page[cursor]': 1
}
params_datacite_tdr = {
    'affiliation': 'true',
    'query': 'publisher:(Texas Data Repository)',
    'page[size]': page_size,
    'page[cursor]': 1
}

##retrieves single page of results
def retrieve_page_datacite(url, params=None):
    """Fetch a single page of results with cursor-based pagination."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return {'data': [], 'links': {}}
    
##recursively retrieves pages
def retrieve_all_data_datacite(url, params, page_limit):
    """Fetch all pages of data using cursor-based pagination."""
    all_data_datacite = []
    data = retrieve_page_datacite(url, params)
    
    if not data['data']:
        print("No data found.")
        return all_data_datacite

    all_data_datacite.extend(data['data'])

    current_url = data.get('links', {}).get('next', None)
    total_count = data.get('meta', {}).get('total', None)
    total_pages = math.ceil(total_count/page_size)
    
    current_url = data.get('links', {}).get('next', None)

    i = 0 #page counter initialing
    while current_url and i < page_limit:
        i+=1
        print(f"Retrieving page {i} of {total_pages} from DataCite...")
        print()
        data = retrieve_page_datacite(current_url)
        
        if not data['data']:
            print("End of response.")
            break
        
        all_data_datacite.extend(data['data'])
        
        current_url = data.get('links', {}).get('next', None)
    
    return all_data_datacite

print("Starting DSpace retrieval\n")
dspace_records = retrieve_all_data_datacite(url_datacite_dspace, params_datacite_dspace, page_limit_dspace)
print(f"Number of DSpace deposits retrieved by DataCite API: {len(dspace_records)}\n")
print("Starting DSpace subsetting\n")

dspace_select_records = []
for item in dspace_records:
    attributes = item.get('attributes', {})
    doi = attributes.get('doi', None)
    publisher = attributes.get('publisher', "")
    publisher_year = attributes.get('publicationYear', "")
    title=attributes.get('titles', [{}])[0].get('title',"")
    creators = attributes.get('creators', [{}])
    filtered_creators = [creator for creator in creators if not creator.get('name', "").startswith('0000-')]
    creator_names = [creator.get('name', "") for creator in filtered_creators] 
    dspace_select_records.append({
        'doi': doi,
        'publisher': publisher,
        'publicationYear': publisher_year,
        'title': title,
        'authors': creator_names,
    })

print("Processing DSpace data\n")
df_dspace_initial = pd.json_normalize(dspace_select_records)
# df_dspace_initial.to_csv(f"outputs/{todayDate}_dspace-deposits-all.csv", index=False)

df_dspace_initial_restricted = df_dspace_initial[df_dspace_initial['publicationYear'] >= cutoff]
print(f"Number of DSpace deposits retained ({cutoff} onward): {len(df_dspace_initial_restricted)}\n")
# df_dspace_initial_restricted.to_csv(f"outputs/{todayDate}_dspace-deposits-{cutoff}-onward.csv", index=False)
df_dspace_initial_restricted_authors = df_dspace_initial_restricted.explode('authors')
df_dspace_initial_restricted_authors_dedup = df_dspace_initial_restricted_authors.drop_duplicates(subset='authors', keep="first")
df_dspace_initial_restricted_authors_dedup = df_dspace_initial_restricted_authors_dedup.rename(columns={"authors": "author"})
# df_dspace_initial_restricted_authors_dedup.to_csv(f"outputs/{todayDate}_dspace-unique-authors-{cutoff}-onward.csv", index=False)
print(f"Number of unique DSpace authors found ({cutoff} onward): {len(df_dspace_initial_restricted_authors_dedup)}\n")

print("Starting TDR retrieval\n")
tdr_records = retrieve_all_data_datacite(url_datacite_tdr, params_datacite_tdr, page_limit_tdr)

print(f"Number of TDR deposits retrieved by DataCite API: {len(tdr_records)}\n")
print("Starting TDR subsetting\n")

tdr_select_records = [] 
for item in tdr_records:
    attributes = item.get('attributes', {})
    doi = attributes.get('doi', None)
    publisher = attributes.get('publisher', "")
    pubYear = attributes.get('publicationYear', "")
    status = attributes.get('versionState', "")
    title = attributes.get('titles', [{}])[0].get('title',"")
    creators = attributes.get('creators', [{}])
    creator_names = [creator.get('name', "") for creator in creators]
    creator_affiliations = [[aff['name'] for aff in creator.get('affiliation', [{"name": ""}])] if creator.get('affiliation') else [None] for creator in creators]
    tdr_select_records.append({
        'doi': doi,
        'status': status,
        'publisher': publisher,
        'publicationYear': pubYear,
        'title': title,
        'authors': creator_names,
        'affiliations': creator_affiliations
    })


print("Processing TDR data\n")
df_tdr_initial = pd.json_normalize(tdr_select_records)
df_tdr_initial['doi'] = df_tdr_initial['doi'].str.replace('doi:', '')
# df_tdr_initial.to_csv(f"outputs/{todayDate}_tdr-deposits-all.csv", index=False)

#reformatting author names
##looks for text in parentheses in author field (malformatted affiliation entry), creates new column, blank if no such text
df_tdr_initial['affiliation_inferred'] = df_tdr_initial['authors'].apply(lambda x: re.search(r'\((.*?)\)', x[0]).group(1) if re.search(r'\((.*?)\)', x[0]) else '')
##removes any text in parentheses (and parentheses) and leading/trailing whitespace
df_tdr_initial['authors'] = df_tdr_initial['authors'].apply(lambda x: [re.sub(r'\s*\([^)]*\)', '', name).strip() for name in x])
##creates semi-colon-delimited list of authors
df_tdr_initial['authors'] = df_tdr_initial['authors'].apply(lambda x: '; '.join(x))

#reformatting affiliations
df_tdr_initial['affiliations'] = df_tdr_initial['affiliations'].apply(lambda x: '; '.join(map(str, x)) if x is not None else '')
df_tdr_initial['affiliations'] = df_tdr_initial['affiliations'].str.replace(r"[\[\]']", "", regex=True)

df_tdr_initial['authors'] = df_tdr_initial['authors'].str.split('; ')
df_tdr_initial['affiliations'] = df_tdr_initial['affiliations'].str.split('; ')
df_tdr_initial['affiliations'] = df_tdr_initial.apply(lambda row: row['affiliations'] if len(row['authors']) == len(row['affiliations']) else [None]*len(row['authors']), axis=1)
df_tdr_initial_authors = df_tdr_initial.explode(['authors', 'affiliations'])

#replace blank affiliation with scraped/inferred affiliation from author name field
df_tdr_initial_authors['affiliations'] = df_tdr_initial_authors.apply(lambda row: row['affiliation_inferred'] if not row['affiliations'] else row['affiliations'], axis=1)
# df_tdr_initial_authors = df_tdr_initial_authors.drop(columns=['author_affiliation', 'authors', 'affiliations', 'affiliation_inferred'])
df_tdr_initial_authors = df_tdr_initial_authors.rename(columns={"authors": "author"})

df_tdr_initial_authors_dedup = df_tdr_initial_authors.drop_duplicates(subset='author', keep="first")
df_tdr_initial_authors_dedup.to_csv(f"outputs/{todayDate}_tdr-unique-authors.csv", index=False)
print(f"Number of unique TDR authors found (all): {len(df_tdr_initial_authors_dedup)}\n")

dspace_tdr = pd.merge(df_tdr_initial_authors_dedup, df_dspace_initial_restricted_authors_dedup, on="author", how="left")
dspace_tdr['matched'] = np.where(dspace_tdr['doi_y'].isnull(), 'Not matched', 'Matched')
dspace_tdr.to_csv(f"outputs/{todayDate}_dspace-into-tdr.csv", index=False)
matched = dspace_tdr['matched'].value_counts()
print(matched)
dspace_tdr_matched = dspace_tdr[dspace_tdr['matched'] == "Matched"]
dspace_tdr_matched.to_csv(f"outputs/{todayDate}_dspace-into-tdr_matched.csv", index=False)
unique_datasets = dspace_tdr_matched['doi_x'].nunique()
unique_deposits = dspace_tdr_matched['doi_y'].nunique()
print(f"Unique TDR datasets with author matches: {unique_datasets}\n")
print(f"Unique DSpace deposits with author matches: {unique_deposits}\n")

print(f"Time to run: {datetime.now() - startTime}")
print("Done.")