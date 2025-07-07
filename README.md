# Assessment and reporting scripts for the Texas Data Repository

## Metadata
* *Version*: 0.0.3.
* *Released*: 2025/07/07
* *Author(s)*: Bryan Gee (UT Libraries, University of Texas at Austin; bryan.gee@austin.utexas.edu; ORCID: [0000-0003-4517-3290](https://orcid.org/0000-0003-4517-3290))
* *Contributor(s)*: None
* *License*: [GNU GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html)
* *README last updated*: 2025/07/07

## Table of Contents
1. [Purpose](#purpose)
2. [Organization & file list](#organization--file-list)
3. [Overview](#overview)
4. [Re-use](#re-use)

## Purpose

This repository includes a series of scripts that are designed for a variety of reporting and assessment purposes for the Texas Data Repository (TDR). They are intended for both institution-level and TDR-level analysis.

## Organization & file list
1. **dataverse-dspace-match.py**: This script is a preliminary development of a process to identify individuals who have published deposits in both an institutional DSpace repository and TDR as part of the potential COAR Notify pilot project. 
2. **dataverse-file-assessment.py**: This script is used to retrieve file-level information in order to calculate the total deposit size per year for either a single institution or all TDR institutions and to gain insights into file format trends.

## Overview
1. **dataverse-dspace-match.py**: This script involves a relatively simple process of retrieving all deposits in TDR and a specified DSpace repository through the DataCite API using the common DOI prefix and a basic publisher filter. The script then creates an entry (row) for each author, de-duplicates it, and looks for exact matches. This script does not involve either the Dataverse or DSpace APIs; there are a few reasons for this, including relatively poor documentation for, and anticipated updates to, [DSpace's REST API](https://wiki.lyrasis.org/display/DSDOC5x/REST+API); the benefit of using a single API and metadata schema for two sources of information; and my own relative familiarity with the DataCite API (I have never had any reason to explore the DSpace API). This process thus relies on a DSpace repository minting DOIs for deposits, rather than just handles; this script will not work otherwise.
2. **dataverse-file-assessment.py**: This script makes exclusive use of the Dataverse API and therefore requires an API token to run. It involves a multi-step process that repeatedly hits the API, which means it will likely be heavily impacted by future rate limiting. The script first queries the [Search API](https://guides.dataverse.org/en/latest/api/search.html) for all records in a given collection (institution-specific) or will look across all TDR institutions (in theory, you could pick some but not all, but I can't think of a use case for that off the top of my head). Regardless of the scope, a non-superuser will pick up deposits in *Draft* and *Deaccessioned* status for their institution but not those of others - a superuser should get everything. If a dataset was previously published and is in *DRAFT* status, it will be double-listed, once for each status. The dataframe is de-duplicated, removing the draft version for datasets that were previously published (so any retained object with *DRAFT* has never been published), and then each DOI is fed into the [Native API](https://guides.dataverse.org/en/latest/api/native-api.html). This will retrieve metadata not available through the Search API for the most recent version (either *Published* or *Draft* and regardless of previous publication or lack thereof), such as total deposit size and information on the individual files (e.g., storageIdentifier, individual size, mimeType, date of creation). *Deaccessioned* datasets are removed at this point, as they do not record any storage size (they probably do for a superuser since the files are retained in the system, so their storage allocation is not zero). The retrieval of file-level information allows for reliable calculation of total storage size based on when the file was first ingested, since it starts imposing a cost at that point, and the counting of file formats. File format counting is NOT the total number of files in the system with a given extension because some disciplines can generate thousands of nearly identical files for one study; instead, this script counts how many unique datasets each file format occurs in, which is considered more reliable for understanding the prevalence of different formats.
3. **tdr-affiliation-ror-matching.csv**: This is a file developed for several other workflows that may be useful in the dataset/file assessment workflow as well. It provides a mapping of every unique listed affiliation in the UT Austin dataverse (as of 2025/07/07) to a ROR identifier (if one exists).

## Outputs
### dataverse-dspace-match
The prototype script for the COAR Notify project will return four outputs:
* ***date*_tdr-unique-authors.csv**: a dataframe with a list of all unique authors in TDR. Note that this de-duplicates on the author name, so authors with multiple deposits will have only one listed dataset. 
* ***date*_dspace-unique-authors-*cutoff date*-onward.csv**: a dataframe with a list of all unique authors in a DSpace instance. Note that this de-duplicates on the author name, so authors with multiple deposits will have only one listed dataset. The 'cutoff date' is specified in the script and can be adjusted.
* ***date*_dspace-into-tdr.csv**: a dataframe with the results of "left-merging" the DSpace dataframe into the TDR dataframe.
* ***date*_dspace-into-tdr_matched.csv**: a dataframe with the results of "left-merging" the Dspace dataframe into the TDR dataframe, restricted only to matches.

The code to export the full record of all deposits in each repository (***repository*-deposits-all.csv**) is included but is coded out at present. 

### dataverse-file-assessment
This script will return eight outputs:
* ***date*_*institution*_all-deposits.csv**: a dataframe with an entry for every dataset (including deaccessioned and never-published drafts) retrieved from the search process. For datasets that were previously published and are now in draft, multiple entries are recorded (default Search API behaviour). 
* ***date*_*institution*_all-deposits-deduplicated.csv**: the same but with only one record for each DOI, retaining the 'PUBLISHED' version over the 'DRAFT' version for datasets with multiple entries.
* ***date*_*institution*_dual-status-datasets.csv**: a dataframe with entries for all datasets that return multiple entries from the Search API (previously published, now in 'DRAFT')
* ***date*_*institution*_all-deposits-deduplicated_expanded-metadata.csv**: a dataframe with an entry for each file retrieved from the search process with file- and dataset-level metadata. Note that is is only through the Search and Native APIs.
* ***date*_*institution*_all-files-deduplicated.csv**: a dataframe with an entry for each file retrieved from the search process. Note that this is through the Search, Native, and Version APIs.
* ***date*_*institution*_all-authors.csv**: a dataframe with an entry for each author associated with at least one dataset. Authors are only deduplicated on a combination of DOI, name, affiliation, and current version state of the dataset, so many authors will have multiple entries.
* ***date*_*institution*_unique-format-summary.csv**: a dataframe with a summary of the number of unique datasets in which each file format occurs.
* ***date*_*institution*_annual-size-summary.csv**: a dataframe with a summary of the total file size of files created in a given year. Which date is used (e.g., 'publication date' versus 'creation date' could be modified).

## Re-use
These scripts can be freely re-used, re-distributed, and modified in line with the associated [GNU GPLv3 license](https://www.gnu.org/licenses/gpl-3.0.en.html). If a re-user is only seeking to replicate a UT-Austin-specific output or to retrieve an equivalent output for a different institution, the script will require very little modification - essentially only the defining of affiliation parameters will be necessary. A superuser could have greater functionality in some instances, but superuser-specific functionality has largely not been developed because I have no way to test it.

### Disclaimer
This workflow is, and likely will always be, perpetually under development. Reusers should be cognizant of these limitations in determining how data gained from this workflow may inform decision-making. The creator(s) and contributor(s) of this repository and any entities to which they are affiliated are not responsible for any decisions, policies, or other actions that are made on the basis of obtained data.

### Config file
API keys and numerical API query parameters (e.g., records per page, page limit) are defined in a *config.json* file. The file included in this repository called *config-template.json* should be populated with API keys and any other user/institution-specific information and renamed. 

### Third-party API access
Users will need to create accounts for [Dataverse](https://guides.dataverse.org/en/latest/api/auth.html) in order to obtain personalized API keys, add those to the *config-template.json* file, and rename it as *config.json*. DataCite does not require an API key for standard access. 

### Test environment
A Boolean variable called *test*, located immediately after the importing of packages, can be used to create a 'test environment.' If this setting is set to TRUE, the script is set to only retrieve a handful of pages of the full response. 

### Rate limiting
In the present configuration, any rate limiting is unlikely to affect the workflows or require modification because of how queries are not targeting specific DOIs (i.e. a few requests return many records). However, potential/planned expansion may necessitate the use of targeted single-object retrieval, and users should be aware that many public APIs impose some kind of rate limiting (e.g., [DataCite](https://support.datacite.org/docs/is-there-a-rate-limit-for-making-requests-against-the-datacite-apis)). 