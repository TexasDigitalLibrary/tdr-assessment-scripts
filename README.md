# Assessment and reporting scripts for the Texas Data Repository

## Metadata
* *Version*: 0.0.1. (initial commit)
* *Released*: 2025/03/31
* *Author(s)*: Bryan Gee (UT Libraries, University of Texas at Austin; bryan.gee@austin.utexas.edu; ORCID: [0000-0003-4517-3290](https://orcid.org/0000-0003-4517-3290))
* *Contributor(s)*: None
* *License*: [GNU GPLv3](https://www.gnu.org/licenses/gpl-3.0.en.html)
* *README last updated*: 2025/03/31

## Table of Contents
1. [Purpose](#purpose)
2. [Organization & file list](#organization--file-list)
3. [Overview](#overview)
4. [Re-use](#re-use)

## Purpose

This repository includes a series of scripts that are designed for a variety of reporting and assessment purposes for the Texas Data Repository (TDR). They are intended for both institution-level and TDR-level analysis.

## Organization & file list
1. **dataverse-dspace-match.py**: This script is a preliminary development of a process to identify individuals who have published deposits in both an institutional DSpace repository and TDR as part of the potential COAR Notify pilot project. 

## Overview
1. **dataverse-dspace-match.py**: This script involves a relatively simple process of retrieving all deposits in TDR and a specified DSpace repository through the DataCite API using the common DOI prefix and a basic publisher filter. The script then creates an entry (row) for each author, de-duplicates it, and looks for exact matches. This script does not involve either the Dataverse or DSpace APIs; there are a few reasons for this, including relatively poor documentation for, and anticipated updates to, [DSpace's REST API](https://wiki.lyrasis.org/display/DSDOC5x/REST+API); the benefit of using a single API and metadata schema for two sources of information; and my own relative familiarity with the DataCite API (I have never had any reason to explore the DSpace API). This process thus relies on a DSpace repository minting DOIs for deposits, rather than just handles; this script will not work otherwise.

## Outputs
### dataverse-dspace-match
The prototype script for the COAR Notify project will return four outputs:
* ***date*_tdr-unique-authors.csv**: a dataframe with a list of all unique authors in TDR. Note that this de-duplicates on the author name, so authors with multiple deposits will have only one listed dataset. 
* ***date*_dspace-unique-authors-*cutoff date*-onward.csv**: a dataframe with a list of all unique authors in a DSpace instance. Note that this de-duplicates on the author name, so authors with multiple deposits will have only one listed dataset. The 'cutoff date' is specified in the script and can be adjusted.
* ***date*_dspace-into-tdr.csv**: a dataframe with the results of "left-merging" the Dspace dataframe into the TDR dataframe.
* ***date*_dspace-into-tdr_matched.csv**: a dataframe with the results of "left-merging" the Dspace dataframe into the TDR dataframe, restricted only to matches.

The code to export the full record of all deposits in each repository (***repository*-deposits-all.csv**) is included but is coded out at present. 

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