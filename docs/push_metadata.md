# Push Metadata to MIDRC BIH - SOP

Last updated: Nov 21, 2025
Author: Chris Meyer, PhD (cgmeyer@uchicago.edu)
Assistant Director of User Services and Outreach
Center for Translational Data Science at the University of Chicago

---

## Purpose / Overview

The MIDRC BIH aims to aggregate imaging discovery metadata from platforms containing high-value datasets in the medical imaging biomedical data fabric in order to help make these data more discoverable / FAIR. 


Ideally, data platforms will provide a metadata API, which MIDRC BIH can use to index discovery metadata and researchers can use to programmatically search and access selected imaging data. However, in cases where providing an API is impossible, we offer this simple solution for sharing discovery metadata with the MIDRC BIH to platforms with no API that would nevertheless like to make their data discoverable.


Note: Data uploaders from your organization must be individually authorized via their preferred login email(s). We ask that you provide us with the names and emails of your data uploaders. Currently, login uses Google IdP; let us know if this doesn’t work, and we can provide other login options. Please, do not share credentials/accounts.


Spreadsheet files containing discovery metadata can be uploaded to the MIDRC BIH staging environment using the mechanism described in this SOP. Note: this is simply a secure mechanism for sharing the metadata with the Gen3 team, after which Gen3 will subsequently process, ingest, and QC the metadata in the staging environment for your review before it’s made public on the production website. 


The metadata fields requested in any structured data spreadsheets you provide are listed in this doc as well. These fields can be provided in a single table, ideally at the imaging series level (each row in the table representing an imaging series), but we can also accept metadata tabulated at the imaging study level if you do not have metadata at that granular level. Note: most of these fields are optional, but the more you can share, the better users will be able to find your dataset(s). 


This is the standard method for secure file transfer using the existing Gen3 infrastructure, but we’re also open to other file transfer methods if this is difficult or inconvenient for you.


## File Upload Procedure Overview

1. Use our ["gen3-client” command-line tool](https://github.com/uc-cdis/cdis-data-client) to upload spreadsheets of imaging series and/or imaging study discovery metadata containing the fields listed below to the MIDRC BIH [staging environment](https://bihstaging.data-commons.org/).
2. Follow steps 1-3 to accomplish the upload(s) following the general gen3-client documentation [here](https://gen3.org/resources/user/gen3-client/).
3. Notify the Gen3 team via email at gen3-midrc@lists.uchicago.edu upon successful upload(s). If possible, please let us know the “GUID” of the file(s) you upload. The client will provide you with the data GUID(s) for the file(s), which look like a UUID with a “dg” prefix, for example: `dg.MD1R/00001e99-cdd5-43d1-871a-09b6f9df5dad`. You can simply copy and paste the command-line output of running the upload command, which will contain the GUID(s).


## File Upload Step-by-step Instructions


1. Create an API key containing your user credentials to use with the client: 
    1. Log into [MIDRC BIH Staging](https://bihstaging.data-commons.org/portal/login). Note: contact midrc-support@gen3.org in order to be granted file upload privileges. 
    2. Navigate to the [Profile page](https://bihstaging.data-commons.org/portal/identity).
    3. Click on the button “Create API Key” and then “Download JSON” to save your credentials.json file somewhere safe. Note: this file is essentially your personal password to the data commons, so please don’t share it or save it in a shared location. 
2. Download the [latest release](https://github.com/uc-cdis/cdis-data-client/releases) of the gen3-client command-line tool for your operating system.
    1. Open a terminal window and enter `gen3-client` at the command prompt to verify that it installed successfully. You should see the help message displayed. 
    2. Note: this is a command-line tool. You can run it from the command prompt and should not try to open it by double-clicking the executable file.
3. Configure a BIH Staging profile with the client.
    1. run the command `gen3-client configure` alone to view the help message):
    ```
    gen3-client configure --profile=bihstaging --cred=~/Downloads/bih-staging-credentials.json --apiendpoint=https://bihstaging.data-commons.org
    ```
    2. You should get a message that the profile was successfully configured: `Profile 'bihstaging' has been configured successfully.`
4. Upload the files. 
    1. Run the command (enter `gen3-client upload` to see the help message):
    ```
    gen3-client upload --profile=bihstaging --upload-path=test.tsv
    ```
    2. You should get a message that the files are successfully uploaded to specific GUIDs.
    3. Copy the upload success message when finished uploading all files and send it to the Gen3 team at `gen3-midrc@lists.uchicago.edu` and `midrc-support@gen3.org`.


# Metadata Headers Requested for Indexing Imaging Data in MIDRC BIH

## Required fields:
---
* Study ID (unique ID for imaging studies; ideally the DICOM “StudyInstanceUID”)
* Series ID (if providing series-level metadata: unique ID for all imaging series; ideally the DICOM “SeriesInstanceUID”)
* Patient ID (index for subjects: ideally the DICOM “PatientID”)
* Collection ID (unique ID of the overall dataset)
* commons_long_name (host platform long name; e.g., ”ACR data analysis & research toolkit”)
* commons_name (host platform abbreviated name; e.g., “ACRdart”)
* data_contributor (host platform contributing the metadata / hosting the files; e.g., “ACRdart”)
* license (data usage license or URL to data use agreement; e.g., “CC BY-NC 3.0”)
* Modality (modality of the images)

## Preferred fields:
---
* data_url_doi (a URL or a DOI where users can go to find the data)
* StudyDescription (text description of the imaging study)
* SeriesDescription (text description of the imaging series)
* PatientAge (ideally an integer, e.g., “40” not “040Y” and the like)
* PatientSex (ideally in format “Female” / “Male”)
* EthnicGroup (either the value of the DICOM tag or one of Hispanic/Non-Hispanic or Latino)
* BodyPartExamined (DICOM tag)
* Manufacturer (manufacturer of the imaging equipment)
* disease_type (text name of the disease being studied, if applicable; e.g., “Lung Cancer”)
* primary_site (site of disease, if applicable; e.g., “Lung”)

## Other optional fields:
---
* Subject Race (information on the subject’s race)
* object_ids (Persistent Unique Identifier (PID) of image files; e.g., a data GUID or UUID, etc.)
* dicom_viewer_url (URL users can follow to view images in a DICOM viewer or other SW)
