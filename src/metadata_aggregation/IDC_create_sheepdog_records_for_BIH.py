######################## 
import idc_index
from google.cloud import bigquery
import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 100)
import sys, os, subprocess
import glob, copy, time
import json
from unidecode import unidecode
from pathlib import Path
home= str(Path.home())
import datetime
now = datetime.datetime.now()
date = "{}-{}-{}-{}.{}.{}".format(now.year, now.month, now.day, now.hour, now.minute, now.second)
today = datetime.datetime.today().strftime('%Y%m%d')

import gen3
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.query import Gen3Query

git_dir=f'{home}/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion
subprocess.run(["python3", f"{git_dir}{sdk_dir}/expansion/expansion.py"], capture_output=True, text=True)

######################## BIH
# api = 'https://imaging-hub.data-commons.org'
# cred = f'{home}/Downloads/bih-credentials.json'
# auth = Gen3Auth(api, refresh_file=cred)
# sub = Gen3Submission(api, auth)
# query = Gen3Query(auth)
# index = Gen3Index(auth)
# exp = Gen3Expansion(api,auth,sub)

######################## BIH
bsapi = 'https://bihstaging.data-commons.org'
bscred = f'{home}/Downloads/bih-staging-credentials.json'
bsauth = Gen3Auth(bsapi, refresh_file=bscred)
bssub = Gen3Submission(bsapi, bsauth)
bsquery = Gen3Query(bsauth)
bsindex = Gen3Index(bsauth)
bsexp = Gen3Expansion(bsapi,bsauth,bssub)
ipids = sorted(bsexp.get_project_ids(node='program',name='IDC'))

idir = (f"{home}/Documents/Notes/BIH/IDC")
os.chdir(idir)
# let's set up some more folders
ingest_dir=f"{idir}/metadata/ingest_{today}"
Path(ingest_dir).mkdir(parents=True, exist_ok=True)
Path(f"{ingest_dir}/logs").mkdir(parents=True, exist_ok=True)


#########################################################################################################
# in terminal, authenticate using user credentials: https://cloud.google.com/bigquery/docs/authentication#client-libs
# !gcloud auth application-default login


"""
IDC Big Query Docs:
https://github.com/ImagingDataCommons/IDC-Tutorials/blob/master/notebooks/getting_started/part2_searching_basics.ipynb

Make sure you use the following index: "bigquery-public-data.idc_current" and
*not* "canceridc-data.idc_current"

dicom_all
bigquery-public-data.idc_current
Last modified
Nov 25, 2024, 11:21:56 AM UTC-6

TCIA said the "Source_DOI" that starts with "zenodo" should be IDC data.
The ones that have "10.7937" in them are TCIA DOIs, and "Zenodo" or anything else belongs to IDC.  
also said "datacite API" will show who is original dataset
"""

#########################################################################################################
## Query IDC (bigquery-public-data.idc_current)
from google.cloud import bigquery
# bq_client = bigquery.Client("cgmeyer-001")# BigQuery client is initialized with the ID of the project we specified in the beginning of the notebook! 
bq_client = bigquery.Client("egiger-001")# BigQuery client is initialized with the ID of the project we specified in the beginning of the notebook! 
#bq_client = bigquery.Client("bih-idc")# BigQuery client is initialized with the ID of the project we specified in the beginning of the notebook! 

# alphabetize these fields:
fields = [
    'BodyPartExamined',
    'EthnicGroup',
    'Manufacturer',
    'ManufacturerModelName',
    'Modality',
    'PatientAge',
    'PatientID',
    'PatientSex',
    'SeriesDescription',
    'SeriesInstanceUID',
    'StudyDescription',
    'StudyInstanceUID',
    'collection_id',
    'collection_name',
    'crdc_series_uuid',
    'crdc_study_uuid',
    'gcs_url',
    'license_long_name',
    'license_short_name',
    'license_url',
    'Source_DOI',
    'tcia_api_collection_id',
    'tcia_cancerType',
    'tcia_species',
    'tcia_tumorLocation']
for field in fields:
    print(field+',')

# the fields are now formatted so that they can be easily copy-and-pasted into our Selection Query
"""
BodyPartExamined,
collection_id,
collection_name,
crdc_series_uuid,
crdc_study_uuid,
EthnicGroup,
gcs_url,
license_long_name,
license_short_name,
license_url,
Manufacturer,
ManufacturerModelName,
Modality,
PatientAge,
PatientID,
PatientSex,
SeriesDescription,
SeriesInstanceUID,
Source_DOI,
StudyDescription,
StudyInstanceUID,
tcia_api_collection_id,
tcia_cancerType,
tcia_species,
tcia_tumorLocation,
"""

# Define Selection Query
selection_query = """
SELECT
  DISTINCT(SeriesInstanceUID),
    BodyPartExamined,
    collection_cancerType,
    collection_id,
    collection_name,
    collection_species,
    collection_tumorLocation,
    crdc_series_uuid,
    crdc_study_uuid,
    EthnicGroup,
    license_long_name,
    license_short_name,
    license_url,
    Manufacturer,
    ManufacturerModelName,
    Modality,
    PatientAge,
    PatientID,
    PatientSex,
    SeriesDescription,
    Source_DOI,
    StudyDescription,
    StudyInstanceUID,
FROM
  bigquery-public-data.idc_current.dicom_all
"""
# tcia_api_collection_id, # BadRequest: 400 Unrecognized name: tcia_api_collection_id at [24:5]
# tcia_cancerType,
# tcia_species,
# tcia_tumorLocation,
# gcs_url,

res = bq_client.query(selection_query)
# !pip install 'google-cloud-bigquery[pandas]'# in order to use res.result().to_dataframe()
df = res.result().to_dataframe()
df # 951562

dupes = df.loc[df.duplicated(subset='SeriesInstanceUID',keep='first')] # 3982

## Some duplicates just have one record with a missing element like body part examined; drop those
# Use df.isna().sum(axis=1) to count the amount of NaNs by row, and then GroupBy id and select the row with less NaNs using idxmin:

df = df.loc[df.isna().sum(axis=1).groupby(df.SeriesInstanceUID).idxmin(),:] # 947580 from 951562 for diff of 3982

results_filename = f"{ingest_dir}/IDC_series_bigquery_results_{date}.tsv"
df.to_csv(results_filename,sep='\t',index=False) # 947580

##########################################################################################
##########################################################################################
##########################################################################################
##########################################################################################
##########################################################################################
## Read in saved IDC series metadata
idir = (f"{home}/Documents/Notes/BIH/IDC")
os.chdir(idir)
os.listdir(ingest_dir) # check filename of bigquery results, recently saved in folder "ingest_{today}/"

ifile = "IDC_series_bigquery_results_2025-3-10-20.3.12.tsv"
# ifile=results_filename
adf = pd.read_csv(f"{ingest_dir}/{ifile}",sep='\t',header=0,dtype=str) # 947580
ipids = sorted(list(set(adf['collection_id'])))


## Harmonize IDC 'PatientSex' variables to MIDRC 'cases.sex':
# https://dicom.innolitics.com/ciods/mr-image/patient/00100040
# DICOM standards indicate only F,M, or O. 
# We will map 'U' to 'Unknown'
""" adf['PatientSex'].value_counts(dropna=False)
NaN     661717
F       172567
M       100361
O        12765
0000        85
U           77
6657         8
"""

adf.loc[adf['PatientSex']=='F','PatientSex'] = 'Female'
adf.loc[adf['PatientSex']=='M','PatientSex'] = 'Male'
adf.loc[adf['PatientSex']=='O','PatientSex'] = 'Other'
adf.loc[adf['PatientSex']=='U','PatientSex'] = 'Unknown'

adf.loc[adf.PatientSex=='0000']['collection_id'] # {'acrin_nsclc_fdg_pet', 'nsclc_radiogenomics'} # --> these are all M, from looking at source data on TCIA
adf.loc[(adf['PatientSex']=='0000')&(adf['collection_id'].isin(['acrin_nsclc_fdg_pet','nsclc_radiogenomics'])),'PatientSex']='Male'

adf.loc[adf.PatientSex=='6657']['collection_id'] # ispy1
adf.loc[adf.collection_id=='ispy1'].drop_duplicates(subset="PatientID", keep='first', inplace=False, ignore_index=True)['PatientSex'].value_counts(dropna=False)
adf.loc[(adf['PatientSex']=='6657')&(adf['collection_id'].isin(['ispy1'])),'PatientSex']='Female'
adf.loc[adf.collection_id=='ct_colonography'].drop_duplicates(subset="PatientID", keep='first', inplace=False, ignore_index=True)['PatientSex'].value_counts(dropna=False)

adf.loc[~adf['PatientSex'].isin(['Male','Female','Other','Unknown']),'PatientSex'] = 'Not Reported'
adf['PatientSex'].value_counts(dropna=False)

## Harmonize IDC 'PatientAge' variables (str type: '086Y') to MIDRC 'cases.age_at_index' (int type: 86)
adf['PatientAge'].value_counts()
adf.loc[(adf['PatientAge'].str.isdigit()) & (adf['PatientAge'] != ''),'PatientAge'] = adf.loc[(adf['PatientAge'].str.isdigit()) & (adf['PatientAge'] != ''),'PatientAge'].astype(pd.Int32Dtype())
adf.loc[(adf['PatientAge'].str.endswith(('y','Y'))) & (adf['PatientAge'] != ''),'PatientAge'] = adf.loc[(adf['PatientAge'].str.endswith(('y','Y'))) & (adf['PatientAge'] != ''),'PatientAge'].apply(lambda x: str(x).rstrip('Y').rstrip('y')).astype(pd.Int32Dtype())
adf.loc[(adf['PatientAge'].str.endswith(('M','m'))) & (adf['PatientAge'] != ''),'PatientAge'] = adf.loc[(adf['PatientAge'].str.endswith(('M','m'))) & (adf['PatientAge'] != ''),'PatientAge'].apply(lambda x: str(x).rstrip('M').rstrip('m')).astype(pd.Int32Dtype()).apply(lambda x: x/12).round(0).astype(pd.Int32Dtype())
adf.loc[(adf['PatientAge'].str.endswith(('D','d'))) & (adf['PatientAge'] != ''),'PatientAge'] = adf.loc[(adf['PatientAge'].str.endswith(('D','d'))) & (adf['PatientAge'] != ''),'PatientAge'].apply(lambda x: str(x).rstrip('D').rstrip('d')).astype(pd.Int32Dtype()).apply(lambda x: x/365).round(0).astype(pd.Int32Dtype())
adf.loc[adf['PatientAge'].isin(['NA','N-A','na','','nan','null',' ']),'PatientAge'] = np.nan

############################
## Add dicom viewer URL
## For now, we need to use a different URL for different imaging modalities, as defined below:
viewer_modalities = ['CT', 'MR', 'SR', 'MG', 'CR', 'US', 'XA', 'PT', 'DX', 'RTDOSE', 'REG', 'NM', 'KO', 'FUSION', 'OT', 'SC', 'RF']
slide_modalities = ['SM']
study_modalities = ['PR', 'RWV']
no_viewer_modalities = ['SEG', 'RTSTRUCT', 'RTPLAN', 'XC', 'M3D']
# 'M3D' has been added to no_viewer_modalities because the files do not load properly in IDC DICOM Viewer

adf['dicom_viewer_url'] = '' # set all to empty string, then reset each category below:
# viewer url: "https://viewer.imaging.datacommons.cancer.gov/viewer/{}?SeriesInstanceUID={}".format(study_uid,series_uid)
adf.loc[adf['Modality'].isin(viewer_modalities),'dicom_viewer_url'] = "https://viewer.imaging.datacommons.cancer.gov/viewer/" + adf.loc[adf['Modality'].isin(viewer_modalities),'StudyInstanceUID'].astype(str) + "?SeriesInstanceUID=" + adf.loc[adf['Modality'].isin(viewer_modalities),'SeriesInstanceUID'].astype(str)

# slide url: "https://viewer.imaging.datacommons.cancer.gov/slim/studies/{}/series/{}".format(study_uid,series_uid)
# test = "https://viewer.imaging.datacommons.cancer.gov/slim/studies/" + mdf.loc[mdf['Modality'].isin(slide_modalities),'StudyInstanceUID'].astype(str) + "/series/" + mdf.loc[mdf['Modality'].isin(slide_modalities),'SeriesInstanceUID'].astype(str)
# test[63]
adf.loc[adf['Modality'].isin(slide_modalities),'dicom_viewer_url'] = "https://viewer.imaging.datacommons.cancer.gov/slim/studies/" + adf.loc[adf['Modality'].isin(slide_modalities),'StudyInstanceUID'].astype(str) + "/series/" + adf.loc[adf['Modality'].isin(slide_modalities),'SeriesInstanceUID'].astype(str)

# study url: "https://viewer.imaging.datacommons.cancer.gov/viewer/{}".format(study_uid)
# test = "https://viewer.imaging.datacommons.cancer.gov/viewer/" + mdf.loc[mdf['Modality'].isin(study_modalities),'StudyInstanceUID'].astype(str)
adf.loc[adf['Modality'].isin(study_modalities),'dicom_viewer_url'] = "https://viewer.imaging.datacommons.cancer.gov/viewer/" + adf.loc[adf['Modality'].isin(study_modalities),'StudyInstanceUID'].astype(str)


sorted(list(adf),key=str.casefold)
"""
['BodyPartExamined',
 'collection_cancerType',
 'collection_id',
 'collection_name',
 'collection_species',
 'collection_tumorLocation',
 'crdc_series_uuid',
 'crdc_study_uuid',
 'dicom_viewer_url',
 'EthnicGroup',
 'license_long_name',
 'license_short_name',
 'license_url',
 'Manufacturer',
 'ManufacturerModelName',
 'Modality',
 'PatientAge',
 'PatientID',
 'PatientSex',
 'SeriesDescription',
 'SeriesInstanceUID',
 'Source_DOI',
 'StudyDescription',
 'StudyInstanceUID']
 """

mprops = ['BodyPartExamined',
 'collection_id',
 'collection_name',
 'commons_name',
 'dicom_viewer_url',
 'disease_type',
 'EthnicGroup',
 'Manufacturer',
 'ManufacturerModelName',
 'Modality',
 'PatientAge',
 'PatientID',
 'PatientSex',
 'primary_site',
 'SeriesDescription',
 'SeriesInstanceUID',
 'StudyDescription',
 'StudyInstanceUID']


#########################################################################################################
### Set-up Program, Project, Core Metadata Collection (CMC)
prog = "IDC" 
prog_txt = """{
    "dbgap_accession_number": "%s",
    "name": "%s",
    "type": "program"
}""" % (prog,prog)
prog_json = json.loads(prog_txt)
data = bssub.create_program(json=prog_json)


## Look at overlap between TCIA and IDC collection_ids bc IDC duplicates TCIA data
# tpids = bsexp.get_project_ids(node='program',name='TCIA')
# td = [i.lstrip("TCIA-") for i in tpids]
cids = sorted(list(set(adf['collection_id'])))

for i in range(0,len(cids)):
    cid = cids[i]
    proj = "IDC_{}".format(cid)
    proj_txt = """{
        "availability_type": "Open",
        "code": "%s",
        "dbgap_accession_number": "%s",
        "name": "%s"
        }""" % (proj,proj,proj)
    proj_json = json.loads(proj_txt)
    data = bssub.create_project(program=prog,json=proj_json)
    print(data)

ipids = sorted(bsexp.get_project_ids(node='program',name='IDC'))


######################################################
######################################################
### Create "dataset"
""" BIH dd properties in ETL mapping:
https://github.com/uc-cdis/cdis-manifest/blob/master/bihstaging.data-commons.org/etlMapping.yaml

dataset:
    collection_id
    commons_long_name
    commons_name
    data_contributor
    data_description
    data_host
    data_url_doi
    disease_type
    full_name
    license
    metadata_source_api
    metadata_source_version
    primary_site

metadata_source_date: (not available as of 2025-2-20)

list(adf)
['BodyPartExamined',
 'collection_cancerType',
 'collection_id',
 'collection_name',
 'collection_species',
 'collection_tumorLocation',
 'crdc_series_uuid',
 'crdc_study_uuid',
 'EthnicGroup',
 'license_long_name',
 'license_short_name',
 'license_url',
 'Manufacturer',
 'ManufacturerModelName',
 'Modality',
 'PatientAge',
 'PatientID',
 'PatientSex',
 'SeriesDescription',
 'SeriesInstanceUID',
 'Source_DOI',
 'StudyDescription',
 'StudyInstanceUID',
 'dicom_viewer_url']
"""
### Get IDC version
# https://learn.canceridc.dev/data/data-versioning
# from idc_index import IDCClient
idc_version = idc_index.IDCClient.get_idc_version()

dataset_fields = {"collection_id": "collection_id",
                "collection_name": "full_name",
                "Source_DOI": "data_url_doi", # need to prefix this with "https://doi.org/"
    "collection_cancerType": "disease_type",
    "collection_tumorLocation": "primary_site",
    "license_url": "license",
    }
ddf = copy.deepcopy(adf[[f for f in dataset_fields if f in adf.columns]])
ddf.rename(columns = dataset_fields, inplace = True)
ddf.drop_duplicates(subset='collection_id',inplace=True) # 149
# convert disease_type values from lists to strings

ddf['type'] = "dataset"
ddf['submitter_id'] = ddf['collection_id']
ddf["commons_long_name"] = "NCI Imaging Data Commons"
ddf["commons_name"] = "IDC"
ddf["data_contributor"] = "IDC"
ddf["data_host"] = "IDC"
ddf['projects.code'] = "IDC_" + ddf['submitter_id']
ddf['metadata_source_api'] = "bigquery-public-data.idc_current.dicom_all"
ddf['metadata_source_version'] = idc_version
#ddf['metadata_source_date'] = "Nov 25, 2024, 11:21:56 AM UTC-6"
ddf['data_url_doi'] = 'https://doi.org/' + ddf['data_url_doi']

ddf.to_csv(f"{ingest_dir}/IDC_datasets_metadata_{today}.tsv",sep='\t',index=False,header=True)

failed_datasets = []
projs = ddf['projects.code'].tolist()
for i in range(0,len(projs)):
    proj = projs[i]
    print(f"i:{i} ({i+1}/{len(projs)}): {proj}")
    df = copy.deepcopy(ddf[ddf['projects.code'] == proj])
    df = df.drop_duplicates()
    try:
        d = bsexp.submit_df(project_id = f'IDC-{proj}', df = df)
    except Exception as e:
        print(e)
        failed_datasets.append(proj)
        time.sleep(30)
        continue


######################################################
######################################################
### Create "subject"
"""
subject:
    race
"""

patient_fields = {
    "PatientID": "submitter_id",
    "collection_id": "datasets.submitter_id",
    }

sdf = copy.deepcopy(adf[patient_fields.keys()])
sdf.rename(columns = patient_fields, inplace = True)
# only drop duplicates 
sdf.drop_duplicates(subset=['submitter_id','datasets.submitter_id'],inplace=True) # 69269
sdf['type'] = "subject"
sdf.head()
sdf.to_csv(f"{ingest_dir}/IDC_subjects_metadata_{today}.tsv",sep='\t',index=False,header=True)
sdf=pd.read_csv(f"{ingest_dir}/IDC_subjects_metadata_{today}.tsv",sep='\t')


dids = list(set(sdf['datasets.submitter_id'].tolist())) # 149
#dids = failed_subjects # restart
failed_subjects = []
for i in range(0,len(dids)):
    did = dids[i]
    pid = 'IDC-IDC_' + did
    print(f"\n{i+1}/{len(dids)}: {pid}")
    df = copy.deepcopy(sdf[sdf['datasets.submitter_id'] == did])
    df = df.drop_duplicates(subset='submitter_id')
    try:
        d = bsexp.submit_df(project_id = pid, df = df, chunk_size=1000)
    except Exception as e:
        print(e)
        failed_subjects.append(did)
        time.sleep(30)
        continue

while len(failed_subjects) > 0:
    dids = failed_subjects # restart
    failed_subjects = []
    for i in range(0,len(dids)):
        did = dids[i]
        pid = 'IDC-IDC_' + did
        print(f"\n\n\ni: {i} ({i+1}/{len(dids)}): {pid}")
        df = copy.deepcopy(sdf[sdf['datasets.submitter_id'] == did])
        df = df.drop_duplicates(subset='submitter_id')
        try:
            d = bsexp.submit_df(project_id = pid, df = df, chunk_size=1000)
        except Exception as e:
            print(e)
            failed_subjects.append(did)
            time.sleep(30)
            continue

######################################################
######################################################
### Create imaging_study
"""

imaging_study:
    StudyDescription
    StudyInstanceUID
    PatientAge
    PatientSex
    PatientID
    EthnicGroup
"""
study_fields = {"StudyInstanceUID": "StudyInstanceUID",
    "StudyDescription": "StudyDescription",
    "PatientAge": "PatientAge",
    "PatientSex": "PatientSex",
    "PatientID": "PatientID",
    "EthnicGroup": "EthnicGroup",
    "collection_id": "datasets.submitter_id",
    "Modality":'study_modality'
    }
stdf = copy.deepcopy(adf[[k for k in study_fields if k in adf.columns]]) # bih studies df
stdf.drop_duplicates(subset='StudyInstanceUID',inplace=True) # 147507
stdf.rename(columns = study_fields, inplace = True)
stdf['type'] = "imaging_study"
stdf['subjects.submitter_id'] = stdf['PatientID']
stdf['submitter_id'] = stdf['StudyInstanceUID']
stdf.loc[stdf['StudyInstanceUID'].isnull()] # None
len(list(set(stdf['submitter_id']))) == len(stdf) # True

stdf.to_csv(f"{ingest_dir}/IDC_imaging_studies_metadata_{today}.tsv",sep='\t',index=False,header=True)
stdf=pd.read_csv(f"{ingest_dir}/IDC_imaging_studies_metadata_{today}.tsv",sep='\t')

stdf.loc[stdf.StudyDescription]
stdf.StudyDescription.value_counts(dropna=False)

dids=['pdmr_texture_analysis']
dids=['prostate_mri_us_biopsy'] 

dids = list(set(stdf['datasets.submitter_id'].tolist())) # 149
#dids = failed_studies # restart
failed_studies = []
for i in range(0,len(dids)):
    did = dids[i]
    pid = 'IDC-IDC_' + did
    print(f"\n\n\ni: {i} ({i+1}/{len(dids)}): {pid}")
    df = copy.deepcopy(stdf[stdf['datasets.submitter_id'] == did])
    df = df.drop_duplicates(subset='submitter_id')
    # remove any non utf-8 characters from StudyDescription
    #df['StudyDescription'] = df['StudyDescription'].str.replace('É','E')
    # try unidecode package to remove non utf-8 characters: https://pypi.org/project/Unidecode/
    df['StudyDescription'] = df['StudyDescription'].apply(lambda x: unidecode(str(x)) if isinstance(x, str) else x)
    try:
        res = bsexp.submit_df(project_id = pid, df = df, chunk_size = 500)
        # write submission log
        with open(f"{ingest_dir}/logs/{pid}_{list(set(df['type']))[0]}_submission_log_{datetime.datetime.now().isoformat(timespec='minutes')}.txt", 'w') as log_file:
            json.dump(res, log_file, ensure_ascii=False)
    except Exception as e:
        print(e)
        failed_studies.append(did)
        time.sleep(30)
        continue

# failed_series = list(set(stdf['datasets.submitter_id'].tolist())) # 149
while len(failed_studies) > 0:
    dids = failed_studies # restart
    failed_studies = []
    for i in range(0,len(dids)):
        did = dids[i]
        pid = 'IDC-IDC_' + did
        print(f"\n\n\ni: {i} ({i+1}/{len(dids)}): {pid}")
        df = copy.deepcopy(stdf[stdf['datasets.submitter_id'] == did])
        df = df.drop_duplicates(subset='submitter_id')
        # remove any non utf-8 characters from StudyDescription
        df['StudyDescription'] = df['StudyDescription'].apply(lambda x: unidecode(str(x)) if isinstance(x, str) else x)
        try:
            res = bsexp.submit_df(project_id = pid, df = df, chunk_size = 500)
            # write submission log
            with open(f"{ingest_dir}/logs/{pid}_{list(set(df['type']))[0]}_submission_log_{datetime.datetime.now().isoformat(timespec='minutes')}.txt", 'w') as log_file:
                json.dump(res, log_file, ensure_ascii=False)
        except Exception as e:
            print(e)
            failed_studies.append(did)
            time.sleep(30)
            continue

######################################################
######################################################
### Create imaging_series
"""

imaging_series:
    submitter_id
    object_ids (PIDs of image files)
    BodyPartExamined
    Manufacturer
    Modality
    SeriesDescription
    SeriesInstanceUID
    dicom_viewer_url

"""
series_fields = {
    "SeriesInstanceUID": "SeriesInstanceUID",
    "BodyPartExamined": "BodyPartExamined",
    "Manufacturer": "Manufacturer",
    "Modality": "Modality",
    "SeriesDescription": "SeriesDescription",
    "PatientID": "subjects.submitter_id",
    "StudyInstanceUID": "imaging_studies.submitter_id",
    "collection_id": "datasets.submitter_id",
}

srdf = copy.deepcopy(adf[series_fields.keys()])
srdf.drop_duplicates(subset='SeriesInstanceUID',inplace=True) # 947580
srdf.rename(columns = series_fields, inplace = True)
srdf['submitter_id'] = srdf['SeriesInstanceUID']
srdf['type'] = "imaging_series"
srdf.to_csv(f"{ingest_dir}/IDC_imaging_series_metadata_{today}.tsv",sep='\t',index=False,header=True)
srdf=pd.read_csv(f"{ingest_dir}/IDC_imaging_series_metadata_{today}.tsv",sep='\t')


# failed_series = list(set(srdf['datasets.submitter_id'].tolist())) # 149
while len(failed_series) > 0:
    dids = failed_series # restart
    failed_series = []
    for i in range(0,len(dids)):
        did = dids[i]
        pid = 'IDC-IDC_' + did
        print(f"\n\n\ni: {i} ({i+1}/{len(dids)}): {pid}")
        df = copy.deepcopy(srdf[srdf['datasets.submitter_id'] == did])
        df = df.drop_duplicates(subset='submitter_id')
        # remove any non utf-8 characters from SeriesDescription
        df['SeriesDescription'] = df['SeriesDescription'].apply(lambda x: unidecode(str(x)) if isinstance(x, str) else x)
        df['SeriesDescription'] = df['SeriesDescription'].str.replace('É','E')
        df['SeriesDescription'] = df['SeriesDescription'].str.replace('�','I')
        df['SeriesDescription'] = df['SeriesDescription'].str.replace('Ç','C')
        df['SeriesDescription'] = df['SeriesDescription'].str.replace('Ã','A')
        df['SeriesDescription'] = df['SeriesDescription'].str.replace('²','^2')
        try:
            bsexp = Gen3Expansion(bsapi,Gen3Auth(bsapi, refresh_file=bscred),bssub)
            res = bsexp.submit_df(project_id = pid, df = df, chunk_size = 1000)
            with open(f"{ingest_dir}/logs/{pid}_{list(set(df['type']))[0]}_submission_log_{datetime.datetime.now().isoformat(timespec='minutes')}.txt", 'w') as log_file:
                json.dump(res, log_file, ensure_ascii=False)
        except Exception as e:
            print(e)
            failed_series.append(did)
            time.sleep(30)
            continue

# d = bsexp.submit_df(project_id = pid, df = df, chunk_size = 2500, row_offset=385000)

#### Parse logs
logs = glob.glob(f"{ingest_dir}/logs/*_imaging_series_submission_log_2025-03-12*.txt")+glob.glob(f"{ingest_dir}/logs/*_imaging_series_submission_log_2025-03-13*.txt")
logs_df=pd.DataFrame()
for log in logs:
    file_name=log.rsplit("/",1)[1]
    pid=file_name.rsplit("_imaging_series_submission_log",1)[0]
    with open(log) as log_file:
        data= json.load(log_file)
        if data['succeeded']:
            succeeded=pd.DataFrame(data['succeeded'],columns=['SeriesInstanceUID'])
            succeeded['status']='succeeded'
            succeeded['log_file']=file_name
            succeeded['project_id']=pid
            logs_df=pd.concat([logs_df,succeeded],ignore_index=True)
        if data['invalid']:
            invalid_series = [x for x in set(data['invalid'])]
            print(log)
            invalid=pd.DataFrame(invalid_series,columns=['SeriesInstanceUID'])
            invalid['status']='invalid'
            invalid['log_file']=file_name
            invalid['project_id']=pid
            logs_df=pd.concat([logs_df,invalid],ignore_index=True)
logs_df

logs_df.loc[logs_df.status=='succeeded']
len(list(set(logs_df.loc[logs_df.status=='succeeded']['SeriesInstanceUID']))) # 947580
logs_df.loc[logs_df.status=='invalid']
len(list(set(logs_df.loc[logs_df.status=='invalid']['SeriesInstanceUID']))) # 

## Check counts of things
os.chdir(ingest_dir)
ipids = sorted(bsexp.get_project_ids(node='program',name='IDC'))
idf = bsexp.get_node_tsvs(node='dataset',projects=ipids)
std = bsexp.get_node_tsvs(node='imaging_study', projects=ipids,overwrite=True)
ssd = bsexp.get_node_tsvs(node='imaging_series', projects=ipids,overwrite=True)

# filename="IDC_imaging_series_metadata_20250310.tsv"
# df=pd.read_csv(filename,sep='\t')

