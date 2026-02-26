import os
from anyio import Path
import pandas as pd
import sys
import gen3
import pathlib
import subprocess
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.query import Gen3Query

home = str(Path.home())
git_dir=f'{home}/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion
# %run {home}/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py

######################## BIH
api = 'https://imaging-hub.data-commons.org'
cred = f"{home}/Downloads/bih-credentials.json"
auth = Gen3Auth(api, refresh_file=cred)
sub = Gen3Submission(api, auth)
query = Gen3Query(auth)
index = Gen3Index(auth)
exp = Gen3Expansion(api,auth,sub)

######################## BIH
bsapi = 'https://bihstaging.data-commons.org'
bscred = f"{home}/Downloads/bihstaging-credentials.json"
bsauth = Gen3Auth(bsapi, refresh_file=bscred)
bssub = Gen3Submission(bsapi, bsauth)
bsquery = Gen3Query(bsauth)
bsindex = Gen3Index(bsauth)
bsexp = Gen3Expansion(bsapi,bsauth,bssub)

apids = bsexp.get_project_ids(node='program',name='ACRdart')


#########################################################################################################
##########################################################################################
## Set Authz of pushed files to project
project_id = "ACRdart-ACRIN_6701"
guid = 'dg.MD1RBIH/ad81e3d8-1118-47fe-9071-4928818b7fa0'
irec = bsexp.get_index_for_guids([guid])[0]
authz = ["/programs/ACRdart"]
index.update_record(
        guid,
        authz=authz)

config_cmd = f"gen3-client configure --profile=bihstaging --apiendpoint={bsapi} --cred={bscred}"
config_output = subprocess.run(config_cmd,  shell=True, capture_output=True, text=True)

download_cmd = f"gen3-client download-single --profile=bihstaging --guid={guid} --no-prompt"
download_output = subprocess.run(download_cmd,  shell=True, capture_output=True, text=True)

#########################################################################################################
##########################################################################################
## Read in ACR series metadata
adir = (f"{home}/Documents/Notes/BIH/ACR/ACRIN")
os.chdir(adir)

afile1 = f"{adir}/6666/6666SH.tsv"
adf1 = pd.read_csv(afile1,sep='\t',header=0, dtype=str) # 47865
adf1.loc[adf1['Study ID.1'] != adf1['Study ID']] # 0 
adf1 = adf1.drop(columns=['Study ID.1'],errors='ignore')

afile2 = f"{adir}/6701/6701SHreplacement.tsv"
#adf2 = pd.read_csv(adf2,sep='\t',header=0, dtype=str) # 
## This file is actually comma-separated, not tab-separated despite the name
adf2 = pd.read_csv(afile2, header=0, dtype=str) # 1578
adf2.loc[adf2['Imaging Study ID'] != adf2['Study ID']] # 0
adf2 = adf2.drop(columns=['Imaging Study ID'],errors='ignore')
#### Note: both files have two "Study ID" columns, dropped the duplicate columns above after confirming the study UIDs are all identical
assert(len(list(set(adf1['Series ID']))) == len(adf1)) # confirm all series IDs are unique within each dataset
assert(len(list(set(adf2['Series ID']))) == len(adf2)) # confirm all series IDs are unique within each dataset
sorted(list(adf1)) == sorted(list(adf2)) # True; confirm both datasets have same column names

columns = {'Collection ID':'collection_id',
 'Commons_long_name':'commons_long_name',
 'Commons_name':'commons_name',
 'data_contributor':'data_contributor',
 'license':'license',
 'PatientID':'PatientID',
 'PatientAge':'PatientAge',
 'PatientSex':'PatientSex',
 'EthnicGroup': 'EthnicGroup',
 'Study ID':'StudyInstanceUID',
 'StudyDescription': 'StudyDescription',
 'Series ID':'SeriesInstanceUID',
 'BodyPartExamined':'BodyPartExamined',
 'Manufacturer':'Manufacturer',
 'Modality':'Modality',
 'SeriesDescription':'SeriesDescription',
 'DOI':'data_url_doi'}

## Save re-formatted data:
adf = pd.concat([adf1,adf2],ignore_index=True) # 49443
adf.rename(columns=columns,inplace=True,errors='ignore')
adf['project_id'] = adf['collection_id'].str.replace('ACRIN ','ACRdart-ACRIN_')
# make project_id column first:
cols = list(adf.columns)
cols.insert(0, cols.pop(cols.index('project_id')))
adf = adf[cols]
afile = f"{adir}/ACR_ACRIN_combined_6666_6701_{len(adf)}.tsv"
adf.to_csv(afile, sep='\t', index=False) # 49443

# pid = "ACRdart-ACRIN_6666"
# adf = copy.deepcopy(adf1)

# pid = "ACRdart-ACRIN_6701"
# adf = copy.deepcopy(adf2)

## Get value counts of each to check out data distribution and values
props = ['PatientAge','PatientSex','EthnicGroup','Study ID','StudyDescription','Study ID','Series ID','BodyPartExamined']
for prop in props:
    aprop = columns[prop]
    print("\n{} value counts:".format(aprop))
    display(adf[aprop].value_counts())


#########################################################################################################
#########################################################################################################
# CREATE SHEEPDOG RECORDS
#########################################################################################################
#########################################################################################################
afile = f"{adir}/ACR_ACRIN_combined_6666_6701_{len(adf)}.tsv"
adf = pd.read_csv(afile, sep='\t', dtype=str) # 49443
pids = list(set(adf.project_id))


#########################################################################################################
### Set-up Program, Project, Core Metadata Collection (CMC)
pid = "ACRdart-EA1141Restricted"
pid = 'ACRdart-ACRIN_6701'
pid = 'ACRdart-ACRIN_6666'

prog,proj = pid.split('-',1)
prog_txt = """{
    "dbgap_accession_number": "%s",
    "name": "%s",
    "type": "program"
}""" % (prog,prog)
prog_json = json.loads(prog_txt)
data = bssub.create_program(json=prog_json)


proj_txt = """{
    "availability_type": "Open",
    "code": "%s",
    "dbgap_accession_number": "%s",
    "name": "%s"
    }""" % (proj,proj,proj)
proj_json = json.loads(proj_txt)
data = bssub.create_project(program=prog,json=proj_json)
print(data)

cmc_txt = """{
        "description": "Data from the %s study %s.",
        "submitter_id": "%s",
        "title": "%s",
        "project_id": "%s",
        "type": "core_metadata_collection",
        "projects": [
            {
                "code": "%s"
            }
        ]
    }""" % (prog,proj,proj,proj,pid,proj)

cmc_json = json.loads(cmc_txt)
data = bssub.submit_record(program=prog,project=proj,json=cmc_json)
print(data)


######################################################
### Create "dataset"
"""
Related to the Breast Density metadata collection sent to BIH:

Commons: ACRdart
Collection_id: ACRIN 6666 and 6701

Link: https://dart.acr.org/Home/ImagingHub
Link: https://www.acr.org/Clinical-Resources/Publications-and-Research/Clinical-Research/acrin-legacy-trials
ACRdart long name: "The ACR data analysis & research toolkit (DART)"
ACRIN full name: "The American College of Radiology Imaging Network (ACRIN)"

Dataset / Collection:
- collection_id: the short, usually abbreviated name of the dataset / study / project, etc.
- authz: indicator of user authorization. For open data is “/open”, anything else is controlled, e.g., a dbGaP phs_id- commons: what imaging BDF node the imaging data belongs to
- disease_type: the disease being studied (e.g., “Lung Cancer” for NLST)
- primary_site: the primary site of disease for the patient cohort (e.g., “Lung” for NLST)
"""
pid = 'ACRdart-ACRIN_6666'
pid = 'ACRdart-ACRIN_6701'
prog,proj = pid.split('-',1)


ddf = adf.loc[adf['project_id']==pid]
ddf = ddf[['collection_id']].drop_duplicates()
ddf['type'] = 'dataset'
ddf['projects.code'] = proj
ddf['collection_id'] = proj
ddf['submitter_id'] = proj
ddf['commons_name'] = "ACRdart"
ddf['commons_long_name'] = "The ACR data analysis & research toolkit (DART)"
ddf['data_contributor'] = "ACR"
ddf['data_host'] = "ACRdart"

###################################
# ACRIN 6666: Screening Breast Ultrasound in High-Risk Women
if pid == 'ACRdart-ACRIN_6666':
    ddf['data_description'] = "Data from The American College of Radiology Imaging Network (ACRIN) trial protocol 6666: Screening Breast Ultrasound in High-Risk Women"
    ddf['disease_type'] = 'Breast Cancer'
    ddf['primary_site'] = 'Breast'
    ddf['full_name'] = 'The American College of Radiology Imaging Network (ACRIN) trial protocol 6666'
    ddf['research_description'] = 'ACRIN protocol 6666 was a study that investigated the effectiveness of adding whole-breast screening ultrasound to mammography for high-risk women. The trial, conducted over three years, found that screening with both mammography and ultrasound resulted in a statistically significant increase in cancer detection compared to mammography alone, but also led to more false-positive findings. Ultrasound was shown to be complementary to mammography and particularly effective at finding small, node-negative invasive cancers that might otherwise be missed.'

###################################
# ACRIN 6701: ACRIN 6701 was a study by the ECOG-ACRIN Research Group to assess the reproducibility and repeatability of quantitative MRI metrics in the prostate, specifically the apparent diffusion coefficient (ADC)
if pid == 'ACRdart-ACRIN_6701':
    ddf['data_description'] = "Data from The American College of Radiology Imaging Network (ACRIN) trial protocol 6701: ACRIN 6701 was a study by the ECOG-ACRIN Research Group to assess the reproducibility and repeatability of quantitative MRI metrics in the prostate, specifically the apparent diffusion coefficient (ADC)."
    ddf['disease_type'] = 'Prostate Cancer'
    ddf['primary_site'] = 'Prostate'
    ddf['full_name'] = 'The American College of Radiology Imaging Network (ACRIN) trial protocol 6701'
    ddf['research_description'] = 'The ACRIN 6701 trial protocol was a multicenter study to assess the repeatability and reproducibility of quantitative magnetic resonance imaging (MRI) metrics in the prostate. The study aimed to standardize the use of functional MRI techniques, specifically dynamic contrast-enhanced (DCE-MRI) and diffusion-weighted imaging (DWI), for prostate cancer evaluation.'

## Unused:
#ddf['data_url_doi']
#ddf['license']
#ddf['metadata_source_version']
#ddf['metadata_source_api']

### Submit dataset metadata
d = bsexp.submit_df(df=ddf,project_id=pid)

tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)

dname = f'{tdir}/{pid}_dataset_{len(ddf)}.tsv'
ddf.to_csv(dname,sep='\t',index=False)

######################################################
### Create "subject"
"""
Patient:
- EthnicGroup: the ethnicity of the patient
- PatientAge: (0010,1010) The patient's age in years at the time of the imaging study.
- PatientID: the de-identified ID of the imaging study subject
- PatientSex: the gender or biological sex of the patient
- race: The patient's race category 
"""

subject_props = {
    'PatientID':'submitter_id',
    'EthnicGroup': 'ethnicity',
    'PatientAge':'age_at_index',
    'PatientSex':'gender',
}


pid = 'ACRdart-ACRIN_6666'
pid = 'ACRdart-ACRIN_6701'
prog,proj = pid.split('-',1)

df = adf.loc[adf['project_id']==pid]
sdf = df[list(subject_props)].drop_duplicates()
sdf.rename(columns=subject_props,inplace=True)

sdf['type'] = 'subject'
sdf['datasets.submitter_id'] = proj

## Replace all string values "NA" as np.nan
sdf['age_at_index'] = sdf.age_at_index.str.strip().replace('NA',np.nan).astype('float')


dupes = sdf.loc[sdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv(f'{tdir}/duplicated_{pid}_subjects_{len(dupes)}.tsv', sep='\t', index=False)
sdf.drop_duplicates(subset='submitter_id',inplace=True) # 6701: 34; 6666: 2782
sdf = sdf.loc[~sdf['submitter_id'].isna()]  # 215
sdf.reset_index(drop=True,inplace=True)

# Submit subject metadata
bsexp.submit_df(df=sdf,project_id=pid)

# Save subject TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_subject_{len(sdf)}.tsv'
sdf.to_csv(sname,sep='\t',index=False)

######################################################
### Create imaging_study

"""
list(adf)

Out[186]: 
['project_id',
 'collection_id',
 'commons_long_name',
 'commons_name',
 'data_contributor',
 'license',
 'PatientID',
 'PatientAge',
 'PatientSex',
 'EthnicGroup',
 'StudyInstanceUID',
 'StudyDescription',
 'SeriesInstanceUID',
 'BodyPartExamined',
 'Manufacturer',
 'Modality',
 'SeriesDescription',
 'data_url_doi']
"""

study_props = {
    'PatientID':'PatientID',
    'PatientAge':'PatientAge',
    'PatientSex':'PatientSex',
    'EthnicGroup':'EthnicGroup',
    'BodyPartExamined':'BodyPartExamined',
    'Modality':'study_modality',
    'StudyDescription':'StudyDescription',
    'StudyInstanceUID':'StudyInstanceUID',
}

df = adf.loc[adf['project_id']==pid]
stdf = df[list(study_props)].drop_duplicates(subset='StudyInstanceUID') # 19024
stdf.rename(columns=study_props,inplace=True)
stdf['type'] = 'imaging_study'
stdf['submitter_id'] = stdf['StudyInstanceUID']
stdf['subjects.submitter_id'] = stdf['PatientID']
stdf['datasets.submitter_id'] = proj

if proj == 'ACRIN_6666':
    stdf['primary_site'] = 'Breast'
    stdf['disease_type'] = 'Breast Cancer'
if proj == 'ACRIN_6701':
    stdf['primary_site'] = 'Prostate'
    stdf['disease_type'] = 'Prostate Cancer'

## Fix non float ages and change sex of "O", which is "other", to null
stdf['PatientAge'] = stdf.PatientAge.str.strip().replace('NA',np.nan).astype('float')
stdf.loc[stdf['PatientSex'] == 'O', 'PatientSex'] = np.nan
# {'keys': ['PatientAge'], 'message': "'NA' is not of type 'number', 'null'", 'type': 'INVALID_VALUE'}, 
# {'keys': ['PatientSex'], 'message': "'O' is not one of ['Female', 'Male', None]", 'type': 'ERROR'}]

dupes = stdf.loc[stdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv(f'{tdir}/duplicated_{pid}_imaging_studies_{len(dupes)}.tsv', sep='\t', index=False)

stdf = stdf.loc[~stdf['submitter_id'].isna()] # 6666: 19024; 6701:67
stdf.reset_index(drop=True,inplace=True)

## Submit imaging_study metadata
bsexp.submit_df(df=stdf,project_id=pid)

## Save imaging_study TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_imaging_study_{len(stdf)}.tsv'
stdf.to_csv(sname,sep='\t',index=False)

######################################################
### Create imaging_series
"""
Imaging Series:
- Contrast: indicator of whether contrast was used for imaging
- dicom_viewer_url: the URL where the DICOM image can be previewed
- Manufacturer: (0008, 0070) Manufacturer
- ManufacturerModel: (0008, 1090) Manufacturer's Model Name
- Modality: (0008, 0060) Modality
- SeriesDescription: (0008, 103e) Series Description
- SeriesInstanceUID: the (0020, 000e) Series Instance UID
- object_ids: a list of one or more DRS URIs, GUIDs, or URLs where the data can be accessed.

list(adf)
Out[186]: 
['project_id',
 'collection_id',
 'commons_long_name',
 'commons_name',
 'data_contributor',
 'license',
 'PatientID',
 'PatientAge',
 'PatientSex',
 'EthnicGroup',
 'StudyInstanceUID',
 'StudyDescription',
 'SeriesInstanceUID',
 'BodyPartExamined',
 'Manufacturer',
 'Modality',
 'SeriesDescription',
 'data_url_doi']
"""

series_props = {
    'BodyPartExamined':'BodyPartExamined',
    #'ContrastBolusAgent':'ContrastBolusAgent',
    'Manufacturer':'Manufacturer',
    #'ManufacturerModelName':'ManufacturerModelName',
    'Modality':'Modality',
    'PatientID':'PatientID',
    'SeriesDescription':'SeriesDescription',
    'SeriesInstanceUID':'SeriesInstanceUID',
    'StudyInstanceUID':'StudyInstanceUID',
}
#    'loinc_contrast',
#    'object_id'
#    'radiopharmaceutical',


pid = 'ACRdart-ACRIN_6666'
pid = 'ACRdart-ACRIN_6701'
prog,proj = pid.split('-',1)

df = adf.loc[adf['project_id']==pid]
srdf = df[list(series_props)].drop_duplicates(subset='SeriesInstanceUID') # 6666: 47865; 6701: 1578
srdf['type'] = 'imaging_series'
srdf['submitter_id'] = srdf['SeriesInstanceUID']
srdf.rename(columns={'PatientID':'subjects.submitter_id'},inplace=True)
srdf.rename(columns={'StudyInstanceUID':'imaging_studies.submitter_id'},inplace=True)
srdf['datasets.submitter_id'] = proj

dupes = srdf.loc[srdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv('duplicated_ACR_imaging_series_{}.tsv'.format(len(dupes)),sep='\t',index=False)

srdf.drop_duplicates(subset='submitter_id',inplace=True)
srdf = srdf.loc[~srdf['submitter_id'].isna()]
srdf.reset_index(drop=True,inplace=True) # 6666: 47865; 6701: 1578

## Submit imaging_series metadata
bsexp.submit_df(df=srdf,project_id=pid)
#bsexp.submit_df(df=srdf,project_id=pid,offset=10000)
#bsexp.submit_df(df=srdf,project_id=pid,offset=29000)

## Save imaging_series TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_imaging_series_{len(srdf)}.tsv'
srdf.to_csv(sname,sep='\t',index=False)




#########################################################################################################
##########################################################################################
### ACRIN 6690
# create program and project for 'ACRdart-ACRIN_6666'
pid = 'ACRdart-ACRIN_6690'

prog,proj = pid.split('-',1)
prog_txt = """{
    "dbgap_accession_number": "%s",
    "name": "%s",
    "type": "program"
}""" % (prog,prog)
prog_json = json.loads(prog_txt)
data = bssub.create_program(json=prog_json)

proj_txt = """{
    "availability_type": "Open",
    "code": "%s",
    "dbgap_accession_number": "%s",
    "name": "%s"
    }""" % (proj,proj,proj)
proj_json = json.loads(proj_txt)
data = bssub.create_project(program=prog,json=proj_json)
print(data)


#########################################################################################################
##########################################################################################
## Set Authz of pushed files to project

guid = 'dg.MD1RBIH/ad81e3d8-1118-47fe-9071-4928818b7fa0'
irec = bsexp.get_index_for_guids([guid])[0]

# add acl and authz
res = bsindex.update_record(
    guid=guid,
    acl=[prog,proj],
    authz=[f"/programs/{prog}/projects/{proj}"]
)

#########################################################################################################
##########################################################################################
# configure gen3-client profile and download submission
# gen3-client configure --profile=bihstaging --apiendpoint=https://bihstaging.data-commons.org --cred=~/Downloads/bih-staging-credentials.json
# gen3-client download-single --profile=bihstaging --guid=dg.MD1RBIH/ad81e3d8-1118-47fe-9071-4928818b7fa0 --no-prompt

#########################################################################################################
##########################################################################################
## Read in ACR series metadata
adir = (f"{home}/Documents/Notes/BIH/ACR/ACRIN")
os.chdir(adir)

afile = f"{adir}/6690/6690SH.tsv"
adf = pd.read_csv(afile,sep='\t',header=0,dtype='str',encoding='latin1') # 17633
# remove special characters (non ascii)
for prop in adf.columns:
    adf[prop] = adf[prop].apply(lambda x: unidecode(x) if isinstance(x, str) else x) # remove special characters (non ascii)

columns = {'Collection ID':'collection_id',
 'Commons_long_name':'commons_long_name',
 'Commons_name':'commons_name',
 'Data_contributor':'data_contributor',
 'license':'license',
 'PatientID':'PatientID',
 'PatientAge':'PatientAge',
 'PatientSex':'PatientSex',
 'Ethnicgroup': 'EthnicGroup',
 'StudyID':'StudyInstanceUID',
 'StudyDescription': 'StudyDescription',
 'Series ID':'SeriesInstanceUID',
 'Bodypartexamined':'BodyPartExamined',
 'Manufacturer':'Manufacturer',
 'Modality':'Modality',
 'SeriesDescription':'SeriesDescription',
 'DOI':'data_url_doi'}

## Save re-formatted data:
adf.rename(columns=columns,inplace=True,errors='ignore')
adf['project_id'] = adf['collection_id'].str.replace('ACRIN ','ACRdart-ACRIN_')
# make project_id column first:
cols = list(adf.columns)
cols.insert(0, cols.pop(cols.index('project_id')))
adf = adf[cols]
afile = f"{adir}/ACR_ACRIN_6690_{len(adf)}.tsv"
adf.to_csv(afile, sep='\t', index=False) # 17633

## Get value counts of each to check out data distribution and values
props = ['PatientAge','PatientSex','EthnicGroup','StudyInstanceUID','StudyDescription','Study ID','Series ID','BodyPartExamined']
for prop in props:
    aprop = columns[prop]
    print("\n{} value counts:".format(aprop))
    print(adf[aprop].value_counts())

set(adf.EthnicGroup)
#  {'1', '6', 'REMOVED', nan}

#########################################################################################################
#########################################################################################################
# CREATE SHEEPDOG RECORDS
#########################################################################################################
#########################################################################################################
afile = f"{adir}/ACR_ACRIN_6690_{len(adf)}.tsv"
adf = pd.read_csv(afile, sep='\t', dtype=str) # 49443
pids = list(set(adf.project_id))

#########################################################################################################
### Set-up Program, Project, Core Metadata Collection (CMC)
# pid = "ACRdart-EA1141Restricted"
# pid = 'ACRdart-ACRIN_6701'
pid = 'ACRdart-ACRIN_6690'

cmc_txt = """{
        "description": "Data from the %s study %s.",
        "submitter_id": "%s",
        "title": "%s",
        "project_id": "%s",
        "type": "core_metadata_collection",
        "projects": [
            {
                "code": "%s"
            }
        ]
    }""" % (prog,proj,proj,proj,pid,proj)

cmc_json = json.loads(cmc_txt)
data = bssub.submit_record(program=prog,project=proj,json=cmc_json)
print(data)


######################################################
### Create "dataset"
"""
Related to the Breast Density metadata collection sent to BIH:

Commons: ACRdart
Link: https://dart.acr.org/Home/ImagingHub
Link: https://www.acr.org/Clinical-Resources/Publications-and-Research/Clinical-Research/acrin-legacy-trials
ACRdart long name: "The ACR data analysis & research toolkit (DART)"
ACRIN full name: "The American College of Radiology Imaging Network (ACRIN)"

Dataset / Collection:
- collection_id: the short, usually abbreviated name of the dataset / study / project, etc.
- authz: indicator of user authorization. For open data is “/open”, anything else is controlled, e.g., a dbGaP phs_id- commons: what imaging BDF node the imaging data belongs to
- disease_type: the disease being studied (e.g., “Lung Cancer” for NLST)
- primary_site: the primary site of disease for the patient cohort (e.g., “Lung” for NLST)
"""
pid = 'ACRdart-ACRIN_6690'
prog,proj = pid.split('-',1)

ddf = adf.loc[adf['project_id']==pid]
ddf = ddf[['collection_id']].drop_duplicates()
ddf['type'] = 'dataset'
ddf['projects.code'] = proj
ddf['collection_id'] = proj
ddf['submitter_id'] = proj
ddf['commons_name'] = "ACRdart"
ddf['commons_long_name'] = "The ACR data analysis & research toolkit (DART)"
ddf['data_contributor'] = "ACR"
ddf['data_host'] = "ACRdart"

###################################
# https://clinicaltrials.gov/study/NCT01082224
# ACRIN 6690: A Prospective, Multicenter Comparison of Multiphase Contrast-Enhanced CT and Multiphase Contrast-Enhanced MRI for Diagnosis of Hepatocellular Carcinoma and Liver Transplant Allocation
if pid == 'ACRdart-ACRIN_6690':
    ddf['data_description'] = "Data from The American College of Radiology Imaging Network (ACRIN) trial protocol 6690: A Prospective, Multicenter Comparison of Multiphase Contrast-Enhanced CT and Multiphase Contrast-Enhanced MRI for Diagnosis of Hepatocellular Carcinoma and Liver Transplant Allocation"
    ddf['disease_type'] = 'Liver Cancer' # Hepatocellular carcinoma
    ddf['primary_site'] = 'Liver'
    ddf['full_name'] = 'The American College of Radiology Imaging Network (ACRIN) trial protocol 6690'
    ddf['research_description'] = 'The ACRIN 6690 trial protocol was a prospective, multicenter clinical trial comparing multiphase contrast-enhanced CT and MRI for diagnosing hepatocellular carcinoma (HCC) in chronic liver disease patients. The study aimed to evaluate the accuracy of these imaging methods for cancer detection, staging, and potential liver transplant allocation.'

## Unused:
#ddf['data_url_doi']
#ddf['license']
#ddf['metadata_source_version']
#ddf['metadata_source_api']

### Submit dataset metadata
d = bsexp.submit_df(df=ddf,project_id=pid)

tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)

dname = f'{tdir}/{pid}_dataset_{len(ddf)}.tsv'
ddf.to_csv(dname,sep='\t',index=False)

######################################################
### Create "subject"
"""
Patient:
- EthnicGroup: the ethnicity of the patient
- PatientAge: (0010,1010) The patient's age in years at the time of the imaging study.
- PatientID: the de-identified ID of the imaging study subject
- PatientSex: the gender or biological sex of the patient
- race: The patient's race category 
"""
subject_props = {
    'PatientID':'submitter_id',
    'EthnicGroup': 'ethnicity',
    'PatientAge':'age_at_index',
    'PatientSex':'gender',
}

pid = 'ACRdart-ACRIN_6690'
prog,proj = pid.split('-',1)

df = adf.loc[adf['project_id']==pid]
sdf = df[list(subject_props)].drop_duplicates()
sdf.rename(columns=subject_props,inplace=True)

sdf['type'] = 'subject'
sdf['datasets.submitter_id'] = proj

## Replace all string values "NA" as np.nan
sdf['age_at_index'] = sdf.age_at_index.str.strip().replace('NA',np.nan).astype('float')
## add 'age_at_index_gt89'
sdf.loc[(~sdf.age_at_index.isna())&(sdf.age_at_index<90),'age_at_index_gt89']='No'
sdf.loc[(~sdf.age_at_index.isna())&(sdf.age_at_index>89),'age_at_index_gt89']='Yes'
sdf.loc[(sdf.age_at_index.isna()),'age_at_index_gt89']=np.NaN
sdf.loc[sdf.age_at_index_gt89=='Yes','age_at_index']=np.NaN

## 
# sdf['gender']=sdf['gender'].map(
#     {'M':'Male',
#      'F':'Female',
#      'O':'O'}
# )

dupes = sdf.loc[sdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv(f'{tdir}/duplicated_{pid}_subjects_{len(dupes)}.tsv', sep='\t', index=False)
sdf.drop_duplicates(subset='submitter_id',inplace=True) # 6701: 34; 6666: 2782
sdf = sdf.loc[~sdf['submitter_id'].isna()]  # 215
sdf.reset_index(drop=True,inplace=True)

# Submit subject metadata
bsexp.submit_df(df=sdf,project_id=pid)

# Save subject TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_subject_{len(sdf)}.tsv'
sdf.to_csv(sname,sep='\t',index=False)

######################################################
### Create imaging_study

"""
list(adf)

Out[186]: 
['project_id',
 'collection_id',
 'commons_long_name',
 'commons_name',
 'data_contributor',
 'license',
 'PatientID',
 'PatientAge',
 'PatientSex',
 'EthnicGroup',
 'StudyInstanceUID',
 'StudyDescription',
 'SeriesInstanceUID',
 'BodyPartExamined',
 'Manufacturer',
 'Modality',
 'SeriesDescription',
 'data_url_doi']
"""

study_props = {
    'PatientID':'PatientID',
    'PatientAge':'PatientAge',
    'PatientSex':'PatientSex',
    'EthnicGroup':'EthnicGroup',
    'BodyPartExamined':'BodyPartExamined',
    'Modality':'study_modality',
    'StudyDescription':'StudyDescription',
    'StudyInstanceUID':'StudyInstanceUID',
}

df = adf.loc[adf['project_id']==pid]
stdf = df[list(study_props)].drop_duplicates(subset='StudyInstanceUID') # 19024
stdf.rename(columns=study_props,inplace=True)
stdf['type'] = 'imaging_study'
stdf['submitter_id'] = stdf['StudyInstanceUID']
stdf['subjects.submitter_id'] = stdf['PatientID']
stdf['datasets.submitter_id'] = proj

if proj == 'ACRIN_6666':
    stdf['primary_site'] = 'Breast'
    stdf['disease_type'] = 'Breast Cancer'
if proj == 'ACRIN_6701':
    stdf['primary_site'] = 'Prostate'
    stdf['disease_type'] = 'Prostate Cancer'
if proj == 'ACRIN_6690':
    stdf['primary_site'] = 'Liver'
    stdf['disease_type'] = 'Liver Cancer'

## Fix non float ages and change sex of "O", which is "other", to null
stdf['PatientAge'] = stdf.PatientAge.str.strip().replace('NA',np.nan).astype('float')
stdf.loc[(~stdf.PatientAge.isna())&(stdf.PatientAge>89),'PatientAge']=np.NaN
stdf['PatientSex']=stdf['PatientSex'].map(
    {'M':'Male',
     'F':'Female',
     'O':np.NaN}
)
# {'keys': ['PatientAge'], 'message': "'NA' is not of type 'number', 'null'", 'type': 'INVALID_VALUE'}, 
# {'keys': ['PatientSex'], 'message': "'O' is not one of ['Female', 'Male', None]", 'type': 'ERROR'}]

dupes = stdf.loc[stdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv(f'{tdir}/duplicated_{pid}_imaging_studies_{len(dupes)}.tsv', sep='\t', index=False)
stdf = stdf.loc[~stdf['submitter_id'].isna()] # 6666: 19024; 6701: 67; 6609: 1453
stdf.reset_index(drop=True,inplace=True)

## Submit imaging_study metadata
bsexp.submit_df(df=stdf,project_id=pid)

## Save imaging_study TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_imaging_study_{len(stdf)}.tsv'
stdf.to_csv(sname,sep='\t',index=False)

######################################################
### Create imaging_series
"""
Imaging Series:
- Contrast: indicator of whether contrast was used for imaging
- dicom_viewer_url: the URL where the DICOM image can be previewed
- Manufacturer: (0008, 0070) Manufacturer
- ManufacturerModel: (0008, 1090) Manufacturer's Model Name
- Modality: (0008, 0060) Modality
- SeriesDescription: (0008, 103e) Series Description
- SeriesInstanceUID: the (0020, 000e) Series Instance UID
- object_ids: a list of one or more DRS URIs, GUIDs, or URLs where the data can be accessed.


list(adf)
Out[186]: 
['project_id',
 'collection_id',
 'commons_long_name',
 'commons_name',
 'data_contributor',
 'license',
 'PatientID',
 'PatientAge',
 'PatientSex',
 'EthnicGroup',
 'StudyInstanceUID',
 'StudyDescription',
 'SeriesInstanceUID',
 'BodyPartExamined',
 'Manufacturer',
 'Modality',
 'SeriesDescription',
 'data_url_doi']
"""

series_props = {
    'BodyPartExamined':'BodyPartExamined',
    #'ContrastBolusAgent':'ContrastBolusAgent',
    'Manufacturer':'Manufacturer',
    #'ManufacturerModelName':'ManufacturerModelName',
    'Modality':'Modality',
    'PatientID':'PatientID',
    'SeriesDescription':'SeriesDescription',
    'SeriesInstanceUID':'SeriesInstanceUID',
    'StudyInstanceUID':'StudyInstanceUID',
}
#    'loinc_contrast',
#    'object_id'
#    'radiopharmaceutical',

pid = 'ACRdart-ACRIN_6690'
prog,proj = pid.split('-',1)

df = adf.loc[adf['project_id']==pid]
srdf = df[list(series_props)].drop_duplicates(subset='SeriesInstanceUID') # 6666: 47865; 6701: 1578
srdf['type'] = 'imaging_series'
srdf['submitter_id'] = srdf['SeriesInstanceUID']
srdf.rename(columns={'PatientID':'subjects.submitter_id'},inplace=True)
srdf.rename(columns={'StudyInstanceUID':'imaging_studies.submitter_id'},inplace=True)
srdf['datasets.submitter_id'] = proj

dupes = srdf.loc[srdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv('duplicated_ACR_imaging_series_{}.tsv'.format(len(dupes)),sep='\t',index=False)

srdf.drop_duplicates(subset='submitter_id',inplace=True)
srdf = srdf.loc[~srdf['submitter_id'].isna()]
srdf.reset_index(drop=True,inplace=True) # 6666: 47865; 6701: 1578

## Submit imaging_series metadata
bsexp.submit_df(df=srdf,project_id=pid)
#bsexp.submit_df(df=srdf,project_id=pid,offset=10000)
#bsexp.submit_df(df=srdf,project_id=pid,offset=29000)

## Save imaging_series TSV
tdir = f'{adir}/tsvs/{pid}'
os.makedirs(tdir,exist_ok=True)
sname = f'{tdir}/{pid}_imaging_series_{len(srdf)}.tsv'
srdf.to_csv(sname,sep='\t',index=False)






