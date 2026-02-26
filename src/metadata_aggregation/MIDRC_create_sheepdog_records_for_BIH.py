#########################
import pandas as pd
import sys, os
import gen3
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.query import Gen3Query
import glob
import copy
import json
from pathlib import Path
home= str(Path.home())

import datetime
now = datetime.datetime.now()
date = "{}-{}-{}".format(now.year, now.month, now.day)

git_dir=f'{home}/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion

######################## MIDRC Staging
sapi = 'https://staging.midrc.org'
scred = f'{home}/Downloads/midrc-staging-credentials.json'
sauth = Gen3Auth(sapi, refresh_file=scred)
ssub = Gen3Submission(sapi, sauth)
sindex = Gen3Index(sauth)
squery = Gen3Query(sauth)
sexp = Gen3Expansion(sapi,sauth,ssub)
sexp.get_project_ids()

# ######################## BIH
# api = 'https://imaging-hub.data-commons.org'
# cred = f'{home}/Downloads/bih-credentials.json'
# auth = Gen3Auth(api, refresh_file=cred)
# sub = Gen3Submission(api, auth)
# query = Gen3Query(auth)
# index = Gen3Index(auth)
# exp = Gen3Expansion(api,auth,sub)

######################## BIH STAGING
bsapi = 'https://bihstaging.data-commons.org'
bscred = f'{home}/Downloads/bih-staging-credentials.json'
bsauth = Gen3Auth(bsapi, refresh_file=bscred)
bssub = Gen3Submission(bsapi, bsauth)
bsquery = Gen3Query(bsauth)
bsindex = Gen3Index(bsauth)
bsexp = Gen3Expansion(bsapi,bsauth,bssub)
bsexp.get_project_ids()

######################## MIDRC
mapi = 'https://data.midrc.org'
mcred = f'{home}/Downloads/midrc-credentials.json'
mauth = Gen3Auth(mapi, refresh_file=mcred)
msub = Gen3Submission(mapi, mauth)
mquery = Gen3Query(mauth)
mindex = Gen3Index(mauth)
mexp = Gen3Expansion(mapi,mauth,msub)
mexp.get_project_ids()

### BIH Metadata schema
# https://docs.google.com/document/d/1NFRFQXdAzsZcJBJ4LpuoG-NsUZxqpgmCph2YNsoiEm0/edit#heading=h.egf3f3g09rlh
"""
Dataset / Collection:
- collection_id: the short, usually abbreviated name of the dataset / study / project, etc.
- authz: indicator of user authorization. For open data is “/open”, anything else is controlled, e.g., a dbGaP phs_id
- commons: what imaging BDF node the imaging data belongs to
- disease_type: the disease being studied (e.g., “Lung Cancer” for NLST)
- primary_site: the primary site of disease for the patient cohort (e.g., “Lung” for NLST)
Patient:
- EthnicGroup: the ethnicity of the patient
- PatientAge: (0010,1010) The patient's age in years at the time of the imaging study.
- PatientID: the de-identified ID of the imaging study subject
- PatientSex: the gender or biological sex of the patient
- race: The patient’s race category 
Imaging Study:
- BodyPartExamined: (0018, 0015) Body Part Examined
- StudyDescription: (0008, 1030) Study Description
- StudyInstanceUID: (0020, 000d) Study Instance UID
Imaging Series:
- Contrast: indicator of whether contrast was used for imaging
- dicom_viewer_url: the URL where the DICOM image can be previewed
- Manufacturer: (0008, 0070) Manufacturer
- ManufacturerModel: (0008, 1090) Manufacturer's Model Name
- Modality: (0008, 0060) Modality
- SeriesDescription: (0008, 103e) Series Description
- SeriesInstanceUID: the (0020, 000e) Series Instance UID
- object_ids: a list of one or more DRS URIs, GUIDs, or URLs where the data can be accessed.
"""

#############################################################################
#############################################################################
########### Create MIDRC Sheepdog records in BIH

mdir = f"{home}/Documents/Notes/BIH/MIDRC"
os.chdir(mdir)

# create a metadata folder for storing TSVs later on
Path(f"{mdir}/metadata").mkdir(parents=True, exist_ok=True)

# corresponding properties in MIDRC
props = ['age_at_imaging',
 'body_part_examined',
 'case_ids',
 'contrast_bolus_agent',
 'covid19_positive',
 'data_url_doi',
 'data_contributor',
 'ethnicity',
 'license',
 'loinc_code',
 'loinc_long_common_name',
 'loinc_contrast',
 'loinc_method',
 'loinc_system',
 'manufacturer',
 'manufacturer_model_name',
 'modality',
 'object_id',
 'project_id',
 'race',
 'radiopharmaceutical',
 'series_description',
 'series_uid',
 'sex',
 'study_description',
 'study_modality',
 'study_uid',
 'submitter_id']

# metadata properties: {<name in MIDRC> : <name in BIH dictionary>}
mprops = {'series_uid':'SeriesInstanceUID', 
    'study_uid':'StudyInstanceUID', 
    'project_id':'collection_id',
    'case_ids':'PatientID',
    'sex':'PatientSex',
    'age_at_imaging':'PatientAge',
    'ethnicity':'EthnicGroup',
    'race':'race',
    'body_part_examined':'BodyPartExamined',
    'manufacturer':'Manufacturer',
    'manufacturer_model_name':'ManufacturerModelName',
    'modality':'Modality',
    'study_description':'StudyDescription',
    'series_description':'SeriesDescription'}

source_nodes = ["ct_series_file",
                "cr_series_file",
                "dx_series_file",
                "dicom_annotation_file",
                "mg_series_file",
                "mr_series_file",
                "nm_series_file",
                "pt_series_file",
                "rf_series_file",
                "us_series_file",
                "xa_series_file"]

project_ids = mexp.get_project_ids()
project_ids = sexp.get_project_ids()
#project_ids = ['Open-A1','Open-R1']

## If you're getting 500 / time-out errors, try looping through source_nodes
mseries = []
for node in source_nodes:
    print(node)
    nseries = mquery.raw_data_download( # query Prod, what's currently released
        data_type="data_file",
        fields=props,
        filter_object={
            "AND": [
                #{"IN": {"project_id": project_ids}}, # get all projects
                {"=": {"source_node": node}}
            ]
        }
    )
    mseries+=nseries

display(len(mseries)) # 
display(list(set([i['project_id'] for i in mseries])))

mdf = pd.DataFrame(mseries) # convert to tsv

# dupes = mdf.loc[mdf.duplicated(subset='series_uid',keep=False)]
dupes = mdf.loc[mdf.duplicated(subset='submitter_id',keep=False)]

if len(dupes) > 0:
    dupes.to_csv(f'duplicated_MIDRC_imaging_series_{today}.tsv',sep='\t',index=False)

##############################################
# function for cleaning array properties from query response
def clean_list(list_of_items):
    """
    clean each item in list before appending them together in a string (seperated by commas if multiple items)
    """
    try:
        # ensure input is list
        assert(isinstance(list_of_items,list))
    except:
        raise AssertionError
    # return None if list 
    if not list_of_items:
        return None
    elif list_of_items:
        cleaned_list = []
        for item in list_of_items:
            if item: # if item in list is Not empty, clean it
                cleaned_string = str(item).strip()  # convert item to string and strip whitespace
                cleaned_string = cleaned_string.replace("  "," ") # convert any double-space to single-space
                cleaned_list.append(cleaned_string)
            else: # if string is empty, continue
                continue
        return ",".join(cleaned_list)

## Convert list values to strings: ['64'] -> '64'
mdf["study_uid"] = mdf["study_uid"].apply(clean_list)
mdf["race"] = mdf["race"].apply(clean_list)
mdf["ethnicity"] = mdf["ethnicity"].apply(clean_list)
mdf["covid19_positive"] = mdf["covid19_positive"].apply(clean_list)
mdf["sex"] = mdf["sex"].apply(clean_list)
mdf["study_description"] = mdf["study_description"].apply(clean_list)

mdf["body_part_examined"] = mdf["body_part_examined"].apply(clean_list)
mdf.loc[mdf.body_part_examined=='<Undefined>',"body_part_examined"]=None

mdf["age_at_imaging"] = mdf["age_at_imaging"].apply(clean_list)
mdf["case_ids"] = mdf["case_ids"].apply(clean_list)
mdf["study_modality"] = mdf["study_modality"].apply(clean_list)

mdf["data_url_doi"] = mdf["data_url_doi"].apply(clean_list)
mdf["license"] = mdf["license"].apply(clean_list)
mdf["data_contributor"] = mdf["data_contributor"].apply(clean_list)

mdf["loinc_long_common_name"] = mdf["loinc_long_common_name"].apply(clean_list)
mdf["loinc_contrast"] = mdf["loinc_contrast"].apply(clean_list)
mdf["loinc_code"] = mdf["loinc_code"].apply(clean_list)
mdf["loinc_method"] = mdf["loinc_method"].apply(clean_list)
mdf["loinc_system"] = mdf["loinc_system"].apply(clean_list)

### WARNING: some series lack a study_uid
mdf.loc[mdf['study_uid']=='']
mdf.loc[mdf['study_uid'].isna()].reset_index(drop=True) # 420
outname = "MIDRC_imaging_series_{}_{}.tsv".format(len(mdf),date)
display(outname)

mdf.to_csv(outname,sep='\t',index=False)

#########################################################################################################
##########################################################################################
## Read in saved MIDRC series metadata
mdir = f"{home}/Documents/Notes/BIH/MIDRC"
os.chdir(mdir)

mfiles = glob.glob("MIDRC_imaging_series_*")
print(mfiles)

mfile = mfiles[-1]
mdf = pd.read_csv(mfile, sep='\t', header=0, dtype=str) # 460444


#########################################################################################################
#########################################################################################################
# CREATE SHEEPDOG RECORDS
#########################################################################################################
#########################################################################################################
## Check how many unique projects there are in the metadata; these will be used to create "dataset" records in BIH
mdf.project_id.value_counts()

#########################################################################################################
#########################################################################################################
### Set-up Program, Project, Core Metadata Collection (CMC)
projs = sorted(list(set(mdf.project_id)))
prog = 'MIDRC'
prog_txt = """{
    "dbgap_accession_number": "%s",
    "name": "%s",
    "type": "program"
}""" % (prog,prog)
prog_json = json.loads(prog_txt)
data = bssub.create_program(json=prog_json)

prog = 'MIDRC'
for proj in projs:
    proj_txt = """{
        "availability_type": "Open",
        "code": "%s",
        "dbgap_accession_number": "%s",
        "name": "%s"
        }""" % (proj,proj,proj)
    proj_json = json.loads(proj_txt)
    print(proj_json)
    data = bssub.create_project(program=prog,json=proj_json)
    print(data)


################################################################################
################################################################################
""" 
Dataset / Collection:
- collection_id: the short, usually abbreviated name of the dataset / study / project, etc.
- authz: indicator of user authorization. For open data is “/open”, anything else is controlled, e.g., a dbGaP phs_id
- commons: what imaging BDF node the imaging data belongs to
- disease_type: the disease being studied (e.g., “Lung Cancer” for NLST)
- primary_site: the primary site of disease for the patient cohort (e.g., “Lung” for NLST)

datasets[collection_id,
    commons_long_name,
    commons_name,
    data_contributor,
    data_host,
    data_url_doi,
    disease_type,
    license,
    metadata_source_api,
    metadata_source_version,
    primary_site]
"""

ddf = mdf[['project_id','data_contributor','license','data_url_doi']].drop_duplicates().sort_values(by=['project_id','data_url_doi']) # 16
len(list(set(ddf['project_id']))) # 11
ddf = ddf.loc[ddf['data_contributor']!=''] # 13
ddf.loc[ddf['project_id']=='Open-A1_PETAL_REDCORAL','data_url_doi'] = "https://doi.org/10.4037/ajcc2022549"
ddf.loc[ddf['project_id']=='Open-A1_PETAL_REDCORAL','data_contributor'] = "ACR"
ddf.loc[ddf['project_id']=='Open-A1_PETAL_BLUECORAL','data_url_doi'] = "https://doi.org/10.1001/jamanetworkopen.2022.55795"
ddf.loc[ddf['project_id']=='Open-A1_PETAL_BLUECORAL','data_contributor'] = "ACR"
ddf = ddf.drop_duplicates(subset='project_id',keep='first').reset_index(drop=True)

ddf['data_contributor'] = 'MIDRC'
ddf['data_host'] = 'MIDRC'
ddf['commons_name'] = 'MIDRC'
ddf['commons_long_name'] = 'Medical Imaging Data Resource Center (MIDRC)'
ddf['disease_type'] = 'COVID-19'
ddf['primary_site'] = 'Chest'
ddf['metadata_source_api'] = 'data.midrc.org'
ddf['metadata_source_version'] = '2025.02'
ddf['type'] = 'dataset'
ddf['projects.code'] = ddf['project_id']
ddf['collection_id'] = "MIDRC-" + ddf['project_id']
ddf.rename(columns={'project_id':'submitter_id'},inplace=True)

date = datetime.datetime.now()
date = "{}-{}-{}".format(date.year, date.month, date.day)
ddf.to_csv(f'metadata/MIDRC_dataset_metadata_{date}.tsv', sep='\t', index=False)

### Create "dataset"
failed_datasets = []
pids = sorted(list(set(ddf['collection_id'])))
for pid in pids:
    print(pid)
    df = ddf.loc[ddf['collection_id']==pid]
    try:
        bsexp.submit_df(df=df,project_id=pid)
    except Exception as e:
        print(e)
        failed_datasets.append(pid)
        continue

################################################################################
################################################################################
### Create "subject"
"""
Patient:
- EthnicGroup: the ethnicity of the patient
- PatientAge: (0010,1010) The patient's age in years at the time of the imaging study.
- PatientID: the de-identified ID of the imaging study subject
- PatientSex: the gender or biological sex of the patient
- race: The patient’s race category 

subjects[race,subject_id:submitter_id].
"""

subject_props = {'case_ids':'submitter_id',
    'race':'race',
    'project_id':'datasets.submitter_id'}

sdf = copy.deepcopy(mdf[subject_props.keys()].drop_duplicates())
sdf.rename(columns=subject_props,inplace=True)
sdf['type'] = 'subject'
dupes = sdf.loc[sdf.duplicated(subset='submitter_id',keep=False)]
if len(dupes) > 0:
    dupes.to_csv('duplicated_MIDRC_subjects_{}.tsv'.format(date),sep='\t',index=False)
sdf.reset_index(drop=True,inplace=True)
sdf = sdf.loc[~sdf['submitter_id'].isna()] # 76199

sdf.to_csv(f"{mdir}/metadata/MIDRC_subject_metadata_{date}.tsv", sep='\t', index=False)

prog = 'MIDRC'
## Loop through projs
dids = sorted(list(set(sdf['datasets.submitter_id'])))
failed_subjects = []
for i in range(0,len(dids)):
    did = dids[i]
    pid = f"{prog}-{did}"
    print(f"\n\n\n({i}/{len(dids)}) {did}")
    df = copy.deepcopy(sdf.loc[sdf['datasets.submitter_id']==did])
    df.drop_duplicates(subset='submitter_id',keep='first',inplace=True)
    try:
        d = bsexp.submit_df(df=df, project_id=pid, chunk_size=2500)
    except Exception as e:
        print(e)
        failed_subjects.append(pid)
        continue

################################################################################
################################################################################
### Create imaging_study
"""

imaging_studies[
    StudyDescription,
    StudyInstanceUID,
    PatientAge,
    PatientSex,
    PatientID,
    EthnicGroup].
"""

study_fields = {"study_uid": "StudyInstanceUID",
    "study_description": "StudyDescription",
    "age_at_imaging": "PatientAge",
    "sex": "PatientSex",
    "case_ids": "PatientID",
    "ethnicity": "EthnicGroup",
    "project_id": "datasets.submitter_id",
    "study_modality":'study_modality'
}

stdf = copy.deepcopy(mdf[study_fields.keys()])
stdf.rename(columns=study_fields,inplace=True)
stdf['type'] = 'imaging_study'
stdf['submitter_id'] = stdf['StudyInstanceUID']
stdf['subjects.submitter_id'] = stdf['PatientID']
stdf.drop_duplicates(subset=['submitter_id','datasets.submitter_id'],keep='first',inplace=True)
stdf.reset_index(drop=True,inplace=True) # 187961

dupes = stdf.loc[stdf.duplicated(subset=['submitter_id','datasets.submitter_id'],keep=False)] # 0
if len(dupes) > 0:
    dupes.to_csv('metadata/duplicated_MIDRC_imaging_studies_{}.tsv'.format(proj),sep='\t',index=False)

stdf.loc[stdf['submitter_id'].isna()] # 
dicom_viewer = 'https://data.midrc.org/ohif-viewer/viewer?StudyInstanceUIDs='
stdf['study_viewer_url'] = stdf['StudyInstanceUID'].apply(lambda x: "{}{}".format(dicom_viewer, x))

stdf.to_csv('metadata/MIDRC_imaging_studies_metadata_{}.tsv'.format(datetime.date.today()),sep='\t',index=False)

## Loop through projs
dids = list(set(stdf['datasets.submitter_id']))
failed_studies = []
for i in range(0,len(dids)):
    did = dids[i]
    pid = "MIDRC-" + did
    print("\n\n\n({}/{}) {}".format(i,len(dids),pid))
    df = copy.deepcopy(stdf.loc[stdf['datasets.submitter_id']==did])
    df.drop_duplicates(subset=['submitter_id'],keep='first',inplace=True)
    try:
        d = bsexp.submit_df(df=df, project_id=pid, chunk_size=2500)
    except Exception as e:
        print(e)
        failed_studies.append(did)
        continue


#########################################################################################################
#########################################################################################################
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
series_fields = {"submitter_id":"submitter_id",
    "series_uid": "SeriesInstanceUID",
    "body_part_examined": "BodyPartExamined",
    "manufacturer": "Manufacturer",
    "modality": "Modality",
    "series_description": "SeriesDescription",
    "case_ids": "subjects.submitter_id",
    "study_uid": "imaging_studies.submitter_id",
    "project_id": "datasets.submitter_id",
    "object_id": "object_ids",
}

srdf = copy.deepcopy(mdf[series_fields.keys()])
srdf.rename(columns = series_fields, inplace = True)
srdf.drop_duplicates(subset=['submitter_id','datasets.submitter_id'],inplace=True) # 
srdf['type'] = "imaging_series"
dicom_viewer = 'https://data.midrc.org/ohif-viewer/viewer?StudyInstanceUIDs='
srdf['dicom_viewer_url'] = srdf['imaging_studies.submitter_id'].apply(lambda x: "{}{}".format(dicom_viewer, x))
srdf['series_viewer_url'] = srdf['imaging_studies.submitter_id'].apply(lambda x: "{}{}".format(dicom_viewer, x))
# # remove any special chars; shouldn't need to do this since these data are from Gen3 already
srdf['SeriesDescription'] = srdf['SeriesDescription'].apply(lambda x: x.encode('ascii', 'ignore').decode('ascii') if isinstance(x, str) else x)

srdf.reset_index(drop=True,inplace=True)
srdf.to_csv('metadata/MIDRC_imaging_series_metadata_{}.csv'.format(datetime.date.today()),index=False,header=True)

## Check missing ids and duplicates
# srdf.loc[srdf['submitter_id'].isna()]
# srdf.loc[srdf['submitter_id']=='']
dupes = srdf.loc[srdf.duplicated(subset=['submitter_id','datasets.submitter_id'],keep=False)]
if len(dupes) > 0:
    dupes.to_csv('metadata/duplicated_MIDRC_imaging_series.tsv',sep='\t',index=False)

srdf.loc[srdf['submitter_id'].isna()]
srdf.loc[srdf['imaging_studies.submitter_id'].isna()]

# srdf = srdf.loc[~srdf['submitter_id'].isna()]
# srdf = srdf.loc[~srdf['imaging_studies.submitter_id'].isna()]

failed_series = list(set(srdf['datasets.submitter_id'].tolist())) # 
while len(failed_series) > 0:
    dids = failed_series # restart
    failed_series = []
    for i in range(0,len(dids)):
        did = dids[i]
        pid = 'MIDRC-' + did
        print(f"\n\n\ni: {i} ({i+1}/{len(dids)}): {pid}")
        df = copy.deepcopy(srdf[srdf['datasets.submitter_id'] == did])
        df = df.drop_duplicates(subset='submitter_id')
        # remove any non utf-8 characters from SeriesDescription
        try:
            d = bsexp.submit_df(project_id = pid, df = df, chunk_size = 5000)
        except Exception as e:
            print(e)
            failed_series.append(did)
            time.sleep(30)
            continue

## Check missing collection_ids and viewer URLs
# mpids = bsexp.get_project_ids(node='program',name='MIDRC')
# stdf = bsexp.get_node_tsvs(node='imaging_study',projects=mpids)

srdf = bsexp.get_node_tsvs(node='imaging_series')
md = srdf.loc[srdf['dicom_viewer_url']=='https://data.midrc.org/ohif-viewer/viewer?StudyInstanceUIDs='] # 0

