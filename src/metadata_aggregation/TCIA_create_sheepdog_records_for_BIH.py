import pandas as pd
from tcia_utils import nbia
from tcia_utils import wordpress
import datetime
import sys
import gen3
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.query import Gen3Query
pd.set_option('display.max_rows', None)

git_dir='/Users/christopher/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion
%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py

######################## BIH
api = 'https://imaging-hub.data-commons.org'
cred = '/Users/christopher/Downloads/bih-credentials.json'
auth = Gen3Auth(api, refresh_file=cred)
sub = Gen3Submission(api, auth)
query = Gen3Query(auth)
index = Gen3Index(auth)
exp = Gen3Expansion(api,auth,sub)
exp.get_project_ids()

######################## BIH
bsapi = 'https://bihstaging.data-commons.org'
bscred = '/Users/christopher/Downloads/bih-staging-credentials.json'
bsauth = Gen3Auth(bsapi, refresh_file=bscred)
bssub = Gen3Submission(bsapi, bsauth)
bsquery = Gen3Query(bsauth)
bsindex = Gen3Index(bsauth)
bsexp = Gen3Expansion(bsapi,bsauth,bssub)
tpids = sorted(bsexp.get_project_ids(node='program',name='TCIA'))
# ######################## QA BIH
# qapi = 'https://qa-bih.planx-pla.net/'
# qcred = '/Users/christopher/Downloads/qa-bih-credentials.json'
# qauth = Gen3Auth(qapi, refresh_file=qcred)
# qsub = Gen3Submission(qapi, qauth)
# qquery = Gen3Query(qauth)
# qindex = Gen3Index(qauth)
# qexp = Gen3Expansion(qapi,qauth,qsub)


tdir = ("/Users/christopher/Documents/Notes/BIH/TCIA")
os.chdir(idir)

"""
BIH dd properties in ETL mapping:
https://github.com/uc-cdis/cdis-manifest/blob/master/bihstaging.data-commons.org/etlMapping.yaml

dataset:
  collection_id
  commons_long_name
  commons_name
  data_contributor
  data_host
  data_url_doi
  disease_type
  license
  metadata_source_api
  metadata_source_version
  primary_site

subject:
    race

imaging_study:
    StudyDescription
    StudyInstanceUID
    PatientAge
    PatientSex
    PatientID
    EthnicGroup

imaging_series:
    submitter_id
    object_ids (PIDs of image files)
    BodyPartExamined
    Manufacturer
    Modality
    SeriesDescription
    SeriesInstanceUID
    dicom_viewer_url


Required fields:
    Series ID (index: a unique identifier for imaging series, ideally the DICOM SeriesInstanceUID)
    Study ID (index for imaging studies; ideally the DICOM StudyInstanceUID)
    Patient ID (index for subjects: ideally the DICOM PatientID)
    Collection id (unique ID of the dataset)
    commons_long_name (host platform long name)
    commons_name (host platform abbreviated name)
    data_contributor (host platform contributing the metadata / hosting the files)
    license (dataset usage license)
    Modality

Preferred fields:
    StudyDescription
    SeriesDescription
    PatientAge
    PatientSex    
    EthnicGroup
    BodyPartExamined
    Manufacturer
    disease_type
    primary_site (site of disease)
    data_url_doi (a URL or a DOI where users will go to find the dataset)

Other optional fields:
    Subject Race
    object_ids (PIDs of image files)
    dicom_viewer_url (url where users can view images)
"""
##################################################################
### Create program
prog = "TCIA"
prog_txt = """{
    "dbgap_accession_number": "%s",
    "name": "%s",
    "type": "program"
}""" % (prog,prog)
prog_json = json.loads(prog_txt)
data = bssub.create_program(json=prog_json)


##################################################################
### Create project
### Use notebook to get data
### https://github.com/kirbyju/TCIA_Notebooks/blob/main/TCIA_Radiology_Inventory.ipynb
nbia.getToken() # interactive, enter TCIA username/password
# get all collection IDs
api_url = "restricted"
ndf = nbia.getCollections(api_url = api_url, format ="df") # this only returns "Collection" field which is the "collection_short_title"
ncids = ndf.Collection.tolist() # 128

# use "wordpress API": https://github.com/kirbyju/TCIA_Notebooks/blob/main/TCIA_Data_Curation_Learning_Lab_SIIM_2024.ipynb
"""
From Justin Kirby regarding the difference in collection count bw Wordpress and NBIA functions: 
The discrepancy you're seeing is related to 2 things:
Some collections in TCIA do not have any DICOM data.  These will not appear in any NBIA queries as we host non-DICOM file types in other systems.
Some collections contain "restricted" data that you have to request access to download.  These collections will also not appear in NBIA queries if you don't create a token (i.e. log in) first with an account that has permission to see them.
The Wordpress API will provide high-level summary metadata about all datasets regardless of access permissions and file types, but it isn't intended to provide lower-level information about specific patients or scans.  It sounds like you probably want to focus on the NBIA API if you're trying to find details about every time point or scan that exists for each patient.   
"""

# select fields to retrieve for collections
fields = ["id", "slug", "collection_page_accessibility", "link", "cancer_types",
          "collection_doi", "cancer_locations", "collection_status", "species",
          "versions", "citations", "collection_title", "version_number",
          "date_updated", "subjects", "collection_short_title", "data_types",
          "supporting_data", "program", "collection_summary", "collection_downloads"]
# "slug" is the human-readable, unique ID for a collection
# "collection_short_title" is the same, but with capitalization
#cdf = wordpress.getCollections(format = "df", fields = fields, file_name = "tciaCollections.csv", removeHtml = "yes") # subset of fields
cdf = wordpress.getCollections(format = "df", fields = None, file_name = "tciaCollections.csv", removeHtml = "yes") # gets more fields (all?)

cdf # 219
cdf.to_csv('TCIA_collections_metadata_{}.csv'.format(datetime.date.today()))
#cids = list(set(cdf['collection_short_title'].tolist()))
cids = list(set(cdf['slug'].tolist()))
prog = 'TCIA'
failed=[]
for proj in cids:
    proj_txt = """{
        "availability_type": "Open",
        "code": "%s",
        "dbgap_accession_number": "%s",
        "name": "%s"
        }""" % (proj,proj,proj)
    proj_json = json.loads(proj_txt)
    try:
        data = bssub.create_project(program=prog,json=proj_json)
    except Exception as e:
        print(e)
        failed.append(proj)
        continue
    print(data)

"""
list(cdf)
Out[56]: 
['id',
 'slug',
 'link',
 'cancer_types',
 'citations',
 'collection_doi',
 'collection_downloads',
 'versions',
 'cancer_locations',
 'collection_page_accessibility',
 'collection_status',
 'species',
 'version_number',
 'collection_title',
 'date_updated',
 'subjects',
 'collection_short_title',
 'data_types',
 'supporting_data',
 'collection_summary',
 'program']

# fields in BIH dd
dataset:
  collection_id # collection_short_title
  commons_long_name  # TCIA
  commons_name # TCIA
  data_contributor # TCIA
  data_host # TCIA
  data_url_doi # collection_doi
  disease_type # cancer_types
  license # ? 
  metadata_source_api # ?
  metadata_source_version # ?
  primary_site # ? 

"""

dataset_fields = {"collection_short_title": "collection_id",
    "collection_doi": "data_url_doi",
    "cancer_types": "disease_type",
    "cancer_locations": "primary_site",
    "slug":"submitter_id"}
ddf = copy.deepcopy(cdf[[f for f in dataset_fields if f in cdf.columns]])
ddf.rename(columns = dataset_fields, inplace = True)
# convert disease_type values from lists to strings
ddf['disease_type'] = ddf['disease_type'].apply(lambda x: ', '.join(x))
ddf['primary_site'] = ddf['primary_site'].apply(lambda x: ', '.join(x))
ddf['type'] = "dataset"
ddf["commons_long_name"] = "The Cancer Imaging Archive"
ddf["commons_name"] = "TCIA"
ddf["data_contributor"] = "TCIA"
ddf["data_host"] = "TCIA"
ddf["metadata_source_api"] = "cancerimagingarchive.net"
# version 2 of tcia_utils nbia: https://github.com/kirbyju/tcia_utils/blob/8465511158bd3e9b623d183c301c87f7b082bf79/src/tcia_utils/nbia.py#L135
ddf["metadata_source_version"] = "v2" # 
ddf["metadata_source_date"] = "2025.02" # 
ddf['projects.code'] = ddf['submitter_id']


# get leftover fields
ldf = pd.DataFrame()
for i in range(0,len(cids)):
    cid = cids[i]
    print(f"{i+1}/{len(cids)}: {cid}")
    dl_ids = cdf.loc[cdf['slug'] == cid, 'collection_downloads'].values[0]
    df = wordpress.getDownloads(ids = dl_ids, fields = ["download_title", "data_license"], format = "df")
    df['slug'] = cid
    ldf = pd.concat([ldf, df], ignore_index=True)
    #ldf = wordpress.getDownloads(ids = dl_ids, fields = None, format = "df")
    licenses = list(set(df['data_license'].tolist()))
    if len(licenses) == 1:
        license = licenses[0]
    else:
        license = ", ".join(licenses)
    print(license)
    ddf.loc[ddf['submitter_id'] == cid, 'license'] = license
bdf = copy.deepcopy(ddf) # save a backup copy
bdf.to_csv('TCIA_datasets_metadata_w_raw_licenses_{}.csv'.format(datetime.date.today()),index=False,header=True)
ldf.to_csv('TCIA_licenses_for_downloads_metadata_{}.csv'.format(datetime.date.today()),index=False,header=True)

""" Series metadata have the following LicenseURI values:
['http://creativecommons.org/licenses/by/3.0/',
 'https://creativecommons.org/licenses/by/4.0/',
 'https://creativecommons.org/licenses/by-nc/3.0/',
 'https://creativecommons.org/licenses/by-nc/4.0/']
"""
license_mapping = {'CC BY 4.0': 'https://creativecommons.org/licenses/by/4.0/',
'CC BY 3.0': 'http://creativecommons.org/licenses/by/3.0/',
'CC BY-NC 3.0': 'https://creativecommons.org/licenses/by-nc/3.0/',
'CC BY-NC 4.0': 'https://creativecommons.org/licenses/by-nc/4.0/'}

## replace license values
ddf['license'] = ddf['license'].replace(license_mapping)

## If one of the licenses is in the list, replace it with the corresponding value
for k,v in license_mapping.items():
    ddf.loc[ddf['license'].str.contains(k), 'license'] = v

dfile = 'TCIA_datasets_metadata_{}.csv'.format(datetime.date.today())
ddf.to_csv(dfile,index=False,header=True)

failed_datasets = []
projs = ddf['submitter_id'].tolist()
for i in range(0,len(projs)):
    proj = projs[i]
    print(f"{i+1}/{len(projs)}: {proj}")
    df = copy.deepcopy(ddf[ddf['submitter_id'] == proj])
    df = df.drop_duplicates()
    try:
        d = bsexp.submit_df(project_id = f'TCIA-{proj}', df = df)
    except Exception as e:
        print(e)
        failed_datasets.append(proj)
        continue

###############################################################
## Get all imaging studies
# get inventory of studies
# cids = cdf['collection_short_title'].tolist()
# for i in range(0,len(cids)):
#     cid = cids[i]
#     uids = nbia.getSimpleSearchWithModalityAndBodyPartPaged(collections=[cid], format = "uids")
#     metadata = nbia.getSeriesList(uids, include_patient_study=True)
# list(metadata)

# For all collections, run this cell
nbia.getToken() # create token
api_url = "restricted"
collections_json = nbia.getCollections(api_url = "restricted") # set API URL to include restricted collections
#rcj = nbia.getCollections(api_url = "restricted") # 128
#ocj = nbia.getCollections() # 128 , same result
c = [item['Collection'] for item in rcj]
print(str(len(collections_json)) + " collections were found.")

## c has 128, collection_ids has 219
collection_ids = cdf['collection_short_title'].tolist() # 219
set(c).difference(set(collection_ids)) # nothing 

missing = []
stdf = pd.DataFrame()
for i in range(0,len(collection_ids)):
    collection_id = collection_ids[i]
    print(f"{i+1}/{len(collection_ids)}: {collection_id}")
    st = nbia.getStudy(collection_id)
    if st is None:
        missing.append(collection_id)
        continue
    else:
        stdf = pd.concat([stdf, pd.DataFrame(st)], ignore_index=True)
stdf

# merge the ddf submitter_id to stdf based on collection_id in ddf and Collection in stdf
stdf = stdf.merge(ddf[['collection_id','submitter_id']], left_on = 'Collection', right_on = 'collection_id', how = 'left')
# stdf.loc[stdf['Collection']!=stdf['collection_id']] # None
# stdf.loc[stdf['submitter_id'].isnull()] # None


stdf.to_csv('TCIA_study_metadata_{}.csv'.format(datetime.date.today()))
"""
## TCIA study fields: 
list(stdf)
['StudyInstanceUID',
 'StudyDate',
 'StudyDescription',
 'PatientAge',
 'PatientID',
 'PatientName',
 'PatientSex',
 'Collection',
 'SeriesCount',
 'StudyID',
 'EthnicGroup',
 'AdmittingDiagnosesDescription',
 'LongitudinalTemporalEventType',
 'LongitudinalTemporalOffsetFromEvent',
 'PatientBirthDate']
 
 ## BIH imaging study fields:
imaging_study:
    StudyDescription
    StudyInstanceUID
    PatientAge
    PatientSex
    PatientID
    EthnicGroup

 """


###############################################################
## Create patient records

patient_fields = {
    "PatientID": "submitter_id",
    "submitter_id": "datasets.submitter_id",
    }
bp = copy.deepcopy(stdf[patient_fields.keys()])
bp.rename(columns = patient_fields, inplace = True)
bp['type'] = "subject"
bp.to_csv('TCIA_subjects_metadata_{}.csv'.format(datetime.date.today()),index=False,header=True)
pids = list(set(bp['datasets.submitter_id'].tolist()))
failed_patients = []
for i in range(0,len(pids)):
    pid = 'TCIA-' + pids[i]
    print(f"{i+1}/{len(pids)}: {pid}")
    df = copy.deepcopy(bp[bp['datasets.submitter_id'] == pids[i]])
    df = df.drop_duplicates()
    try:
        d = bsexp.submit_df(project_id = pid, df = df)
    except Exception as e:
        print(e)
        failed_patients.append(pid)
        continue

###############################################################
## Create imaging study records
study_fields = {"StudyInstanceUID": "StudyInstanceUID",
    "StudyDescription": "StudyDescription",
    "PatientAge": "PatientAge",
    "PatientSex": "PatientSex",
    "PatientID": "PatientID",
    "EthnicGroup": "EthnicGroup",
    "submitter_id": "datasets.submitter_id",
    }
bst = copy.deepcopy(stdf[[k for k in study_fields if k in stdf.columns]]) # bih studies df
bst.rename(columns = study_fields, inplace = True)
bst['type'] = "imaging_study"
bst['subjects.submitter_id'] = bst['PatientID']
bst['submitter_id'] = bst['StudyInstanceUID']
bst.loc[bst['StudyInstanceUID'].isnull()] # None
## convert PatientAge to integer

## Fix PatientAge; change months and days to years and remove Y, M, D, and y; change N-A to np.nan
# change N-A to np.nan
bst.loc[bst['PatientAge']=='N-A'] # 1
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: None if x == 'N-A' else x)
## Find all the PatientAge values containing M, ignore NA 
bst.loc[bst['PatientAge'].str.contains('M', na=False), 'PatientAge'] # 13
"""
2050     002M
2212     006M
2279     006M
2282     006M
2301     005M
2537     004M
2538     004M
2540     006M
2644     002M
2645     002M
2764     010M
2922     009M
42104    006M
"""

# change months to years, ignore NA
bst.loc[bst['PatientAge'].str.contains('M', na=False), 'PatientAge'].apply(lambda x: int(x.strip('M'))/12 if x != '' and x is not np.nan and 'M' in x else x).astype(int)
bst['PatientAge'] = bst['PatientAge'].astype(str)
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: int(x.strip('M'))/12 if 'M' in str(x) and x != '' and x is not np.nan and x is not None else x)
# change days to years, ignore NA
bst.loc[bst['PatientAge'].str.contains('D', na=False), 'PatientAge'].apply(lambda x: int(x.strip('D'))/365 if x != '' and x is not np.nan and 'D' in x else x).astype(int)
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: int(x.strip('D'))/365 if 'D' in str(x) and x != '' and x is not np.nan and x is not None else x)
# change years to int
bst.loc[bst['PatientAge'].str.contains('Y', na=False), 'PatientAge'].apply(lambda x: int(x.strip('Y')) if 'Y' in x and x != '' and x is not np.nan else x).astype(int)
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: int(x.strip('Y')) if 'Y' in str(x) and x != '' and x is not np.nan and x is not None else x)
bst.loc[bst['PatientAge'].str.contains('y', na=False), 'PatientAge'].apply(lambda x: int(x.strip('y')) if 'y' in x and x != '' and x is not np.nan else x).astype(int)
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: int(x.strip('y')) if 'y' in str(x) and x != '' and x is not np.nan and x is not None else x)
# convert PAtientAge nan to np.nan
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: np.nan if x is None or x == 'nan' or x == '' or x == 'None' else x)
bst['PatientAge'] = bst['PatientAge'].apply(lambda x: int(x) if x is not np.nan else x)
bst['PatientAge'] = bst['PatientAge'].astype('Int64')

bst.to_csv('TCIA_imaging_studies_metadata_{}.csv'.format(datetime.date.today()),index=False,header=True)

pids = list(set(bst['datasets.submitter_id'].tolist()))
failed_studies = []
for i in range(0,len(pids)):
    pid = 'TCIA-' + pids[i]
    print(f"\n\n{i+1}/{len(pids)}: {pid}")
    df = copy.deepcopy(bst[bst['datasets.submitter_id'] == pids[i]])
    df = df.drop_duplicates()
    #df.drop(columns = ['StudyDescription'], inplace = True) # 
    # remove any non utf-8 characters from StudyDescription
    #df['StudyDescription'] = df['StudyDescription'].str.strip("^")
    #df['StudyDescription'] = df['StudyDescription'].str.replace('^',' ')
    df['StudyDescription'] = df['StudyDescription'].str.replace('É','E')
    try:
        d = bsexp.submit_df(project_id = pid, df = df, chunk_size = 500)
    except Exception as e:
        print(e)
        failed_studies.append(pid)
        continue
    if len(d['invalid']) > 0:
        d = bsexp.submit_df(project_id = pid, df = df, chunk_size = 30)



###############################################################
## Get all imaging series
srdf = pd.DataFrame()
for cid in cids:
    seriesDescription = nbia.getSeries(cid, api_url = api_url)
    srdf = pd.concat([srdf, pd.DataFrame(seriesDescription)], ignore_index=True)


# merge the ddf submitter_id to stdf based on collection_id in ddf and Collection in stdf
srdf = srdf.merge(ddf[['collection_id','submitter_id']], left_on = 'Collection', right_on = 'collection_id', how = 'left')
srdf.to_csv('TCIA_series_metadata_{}.csv'.format(datetime.date.today()))
"""
['SeriesInstanceUID',
 'StudyInstanceUID',
 'Modality',
 'ProtocolName',
 'SeriesDate',
 'SeriesDescription',
 'BodyPartExamined',
 'SeriesNumber',
 'Collection',
 'PatientID',
 'Manufacturer',
 'ManufacturerModelName',
 'SoftwareVersions',
 'ImageCount',
 'TimeStamp',
 'LicenseName',
 'LicenseURI',
 'CollectionURI',
 'FileSize',
 'DateReleased',
 'StudyDesc',
 'StudyDate',
 'ThirdPartyAnalysis',
 'AnnotationsFlag',
 'collection_id',
 'submitter_id']

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
    "submitter_id": "datasets.submitter_id",
}


###############################################################
## Create imaging series records
bsr = copy.deepcopy(srdf[series_fields.keys()])
bsr.rename(columns = series_fields, inplace = True)
bsr['submitter_id'] = bsr['SeriesInstanceUID']
bsr['type'] = "imaging_series"

# clean up SeriesDescription special chars
bsr['SeriesDescription'] = bsr['SeriesDescription'].str.replace('OBL�QUO','OBLIQUO')
bsr['SeriesDescription'] = bsr['SeriesDescription'].str.replace('PESCOÇO','PESCOCO')
bsr['SeriesDescription'] = bsr['SeriesDescription'].str.replace('PULMÃO','PULMAO')
bsr['SeriesDescription'] = bsr['SeriesDescription'].str.replace('²','mm^2') # ADC (10^-6 mm²/s)[No-Q]:Jul 16 2020 08-17-41 EDT        1
bsr['SeriesDescription'] = bsr['SeriesDescription'].str.replace('µ','u') # AC  Chest  5.0  B08s [µ-map - Recon]                    1

# save master series metadata df
bsr.to_csv('TCIA_imaging_series_metadata_{}.csv'.format(datetime.date.today()),index=False,header=True)

# submit the metadata to sheepdog per project
datasets = list(set(bsr['datasets.submitter_id'].tolist()))
pids = ['TCIA-' + d for d in datasets]
failed_series = []
for i in range(0,len(pids)):
    pid = pids[i]
    dataset = pid.split('-',1)[1]
    print(f"\n\n{i+1}/{len(datasets)}: {pid}")
    df = copy.deepcopy(bsr[bsr['datasets.submitter_id'] == dataset])
    df = df.drop_duplicates()
    try:
        d = bsexp.submit_df(project_id = pid, df = df, chunk_size = 500)
    except Exception as e:
        print(e)
        failed_series.append(pid)
        display(df['SeriesDescription'].value_counts())
        continue

""" failed_series
pids = ['TCIA-covid-19-ny-sbu',
 'TCIA-tcga-blca',
 'TCIA-tcga-kirp',
 'TCIA-prostate-mri-us-biopsy',
 'TCIA-tcga-esca']

"""

