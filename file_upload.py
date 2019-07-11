# Parse sample sheet to extract sampleid (col 1- header Sample_ID), sample name (col 2- header Sample_Name),
# worksheet id (col 3- header Sample_plate)

import pandas as pd
import glob
import requests
import json

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1
# Assumes IlluminaQC script has run- change path if demuxed fastqs will be located elsewhere
fastq_location = "/Users/sararey/Documents/cruk_test_data/rawFQs/"
config_file_pth = "/Users/sararey/PycharmProjects/CRUK/"


def read_in_sample_sheet(ss):
    '''
    :param ss: the Illumina sample sheet
    :return: a dataframe containing the sample-related information from the Illumina sample sheet
    '''
    # Locate index of header for sample information in sample sheet
    header_index = False
    with open(ss) as sample_sheet:
        for index, line in enumerate(sample_sheet):
            if line.startswith("Sample_ID"):
                header_index = index
    ss_data = pd.read_csv(ss_location, header=0, skiprows=header_index)
    # Delete empty rows- use second column to handle case where a space may be entered into the first column
    ss_data_nona = ss_data.dropna(subset=["Sample_Name"])
    return ss_data_nona


def identify_samples(ss_df):
    # Extract sample identifiers (column 1 of sample sheet)
    sample_ids = ss_df["Sample_ID"] # Select samples as a series object
    return sample_ids


def identify_worksheet(ss_df):
    '''
    :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
    :return: a string of the worksheet identifier for the run
    '''
    worksheet_id = ss_df["Sample_Plate"].unique().tolist()[0] # Only one entry in list if there is one worksheet- assumed
    return worksheet_id


def locate_fastqs(samples, fq_loc):
    '''
    :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
    :param fq_loc:
    :return:
    '''
    # Create dictionary to hold information about fastqs
    sample_fastqs_dict = {}
    # Iterate over all sample identifiers
    for index_sample in samples.iteritems():
        sample = index_sample[1] # row labels not required, data in first column of series
        # Create list of all fastqs matching sample id- all for upload into <sample>- pre-requisite to app launch
        sample_fastqs_list = (glob.glob(fq_loc + sample + '*'))
        sample_fastqs_dict[sample] = sample_fastqs_list
    return sample_fastqs_dict


def load_config_file(pth):
    '''
    :param pth: path to the location of the config file (usually location of the code)
    :return:
    '''
    with open(pth + "bs.config.json") as config_file:
        config_json = json.load(config_file)
    return config_json


def create_basespace_project(project_name, authorise):
    '''
    :param project_name: Worksheet id from the sample sheet, which will be the project name in BaseSpace
    :param authorise:
    :return:
    '''
    project_id = None
    response = requests.post(v1_api + "/projects", data={"name": project_name},
                            headers={"Authorization": authorise},
                            allow_redirects=True)
    # print(response.headers.get('content-type'))
    if response.status_code != 200 and response.status_code != 201:
        print("error")
        print(response.status_code)
    elif response.status_code == 200:
        print(f"project {project_name} already exists and is writeable")
        project_id = response.json().get("Response").get("Id")
    elif response.status_code == 201:
        print(f"project {project_name} successfully created")
        project_id = response.json().get("Response").get("Id")
    return project_id


def create_appresults(samples, worksheet, proj_id, authorise):
    '''
    :param samples:
    :param worksheet:
    :param proj_id:
    :param authorise:
    :return:
    '''
    appresults_dict = {}
    for index_sample in samples.iteritems():
        sample = index_sample[1]
        app_name = f"{sample}_{worksheet}" # row labels not required, data in first column of series
        appresult_id = create_an_appresult(app_name, proj_id, authorise)
        appresults_dict[app_name] = appresult_id
    return appresults_dict


def create_an_appresult(appresult_name, proj_id, authorise):
    '''
    :param appresult_name:
    :param proj_id:
    :param authorise:
    :return:
    '''
    appresult_id = None
    response = requests.post(v1_api + "/projects/" + proj_id + "/appresults", data={"Name": appresult_name},
                            headers={"Authorization": authorise},
                            allow_redirects=True)
    if response.status_code != 201:
        print("error")
        print(response.status_code)
    elif response.status_code == 201:
        print(f"appresult {appresult_name} successfully created")
        appresult_id = response.json().get("Response").get("Id")
    return appresult_id


def file_upload(appresults, files, authorise):
    # Several files per sample, one sample per appresult, several appresults
    for sample_worksheet in appresults.keys():
        appid = appresults.get(sample_worksheet)
        files_per_sample = files.get(sample_worksheet.split("_")[0]) # Key is sample name without worksheet id appended
        upload_fastqs_to_basespace(files_per_sample, appid, authorise)
    return None


def upload_fastqs_to_basespace(all_fastqs_per_appresult, appresult_id, authorise):
    #Iterate over multiple files per appresult (all fastqs per sample)
    print(appresult_id)
    for fastq in all_fastqs_per_appresult:
        print(fastq)
        initiate_upload(fastq, appresult_id, authorise)
    return None


def initiate_upload(file_to_upload, appresult_id, authorise):
    # WORKING HERE- SPLIT FILE INTO MULTIPART AND ADD MD5
    response = requests.post(v1_api + "/appresults/" + appresult_id, data={"name": file_to_upload, "multipart": "true"},
                             headers={"Authorization": authorise},
                             allow_redirects=True)
    if response.status_code != 200 and response.status_code != 201:
        print("error")
        print(response.status_code)
    else:
        print(response.json())
    return None


def main():
    # Parse sample sheet to extract relevant sample information
    parsed_sample_sheet = read_in_sample_sheet(ss_location)

    # Pull out a series of samples to upload to BaseSpace
    samples_to_upload = identify_samples(parsed_sample_sheet)

    # Identify the worksheet number which will be used as the project name in BaseSpace
    worksheet = identify_worksheet(parsed_sample_sheet)

    # Locate the fastqs associated with each sample
    fastqs = locate_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    # Create project and return BaseSpace project identifier
    project = create_basespace_project(worksheet, auth) # Note can save results to a different project through the gui

    # Create appresults to store files and return BaseSpace appresults identifier
    appresults_dictionary = create_appresults(samples_to_upload, worksheet, project, auth)

    # Upload files into appresults
    file_upload(appresults_dictionary, fastqs, auth)





if __name__ == '__main__':
        main()