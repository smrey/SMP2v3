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


def identify_worksheet(ss_df):
    '''
    :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
    :return: a string of the worksheet identifier for the run
    '''
    worksheet_id = ss_df["Sample_Plate"].unique().tolist()[0]
    return worksheet_id



def locate_fastqs(ss_df, fq_loc):
    '''
    :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
    :param fq_loc:
    :return:
    '''
    print(ss_df)
    # Create dictionary to hold information about fastqs
    sample_fastqs_dict = {}
    # Extract sample identifier (column 1 of sample sheet)
    sample_ids = ss_df["Sample_ID"] # Select as a series object
    for index_sample in sample_ids.iteritems():
        sample = index_sample[1] # row labels not required
        print(sample)
        # Create list of all fastqs matching sample id- all for upload into <sample>- pre-requisite to app launch
        sample_fastqs_list = (glob.glob(fq_loc + sample + '*'))
        print(sample_fastqs_list)
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


def create_basespace_project(project_name, conf):
    auth = 'Bearer ' + conf.get("authenticationToken")
    response = requests.get(v2_api + "/projects/32881883/datasets?Limit=50",
                            headers={"Authorization": auth},
                            allow_redirects=True)
    # print(response.headers.get('content-type'))
    if response.status_code != 200:
        print("error")
        print(response.status_code)
    else:
        return None


def upload_fastqs_to_basespace():
    return None


def main():
    parsed_sample_sheet = read_in_sample_sheet(ss_location)
    worksheet = identify_worksheet(parsed_sample_sheet)
    fastqs = locate_fastqs(parsed_sample_sheet, fastq_location)
    configs = load_config_file(config_file_pth)
    create_basespace_project(worksheet, configs) # Note can save results to a different project through the gui



if __name__ == '__main__':
        main()