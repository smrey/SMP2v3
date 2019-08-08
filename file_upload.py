import pandas as pd
import glob
import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1
# Assumes IlluminaQC script has run- change path if demuxed fastqs will be located elsewhere
fastq_location = "/Users/sararey/Documents/cruk_test_data/rawFQs/"
config_file_pth = "/Users/sararey/PycharmProjects/CRUK/"


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
        try:
            config_json = json.load(config_file)
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
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


# Makes an empty biosample to put files into
def make_sample(file_to_upload, project_id, authorise):
    file_name = os.path.basename(file_to_upload)
    print(file_name)
    url = f"{v1_api}/projects/{project_id}/samples"
    data = {"Name": "Ki", "SampleId": "Ki", "SampleNumber": "8", "Read1": "1", "IsPairedEnd": "false"}
    head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": authorise, "User-Agent": "/python-requests/2.22.0"}
    response = requests.post(url, headers=head, data=data, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response.json())
    else:
        print(response.json())
    return None


def make_file(file_to_upload, sample_id, authorise):
    file_name = os.path.basename(file_to_upload)
    print(file_name)
    url = f"{v1_api}/samples/{sample_id}/files"
    data = {"name": "filename"}
    p = {"name": "filename.fastq.gz", "multipart": "true"}
    head = {"Content-Type": "json/application", "Authorization": authorise, "User-Agent": "/python-requests/2.22.0"}
    response = requests.post(url, headers=head, data=data, params=p, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response.json())
    else:
        print(response.json())
    return None


def retrieve_sample_info(sample_id, authorise):
    url = f"{v1_api}/samples/{sample_id}"
    head = {"Authorization": authorise}
    response = requests.get(url, headers=head, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200:
        print("error")
        print(response.status_code)
        print(response.json())
    else:
        print(response.json())
    return None


def upload_into_file(upload_file, file_id, authorisation):
    part_num = "1"
    url = f"{v1_api}/files/{file_id}/parts/{part_num}"
    # Note can put MD5 checksum in header- Content-MD5??
    head = {"Authorization": authorisation}
    file = {"fn": open(upload_file, 'rb')}
    response = requests.put(url, headers=head, files=file, allow_redirects=True)
    print(response.json())
    return None


def retrieve_file_info(file_id, authorise):
    url = f"{v1_api}/files/{file_id}"
    head = {"Authorization": authorise}
    response = requests.get(url, headers=head, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200:
        print("error")
        print(response.status_code)
        print(response.json())
    else:
        print(response.json())
    return None


def set_file_upload_status(file_id, file_status, authorisation):
    url = f"{v1_api}/files/{file_id}"
    p = {"uploadstatus": file_status}
    head = {"Authorization": authorisation}
    response = requests.post(url, headers=head, params=p, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response.json())
    else:
        print(response.json())
    return None



def main():
    # Parse sample sheet to extract relevant sample information
    #parsed_sample_sheet = read_in_sample_sheet(ss_location)

    # Pull out a series of samples to upload to BaseSpace
    #samples_to_upload = identify_samples(parsed_sample_sheet)

    # Identify the worksheet number which will be used as the project name in BaseSpace
    #worksheet = identify_worksheet(parsed_sample_sheet)

    # Locate the fastqs associated with each sample
    #fastqs = locate_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken") #TODO This could be better as a global variable?

    # Create project and return BaseSpace project identifier
    ##project = create_basespace_project(worksheet, auth) # Note can save results to a different project through the gui

    #make_sample("placeholder", "138381252", auth)
    #make_file()

    #find file made
    retrieve_sample_info("274828624", auth) # does not appear in gui- sample status is aborted rather than complete
    retrieve_sample_info("273269056", auth) # appears in gui
    #make_file("/Users/sararey/Documents/cruk_test_data/rawFQs/NTC_S24_L001_R1_001.fastq.gz", "274828624", auth)
    retrieve_file_info("15805554012", auth)
    #upload_into_file("/Users/sararey/Documents/cruk_test_data/rawFQs/NTC_S24_L001_R1_001.fastq.gz", "15805554012", auth)
    #set_file_upload_status("15805554012", "complete", auth)
    retrieve_file_info("273269056", auth)





if __name__ == '__main__':
        main()