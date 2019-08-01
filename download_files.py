import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

config_file_pth = "/Users/sararey/PycharmProjects/CRUK/"
dl_location = "/Users/sararey/Documents/cruk_test_data/"


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


def get_files_from_appresult(authorise, appresultid, file_extensions):
    files = None
    url = f"{v1_api}/appresults/{appresultid}/files/"
    p = {"Extensions": file_extensions}
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head)
    if response.status_code != 200:
        print("error")
        print(response.status_code)
    else:
        files = response.json().get("Response")
    return files.get("Items")


def get_file_name_id(file_data_list):
    dict_of_file_ids = {}
    for f in file_data_list:
        dict_of_file_ids[f.get("Name")] = (f.get("Id"))
    return dict_of_file_ids


def download_files(authorise, files_to_dl, dl_to, appresults_id):
    file_success = []
    for fn, dl in files_to_dl.items():
        file_success.append(download_file(authorise, fn, dl, dl_to))
    return f"All files, {[x for x in file_success]} from appresult, {appresults_id}, successfully downloaded"


def download_file(authorise, file_name, file_id, download_directory):
    file_downloaded = f"File {file_name} not completed download"
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
    #TODO multipart download if required- chunk file and reassemble
    url = f"{v1_api}/files/{file_id}/content/"
    p = {"redirect": "true"} #this may need to be set to meta
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head)
    if response.status_code != 200:
        raise Exception(f"Error response from API: {response.status_code}")
    else:
        with open(os.path.join(dl_location, file_name), 'wb') as wf:
            for chunk in response:
                wf.write(chunk)
            file_downloaded = f"File {file_name} downloaded"
    print(file_downloaded)
    return file_name


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    required_file_extensions = ".vcf" #,.bam,.bai,.xlsx"

    #TODO Where there are multiple appresults, iterate through this and make final message about all files
    file_dict = get_files_from_appresult(auth, "42892893",  required_file_extensions)
    file_id = get_file_name_id(file_dict)
    print(download_files(auth, file_id, dl_location, "42892893"))


if __name__ == '__main__':
    main()