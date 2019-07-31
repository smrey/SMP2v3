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


def get_files_from_appresult(authorise, appresultid):
    url = v1_api + "/appresults/" + appresultid + "/files/"
    p = {"Extensions": ".bam"}
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200:  # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        print(response.json())
    return None


def get_file_id(authorise):
    return None


def download_file(authorise, file_id, download_directory):
    if not os.path.exists(download_directory):
        print("create directory")

    url = v1_api + "/applications/" + file_id + "/content/"
    p = {"redirect": "true"} #this may need to be set to meta
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200:  # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        print(response.json())
    return None


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    # There will be one appresult per sample

    get_files_from_appresult(auth, "42892893")
    #file_id = get_file_id(auth)
    #download_file(auth, file_id, dl_location)


if __name__ == '__main__':
    main()