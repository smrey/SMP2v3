import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

config_file_pth = "/Users/sararey/PycharmProjects/CRUK/"

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


def get_appresults(authorise, project_id):
    url = v1_api + "/projects/" + project_id + "/appresults/"
    p = {}
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

    get_appresults(auth, "32881883")


if __name__ == '__main__':
    main()