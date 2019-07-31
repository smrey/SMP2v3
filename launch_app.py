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

def generate_app_parameters(pth):
    with open(pth + "app.config.template.json") as app_config_file:
        try:
            app_config = json.load(app_config_file)
            print(app_config)
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
    return None

def launch_application(authorise, app_parameters):
    url = v2_api + "/applications/"
    p = {app_parameters}
    head = {"Authorization": authorise, "Content-Type": "application/json"}
    response = requests.post(url, params=p, headers=head, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        print(response.json().get("Items"))
        for i in (response.json().get("Items")):
            print(i.get("Name"))
    return None




def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    #launch_application(auth)
    generate_app_parameters(config_file_pth)

if __name__ == '__main__':
    main()