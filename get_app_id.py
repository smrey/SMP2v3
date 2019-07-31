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


def get_applications(authorise):
    applications = []
    url = v2_api + "/applications/"
    p = {"category": "Native", "limit": "150"}
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head, allow_redirects=True)
    print(response.request.headers)
    print(response.url)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        print(response.json().get("Items"))
        for i in (response.json().get("Items")):
            applications.append(i.get("Name"))
            print(i.get("Name"))
    return applications


def get_application(authorise, application_name):
    application = f"No application with name {application_name} found"
    url = v2_api + "/applications/"
    p = {"category": "Native", "limit": "250"}
    head = {"Authorization": authorise}
    response = requests.get(url, params=p, headers=head, allow_redirects=True)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        for i in (response.json().get("Items")):
            if (i.get("Name") == application_name):
                application = (f"Application {i.get('Name')} has ID {i.get('Id')}")
    return application


def get_app_info(authorise, app_id):
    application = f"No application with name {app_id} found"
    url = v2_api + "/applications/" + app_id
    head = {"Authorization": authorise}
    response = requests.get(url, headers=head, allow_redirects=True)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        application = response.json()
    return application


def get_app_settings(authorise, app_id):
    application = f"No application with name {app_id} found"
    url = v2_api + "/applications/" + app_id + "/settings/"
    head = {"Authorization": authorise}
    response = requests.get(url, headers=head, allow_redirects=True)
    if response.status_code != 200: # and response.status_code != 201:
        print("error")
        print(response.status_code)
        print(response)
    else:
        application = response.json()
    return application


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    #get_applications(auth)
    print(get_application(auth, "TruSight Tumor 170"))
    print(get_app_info(auth, "6132126"))
    #print(get_app_settings(auth, "6132126")) #unavailable (as this is not my app?)


if __name__ == '__main__':
    main()