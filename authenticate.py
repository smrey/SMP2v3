# authenticate to obtain access token with write permissions
import requests
import json
import os

from config import v1_api
from config import v2_api

config_file_path = "/data/diagnostics/pipelines/CRUK/CRUK-2.0.0/"


def load_config_file(pth):
    '''
    :param pth: path to the location of the config file (usually location of the code)
    :return:
    '''
    with open(os.path.join(pth, "bs.config.json")) as config_file:
        try:
            config_json = json.load(config_file)
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
    return config_json


def get_verification_device_code(client_id, requested_scope):
    #if resource != None: # Handle this better in production
        #requested_scope = f"{requested_scope} {resource}"
    device_code = None
    url = f"{v1_api}/oauthv2/deviceauthorization"
    p = {"client_id": client_id, "response_type": "device_code", "scope": requested_scope}
    response = requests.post(url, params=p)
    if response.status_code != 200:
        print("error")
        print(response.status_code)
        print(response.text)
    else:
        print(response.json())
        link = response.json().get("verification_with_code_uri")
        print(f"Please open this link and authorise the app: {link}")
        device_code = response.json().get("device_code")
    return device_code


def poll_for_access_token(config, device_code):
    token = None
    iterate = True
    while iterate:
        url = f"{v1_api}/oauthv2/token"
        p = {"client_id": config.get("clientId"), "client_secret": config.get("clientSecret"),
             "code": device_code, "grant_type":"device"}
        response = requests.post(url, params=p)
        if response.status_code != 200 and response.status_code != 400:
            print("error")
            print(response.status_code)
            print(response.text)
            iterate = False
        elif response.status_code == 200:
            token = response.json()
            iterate = False
        else:
            print(response.json())
            time.sleep(0.7)
    return token


def update_config_file(pth, access_token):
    with open(os.path.join(pth, "bs.config.json"), 'r') as config_file:
        try:
            config_json = json.load(config_file)
            print(config_json)
            config_json["authenticationToken"] = access_token.get("access_token")
            print(config_json)
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
    with open(os.path.join(pth, "bs.config.json"), 'w') as config_file_write:
        try:
            json.dump(config_json, config_file_write)
        except:
            raise Exception("JSON file write failed")
    return config_json


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_path)
    auth = 'Bearer ' + configs.get("authenticationToken")

    # Start here for new authentication workflow
    code = get_verification_device_code(configs.get("clientId"),
                                        "browse global, create global, create projects, start applications")
    token = poll_for_access_token(configs, code)

    update_config_file(config_file_path, token)


if __name__ == '__main__':
    main()
