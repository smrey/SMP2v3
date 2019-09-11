# authenticate to obtain access token with write permissions
import requests
import json
import time

from config import v1_api
from config import v2_api

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


def get_verification_device_code(client_id, requested_scope, resource):
    if resource != None: # Handle this better in production
        requested_scope = f"{requested_scope} {resource}"
    device_code = None
    url = "https://api.basespace.illumina.com/v1pre3/oauthv2/deviceauthorization"
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
        url = "https://api.basespace.illumina.com/v1pre3/oauthv2/token"
        p = {"client_id": config.get("clientId"), "client_secret": config.get("clientSecret"),
             "code": device_code, "grant_type":"device"}
        response = requests.post(url, params=p)
        if response.status_code != 200 and response.status_code != 400:
            print("error")
            print(response.status_code)
            print(response.text)
        elif response.status_code == 200:
            token = response.json()
            iterate = False
        else:
            print(response.json())
            time.sleep(0.7)
    return token


def update_config_file(pth, access_token):
    with open(pth + "bs.config.json") as config_file:
        try:
            config_json = json.load(config_file)
            config_json["authenticationToken"] = access_token
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
    return config_json


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    # Start here for new authentication workflow
    code = get_verification_device_code(configs.get("clientId"), "create global", None)
    token = poll_for_access_token(configs, code)

    update_config_file(config_file_pth, token)


if __name__ == '__main__':
    main()