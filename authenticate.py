# authenticate to obtain access token with write permissions
import requests
import json

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
    files = None
    url = f"{v1_api}/oauthv2/deviceauthorization"
    p = {"client_id": client_id, "response_type": "device_code", "scope": requested_scope}
    response = requests.post(url, params=p)
    print(response.url)
    if response.status_code != 200:
        print("error")
        print(response.status_code)
        print(response.text)
    else:
        files = response.json()
    return files


def main():
    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken")

    # Start here for new authentication workflow
    print(get_verification_device_code(configs.get("clientId"), "create global", None))


if __name__ == '__main__':
    main()