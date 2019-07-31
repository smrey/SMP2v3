import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

config_file_pth = "/Users/sararey/PycharmProjects/CRUK/"


project_id = "234764918" #temp
dna_sample_id = "15642666" #temp
rna_sample_id = "8957983" #temp

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


def generate_app_config(pth, project_id):
    with open(pth + "app.config.template.json") as app_config_file:
        try:
            app_config = json.load(app_config_file)
            properties_list = app_config.get("Properties")
            for p in properties_list:
                if p.get("Name") == "Input.project-id":
                    p["Content"] = "v1pre3/projects/" + project_id
                elif p.get("Name") == "Input.dna-sample-id":
                    p["items"] = ["v2/biosamples/" + dna_sample_id]
                elif p.get("Name") == "Input.rna-sample-id":
                    p["items"] = ["v2/biosamples/" + rna_sample_id]
        except json.decoder.JSONDecodeError:
            raise Exception("Config file does not contain valid json")
    return app_config


def launch_application(authorise, app_conf, app_id):
    url = v1_api + "/applications/" + app_id + "/appsessions/"
    print(type(json.dumps(app_conf)))
    d = {json.dumps(app_conf)}
    head = {"Authorization": authorise, "Content-Type": "application/json"}
    response = requests.post(url, headers=head, data=d)
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


    app_config = generate_app_config(config_file_pth, project_id)
    launch_application(auth, app_config, "6132126")


if __name__ == '__main__':
    main()