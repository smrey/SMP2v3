import requests
import json
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"


#project_id = "234764918" #temp
#dna_sample_id = "15642666" #temp
#rna_sample_id = "8957983" #temp

class LaunchApp:

    def __init__(self, auth, project_id, app_name, app_version):
        self.authorise = auth
        self.project_id = project_id
        self.app_name = app_name
        self.app_version = app_version
        self.app_group_id = None
        self.app_id = None


    def generate_app_config_old(self, pth, dna_sample_id, rna_sample_id):
        with open(os.path.join(pth, "app.config.template.json")) as app_config_file:
            try:
                app_config = json.load(app_config_file)
                properties_list = app_config.get("Properties")
                for p in properties_list:
                    if p.get("Name") == "Input.project-id":
                        p["Content"] = "projects/" + self.project_id
                    elif p.get("Name") == "Input.dna-sample-id":
                        p["items"] = ["biosamples/" + dna_sample_id + "/librarypreps/1014015"]
                    elif p.get("Name") == "Input.rna-sample-id":
                        p["items"] = ["v2/biosamples/" + rna_sample_id + "/librarypreps/1014015"]
            except json.decoder.JSONDecodeError:
                raise Exception("Config file does not contain valid json")
        return app_config


    def generate_app_config(self, pth, dna_sample_id, rna_sample_id):
        with open(os.path.join(pth, "app.config.template.json")) as app_config_file:
            try:
                app_config = json.load(app_config_file)
            except json.decoder.JSONDecodeError:
                raise Exception("Config file does not contain valid json")
        return app_config


    def get_app_group_id(self):
        url = v1_api + "/applications"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.json()}")
        else:
            for i in response.json().get("Response").get("Items"):
                if i.get("Name") == self.app_name:
                    self.app_group_id = i.get("Id")
        return self.app_group_id


    def get_app_id(self):
        url = v1_api + "/applications/" + self.app_group_id + "/versions"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.json()}")
        else:
            for i in response.json().get("Response").get("Items"):
                if i.get("VersionNumber") == self.app_version:
                    self.app_id = i.get("Id")
        return self.app_id


    def get_app_form(self):
        a = None
        url = v1_api + "/applications/" + self.app_id + "/assets/forms"
        p = {"status":"active"}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.json()}")
        else:
            app_form_id = response.json().get("Response").get("Items")[0] #TODO Set this line raise exception if >1
        return str(app_form_id.get("Id"))


    def get_app_form_items(self, app_form_id):
        url = v1_api + "/applications/" + self.app_id + "/assets/Forms/" + app_form_id + "/items"
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.json()}")
        else:
            print(response.json())
        return None


    def getter(self):
        url = v1_api + "/applications/6132126/assets/Forms/17926910/items/20293275/content"
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.json()}")
        else:
            print(response.text)
        return None


    def get_biosample_info(self, biosample_id):
        url = v2_api + "/biosamples/" + biosample_id + "/libraries"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 200:
            print("error")
            print(response.status_code)
            print(response)
        else:
            print(response.json().get("Items"))
            for i in (response.json().get("Items")):
                print(i.get("Name"))
        return response.json().get("Items")[0].get("LibraryPrep").get("Id") #TODO Set this line raise exception if >1


    def launch_application(self, app_conf):
        url = v2_api + "/applications/" + self.app_id + "/launch/"
        print(type(json.dumps(app_conf)))
        d = {json.dumps(app_conf)}
        '''
        d = {"AutoStart": True, "InputParameters":{"app-session-name":"Example [LocalDateTime]",
                                                     "dna-sample-id":"biosamples/198193717/librarypreps/1014015",
                                                     "project-id":"projects/140106975",
                                                     "rna-sample-id":"biosamples/198263112/librarypreps/1014015"},
                                                     "Name": "TST Test",
                                                     "StatusSummary":"Test Launch"}
        '''
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.post(url, headers=head, data=d)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 200:
            print("error")
            print(response.status_code)
            print(response.text)
        else:
            print(response.json().get("Items"))
            for i in (response.json().get("Items")):
                print(i.get("Name"))
        return None
