import requests
import json
import os
import datetime

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

class LaunchApp:

    def __init__(self, auth, project_id, app_name, app_version):
        self.authorise = auth
        self.project_id = project_id
        self.app_name = app_name
        self.app_version = app_version
        self.app_group_id = None
        self.app_id = None


    def get_biosamples(self, biosample_name):
        url = v2_api + "/biosamples"
        p = {"biosamplename": biosample_name, "projectid": self.project_id}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Items")[0].get("Id") #TODO Make this more robust


    def generate_app_config(self, pth, dna_biosample_ids, rna_biosample_ids):
        # Generate list of biosamples in correct format for application launch
        dna_list = []
        for dna_biosample_id in dna_biosample_ids:
            dna_libraryprep_id = self.get_biosample_info(dna_biosample_id)
            dna_list.append(f"biosamples/{dna_biosample_id}/librarypreps/{dna_libraryprep_id}")
        rna_list = []
        for rna_biosample_id in rna_biosample_ids:
            rna_libraryprep_id = self.get_biosample_info(rna_biosample_id)
            rna_list.append(f"biosamples/{rna_biosample_id}/librarypreps/{rna_libraryprep_id}")
        # Obtain date and time
        current_time = datetime.datetime.now()
        # remove seconds from date and time and create string
        date_time = ":".join(str(current_time).split(":")[:-1])
        with open(os.path.join(pth, "app.config.template.json")) as app_config_file:
            try:
                app_config = json.load(app_config_file)
                input = app_config.get("InputParameters")
                input["dna-sample-id"] = dna_list
                input["project-id"] = f"projects/{self.project_id}"
                input["rna-sample-id"] = rna_list
                app_config["Name"] = f"TruSight Tumour 170 {date_time}"
            except json.decoder.JSONDecodeError:
                raise Exception("Config file is incorrectly formatted and does not contain valid json")
        return app_config


    def get_app_group_id(self):
        url = v1_api + "/applications"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
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
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            for i in response.json().get("Response").get("Items"):
                if i.get("VersionNumber") == self.app_version:
                    self.app_id = i.get("Id")
        return self.app_id


    def get_app_form(self):
        url = v1_api + "/applications/" + self.app_id + "/assets/forms"
        p = {"status":"active"}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            app_form_id = response.json().get("Response").get("Items")[0] #TODO Set this line raise exception if >1
        return str(app_form_id.get("Id"))


    def get_app_form_items(self, app_form_id):
        url = v1_api + "/applications/" + self.app_id + "/assets/Forms/" + app_form_id + "/items"
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            response = response.json()
        return response


    def getter(self):
        url = v1_api + "/applications/6132126/assets/Forms/17926910/items/20293275/content"
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            print(response.text)
        return None


    def get_biosample_info(self, biosample_id):
        url = v2_api + "/biosamples/" + biosample_id + "/libraries"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Items")[0].get("LibraryPrep").get("Id") #TODO Set this line raise exception if >1


    def launch_application(self, app_conf):
        url = v2_api + "/applications/" + self.app_id + "/launch/"
        d = json.dumps(app_conf)
        head = {"Content-Type": "application/json", "Authorization": self.authorise}
        response = requests.post(url, headers=head, data=d)
        if response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Id")
