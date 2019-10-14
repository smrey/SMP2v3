import requests
import json
import os
import datetime
from config import v1_api
from config import v2_api

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
        if len(response.json().get("Items")) > 1:
            raise Exception(f"Multiple biosamples with name {biosample_name} in project {self.project_id}")
        if len(response.json().get("Items")) < 1:
            raise Exception(f"No biosample with name {biosample_name} in project {self.project_id}")
        try:
            response.json().get("Items")[0]
        except:
            raise Exception(f"Problem with finding biosample data in BaseSpace: {response.json()}")
        return response.json().get("Items")[0].get("Id")

    def generate_app_config(self, pth, dna_biosample_id, rna_biosample_id):
        # Generate biosamples in correct format for application launch
        dna_libraryprep_id = self.get_biosample_info(dna_biosample_id)
        dna_config = f"biosamples/{dna_biosample_id}/librarypreps/{dna_libraryprep_id}"
        rna_libraryprep_id = self.get_biosample_info(rna_biosample_id)
        rna_config = f"biosamples/{rna_biosample_id}/librarypreps/{rna_libraryprep_id}"
        # Obtain date and time
        current_time = datetime.datetime.now()
        # remove seconds from date and time and create string
        date_time = ":".join(str(current_time).split(":")[:-1])
        with open(os.path.join(pth, "app.config.template.json")) as app_config_file:
            try:
                app_config = json.load(app_config_file)
                input = app_config.get("InputParameters")
                input["dna-sample-id"] = dna_config
                input["project-id"] = f"projects/{self.project_id}"
                input["rna-sample-id"] = rna_config
                app_config["Name"] = f"TruSight Tumour 170 {date_time}"
            except json.decoder.JSONDecodeError:
                raise Exception("Config file is incorrectly formatted and does not contain valid json")
        return app_config

    def generate_smp_app_config(self, dna_dataset_id, rna_dataset_id):
        dna_config = f"datasets/{dna_dataset_id}"
        rna_config = f"datasets/{rna_dataset_id}"
        # Obtain date and time
        current_time = datetime.datetime.now()
        # remove seconds from date and time and create string
        date_time = ":".join(str(current_time).split(":")[:-1])
        with open(os.path.join("smpapp.config.template.json")) as smpapp_config_file:
            try:
                app_config = json.load(smpapp_config_file)
                input = app_config.get("InputParameters")
                input["dna-sample-id"] = dna_config
                input["project-id"] = f"projects/{self.project_id}"
                input["rna-sample-id"] = rna_config
                app_config["Name"] = f"SMP2 v3 {date_time}"
            except json.decoder.JSONDecodeError:
                raise Exception("SMP config file is incorrectly formatted and does not contain valid json")
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

    def get_biosample_info(self, biosample_id):
        url = v2_api + "/biosamples/" + biosample_id + "/libraries"
        p = {"Limit":200, "Offset":0}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        if len(response.json().get("Items")) > 1:
            raise Exception(f"More than one library for biosample {biosample_id}")
        return response.json().get("Items")[0].get("LibraryPrep").get("Id")

    def get_datasets(self, appsession_id, biosample_id):
        dataset_id = ""
        url = v2_api + "/datasets"
        p = {"limit": "50", "inputbiosamples": biosample_id, "datasettypes": "common.files",
             "include": "AppSessionRoot"}
        head = {"Authorization": self.authorise, "Content-Type": "application/json"}
        response = requests.get(url, headers=head, params=p)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            if len(response.json().get("Items")) > 1:
                for dataset in (response.json().get("Items")):
                    if dataset.get("AppSession").get("AppSessionRoot").get("Id") == appsession_id:
                        dataset_id = dataset.get("Id")
                    else:
                        raise Exception(f"Could not determine dataset id required to launch SMP2 app.")
            else:
                dataset_id = response.json().get("Items")[0].get("Id")
        return dataset_id

    def launch_application(self, app_conf):
        url = v2_api + "/applications/" + self.app_id + "/launch/"
        d = json.dumps(app_conf)
        head = {"Content-Type": "application/json", "Authorization": self.authorise}
        response = requests.post(url, headers=head, data=d)
        if response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Id")
