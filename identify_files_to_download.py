import requests
from config import v1_api
from config import v2_api

class IdentifyFiles:

    def __init__(self, appresultid, file_extensions, auth):
        self.appresultid = appresultid
        self.file_extensions = file_extensions
        self.authorise = auth
        self.files = {}


    def  get_files_from_appresult(self):
        url = f"{v1_api}/appresults/{self.appresultid}/files/"
        p = {"Extensions": self.file_extensions}
        head = {"Authorization": self.authorise}
        response = requests.get(url, params=p, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        else:
            self.files = response.json().get("Response").get("Items")
        return self.files


    def get_file_name_id(self):
        dict_of_file_ids = {}
        for f in self.files:
            dict_of_file_ids[f.get("Name")] = (f.get("Id"))
        return dict_of_file_ids