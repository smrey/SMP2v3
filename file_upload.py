import requests
import os
from parse_sample_sheet import ParseSampleSheet

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

class FileUpload:

    def __init__(self, auth):
        self.authorise = auth
        self.project_id = None


    def create_basespace_project(self, project_name):
        '''
        :param project_name: Worksheet id from the sample sheet, which will be the project name in BaseSpace
        :param authorise:
        :return:
        '''
        project_id = None
        response = requests.post(f"{v1_api}/projects", data={"name": project_name},
                                 headers={"Authorization": self.authorise},
                                 allow_redirects=True)
        if response.status_code != 200 and response.status_code != 201:
            raise Exception(f"An error occurred communicating with BaseSpace. Error code {response.status_code}")
        elif response.status_code == 200:
            print(f"project {project_name} already exists and is writeable")
            project_id = response.json().get("Response").get("Id")
        elif response.status_code == 201:
            print(f"project {project_name} successfully created")
            project_id = response.json().get("Response").get("Id")
        # Update project id inside object
        self.project_id = project_id
        return project_id


    def make_sample(self, file_to_upload, sample_number):
        file_identifier = None
        file_name = os.path.basename(file_to_upload)
        print(file_name)
        url = f"{v1_api}/projects/{self.project_id}/samples"
        data = {"Name": file_name, "SampleId": file_name, "SampleNumber": sample_number} # more metadata update after u/l, "Read1": "1", "IsPairedEnd": "false"}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=data, allow_redirects=True)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 201:
            raise Exception(f"An error occurred communicating with BaseSpace. Error code {response.status_code}")
        else:
            print(response.json())
        return file_identifier
