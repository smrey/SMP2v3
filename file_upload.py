import requests
import os

v1_api = "https://api.basespace.illumina.com/v1pre3"
v2_api = "https://api.basespace.illumina.com/v2"

class FileUpload:

    def __init__(self, auth, project_name):
        self.authorise = auth
        self.project_name = project_name
        self.project_id = None


    def create_basespace_project(self):
        '''
        :param project_name: Worksheet id from the sample sheet, which will be the project name in BaseSpace
        :param authorise:
        :return:
        '''
        response = requests.post(f"{v1_api}/projects", data={"name": self.project_name},
                                 headers={"Authorization": self.authorise},
                                 allow_redirects=True)
        if response.status_code != 200 and response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        elif response.status_code == 200:
            print(f"Project {self.project_name} already exists and is writeable")
            # Update project id inside object
            self.project_id = response.json().get("Response").get("Id")
        elif response.status_code == 201:
            print(f"Project {self.project_name} successfully created")
            # Update project id inside object
            self.project_id = response.json().get("Response").get("Id")
        return self.project_id


    def make_sample(self, file_to_upload, sample_number):
        sample_metadata = {}
        file_name = os.path.basename(file_to_upload)
        url = f"{v1_api}/projects/{self.project_id}/samples"
        data = {"Name": f"{self.project_name}-{file_name}", "SampleId": f"{self.project_name}-{file_name}",
                "SampleNumber": sample_number, "Read1": "1", "Read2": "2", "IsPairedEnd": "true"} #TODO Read1 and Read2 parameters are the read lengths
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=data, allow_redirects=True)
        if response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        sample_metadata["sample_id"] = response.json().get("Response").get("Id")
        sample_metadata["appsession_id"] = response.json().get("Response").get("AppSession").get("Id")
        return sample_metadata


    def make_file(self, file_to_upload, sample_id):
        file_name = os.path.basename(file_to_upload)
        url = f"{v1_api}/samples/{sample_id}/files"
        p = {"name": file_name, "multipart": "true"}
        head = {"Content-Type": "json/application", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, params=p, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Response").get("Id")


    def upload_into_file(self, upload_file, file_id, file_chunk_num, hash):
        part_num = file_chunk_num
        url = f"{v1_api}/files/{file_id}/parts/{part_num}"
        head = {"Authorization": self.authorise, "Content-MD5": hash}
        file = {"fn": open(upload_file, 'rb')}
        response = requests.put(url, headers=head, files=file, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return response.json().get("Response").get("ETag")


    def set_file_upload_status(self, file_id, file_status):
        url = f"{v1_api}/files/{file_id}"
        p = {"uploadstatus": file_status}
        head = {"Authorization": self.authorise}
        response = requests.post(url, headers=head, params=p, allow_redirects=True)
        if response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return print(f"{response.json().get('Response').get('Name')} set to status {file_status}")


    def finalise_appsession(self, appsession_id, file_name):
        url = f"{v2_api}/appsessions/{appsession_id}"
        d = {"ExecutionStatus": "Complete", "StatusSummary": f"Finished uploading {file_name}"}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=d, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return f"AppSession {appsession_id} for file {self.project_name}-{file_name} set to status Complete"
