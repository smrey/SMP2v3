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
        sample_metadata = {}
        appsession_id = None
        sample_identifier = None
        file_name = os.path.basename(file_to_upload)
        print(file_name)
        url = f"{v1_api}/projects/{self.project_id}/samples"
        data = {"Name": file_name, "SampleId": file_name, "SampleNumber": sample_number,
                "Read1": "1", "Read2": "2", "IsPairedEnd": "true"} #TODO What are the read1 and read2 parameters for?
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=data, allow_redirects=True)
        if response.status_code != 201:
            raise Exception(f"An error occurred communicating with BaseSpace. Error code {response.status_code}")
        else:
            print(response.json())
            sample_identifier = response.json().get("Response").get("Id")
            appsession_id = response.json().get("Response").get("AppSession").get("Id")
        sample_metadata["sample_id"] = sample_identifier
        sample_metadata["appsession_id"] = appsession_id
        return sample_metadata


    def make_file(self, file_to_upload, sample_id):
        file_name = os.path.basename(file_to_upload)
        print(file_name)
        file_identifier = None
        url = f"{v1_api}/samples/{sample_id}/files"
        #data = {"name": file_to_upload}
        p = {"name": file_name, "multipart": "true"} #TODO make this the correct name
        head = {"Content-Type": "json/application", "Authorization": self.authorise, "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, params=p, allow_redirects=True)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 200:
            raise Exception(f"An error occurred communicating with BaseSpace. Error code {response.status_code}")
        else:
            print(response.json())
            file_identifier = response.json().get("Response").get("Id")
        return file_identifier


    def upload_into_file(self, upload_file, file_id, file_chunk_num, hash):
        part_num = file_chunk_num
        url = f"{v1_api}/files/{file_id}/parts/{part_num}"
        head = {"Authorization": self.authorise, "Content-MD5": hash}
        file = {"fn": open(upload_file, 'rb')}
        response = requests.put(url, headers=head, files=file, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"An error occurred communicating with BaseSpace. Error code {response.status_code}")
        else:
            md5 = response.json().get("Response").get("ETag")
        return md5


    def set_file_upload_status(self, file_id, file_status):
        url = f"{v1_api}/files/{file_id}"
        p = {"uploadstatus": file_status}
        head = {"Authorization": self.authorise}
        response = requests.post(url, headers=head, params=p, allow_redirects=True)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 201:
            print("error")
            print(response.status_code)
            print(response.json())
        else:
            print(response.json())
        return None


    def finalise_sample_data(self, sample_id):
        url = f"{v1_api}/samples/{sample_id}"
        d = {"NumReadsPF": "1000000000", "NumReadsRaw": "1000000000"} #TODO make these correct values
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=d, allow_redirects=True)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 201:
            print("error")
            print(response.status_code)
            print(response.json())
        else:
            print(response.json())
        return None


    def finalise_appsession(self, appsession_id, file_name):
        url = f"{v2_api}/appsessions/{appsession_id}"
        d = {"ExecutionStatus": "Complete", "StatusSummary": f"Finished uploading {file_name}"}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=d, allow_redirects=True)
        print(response.request.headers)
        print(response.url)
        if response.status_code != 200:
            print("error")
            print(response.status_code)
            print(response.json())
        else:
            print(response.json())
        return None
