import requests
import os
from config import v1_api
from config import v2_api

class DownloadFiles:

    def __init__(self, file_info, download_location, auth):
        self.file_info = file_info
        self.download_location = download_location
        self.authorise = auth

    def download_files(self):
        file_name = self.file_info.get("Name") #TODO Change file name to remove worksheetid from beginning before -
        file_id = self.file_info.get("Id")
        # TODO multipart download if required- chunk file and reassemble
        url = f"{v1_api}/files/{file_id}/content/"
        p = {"redirect": "true"}  # this may need to be set to meta
        head = {"Authorization": self.authorise}
        response = requests.get(url, params=p, headers=head)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}. "
                            f"File {file_name} not completed download")
        else:
            with open(os.path.join(self.download_location, file_name), 'wb') as wf:
                for chunk in response:
                    wf.write(chunk)
        return file_name


