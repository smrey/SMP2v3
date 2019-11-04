import requests
import os
import logging
from config import v1_api
from download_files import DownloadFiles

log = logging.getLogger("cruk_smp")


class IdentifyFiles:

    def __init__(self, auth, worksheet, dna_sample, appresultid, file_extensions):
        self.authorise = auth
        self.worksheet = worksheet
        self.dna_sample = dna_sample
        self.appresultid = appresultid
        self.file_extensions = file_extensions
        self.authorise = auth
        self.files = {}

    def download_sample_files(self):
        '''
        :param authorisation:
        :param worksheet_id:
        :param sample:
        :param appresult:
        :param download_file_extensions:
        :return:
        '''
        # Create directory for downloaded files where one does not exist
        if not os.path.isdir(os.path.join(os.getcwd(), self.worksheet)):
            os.mkdir(os.path.join(os.getcwd(), self.worksheet))
        # Make sample directory
        if not os.path.isdir(os.path.join(os.getcwd(), self.worksheet, self.dna_sample)):
            os.mkdir(os.path.join(os.getcwd(), self.worksheet, self.dna_sample))
        all_file_metadata = self.get_files_from_appresult()
        # Iterate over identified files of required file types
        file_download_success = []
        for fl in all_file_metadata:
            file_download = DownloadFiles(fl, os.path.join(os.getcwd(), self.worksheet, self.dna_sample),
                                          self.authorise)
            file_download_success.append(file_download.download_files())
        if len(all_file_metadata) == len(file_download_success):
            file_dl_result = f"All files successfully downloaded for sample {self.dna_sample}, " \
                             f"appresult {self.appresultid}"
        else:
            file_dl_result = f"Files may be missing for sample {self.dna_sample}, " \
                             f"appresult {self.appresultid}. Please check."
        return file_dl_result

    def get_files_from_appresult(self):
        url = f"{v1_api}/appresults/{self.appresultid}/files/"
        p = {"Extensions": self.file_extensions, "Limit": 200}
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
