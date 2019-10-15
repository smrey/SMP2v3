import requests
import os
import gzip
from Bio.SeqIO.QualityIO import FastqGeneralIterator
from config import v1_api
from config import v2_api


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

    def make_sample(self, file_to_upload):
        sample_metadata = {}
        file_name = os.path.basename(file_to_upload)
        url = f"{v1_api}/projects/{self.project_id}/samples"
        data = {"Name": f"{self.project_name}-{file_name}", "SampleId": f"{self.project_name}-{file_name}",
                "SampleNumber": 0, "Read1": "101", "Read2": "101", "IsPairedEnd": "true"}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=data, allow_redirects=True)
        if response.status_code != 201:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        sample_metadata["sample_id"] = response.json().get("Response").get("Id")
        sample_metadata["appsession_id"] = response.json().get("Response").get("AppSession").get("Id")
        return sample_metadata

    def get_read_length_one_fq(self, fq_file):
        with gzip.open(fq_file, "rt") as fh:
            fq = FastqGeneralIterator(fh)
            for fq_id, fq_seq, fq_qual in fq:
                # Read length
                len_reads = len(fq_seq)
                return len_reads

    def get_fastq_metadata(self, fastq):
        read_metadata = {}
        num_reads = 0
        len_reads = 0
        # Open fastq
        with gzip.open(fastq, "rt") as fh_r1:
            fq_r1 = FastqGeneralIterator(fh_r1)
            for index, (fq_id, fq_seq, fq_qual) in enumerate(fq_r1, 1):  # Python is zero indexed
                # Read length
                if len(fq_seq) > len_reads:
                    len_reads = len(fq_seq)
                # Number of reads
                num_reads = index
        read_metadata["len_reads"] = len_reads
        read_metadata["num_reads"] = num_reads
        return read_metadata

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
        return f"{response.json().get('Response').get('Name')} set to status {file_status}"

    def update_sample_metadata(self, file_to_upload, sample_number, sample_id, r_len, num_reads):
        sample_metadata = {}
        file_name = os.path.basename(file_to_upload)
        url = f"{v1_api}/samples/{sample_id}"
        data = {"Name": f"{self.project_name}-{file_name}", "SampleId": f"{self.project_name}-{file_name}",
                "SampleNumber": sample_number, "Read1": r_len, "Read2": r_len, "IsPairedEnd": "true",
                "NumReadsRaw": num_reads, "NumReadsPF": num_reads}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=data, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        sample_metadata["sample_id"] = response.json().get("Response").get("Id")
        sample_metadata["appsession_id"] = response.json().get("Response").get("AppSession").get("Id")
        return sample_metadata

    def finalise_appsession(self, appsession_id, file_name):
        url = f"{v2_api}/appsessions/{appsession_id}"
        d = {"ExecutionStatus": "Complete", "StatusSummary": f"Finished uploading {file_name}"}
        head = {"Content-Type": "application/x-www-form-urlencoded", "Authorization": self.authorise,
                "User-Agent": "/python-requests/2.22.0"}
        response = requests.post(url, headers=head, data=d, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"BaseSpace error. Error code {response.status_code} message {response.text}")
        return f"AppSession {appsession_id} for file {self.project_name}-{file_name} set to status Complete"
