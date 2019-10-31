from identify_files_to_download import IdentifyFiles
from config import download_file_extensions


class FileDownloader:
    def __init__(self, auth, appresults, worksheet):
        self.auth = auth
        self.appresults = appresults
        self.worksheet = worksheet
        # Adds the leading . for the first extension
        download_file_extensions[0] = f".{download_file_extensions[0]}"
        self.download_file_extensions = download_file_extensions

    def download_files(self):
        # Download files within appresults for which the SMP2 app successfully completed
        # Iterate over all appresults- one per dna sample successfully completed
        for dna_sample, appresult_dict in self.appresults.items():
            appresult = appresult_dict.get("appresult")
            if appresult_dict.get("status") == "Fail":
                #log.info(f"SMP2 v3 app for dna sample {dna_sample} has failed to "#TODO logger needs to be shared across files
                        #f"complete. Investigate further through the BaseSpace website.")
                identify_files = IdentifyFiles(self.auth, self.worksheet, dna_sample, appresult, [".log"])
                identify_files.download_sample_files()
            #log.info(f"Downloading results for sample {dna_sample}") #TODO logger needs to be shared across files
            identify_files = IdentifyFiles(self.auth, self.worksheet, dna_sample, appresult,
                                           ",.".join(self.download_file_extensions))
            print(identify_files.download_sample_files()) #TODO ??#TODO logger needs to be shared across files (log.info)
        return "Files downloaded for all samples and appresults"

