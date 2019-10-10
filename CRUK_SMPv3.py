import os
import gzip
import time
from Bio.SeqIO.QualityIO import FastqGeneralIterator
from parse_sample_sheet import ParseSampleSheet
from load_configuration import LoadConfiguration
from split_file import SplitFile
from file_upload import FileUpload
from launch_app import LaunchApp
from poll_appsession_status import PollAppsessionStatus
from identify_files_to_download import IdentifyFiles
from download_files import DownloadFiles
from config import app_name
from config import app_version
from config import smp2_app_name
from config import smp2_app_version
from config import download_file_extensions

# testing file paths
ss_location = os.getcwd() # to be commandline arg1- os.path.join() useful
results_directory = os.getcwd() # results directory
config_file_path = "/data/diagnostics/pipelines/CRUK/CRUK-2.0.0/" # TODO import version from illuminaQC- cmdline opt (argparse)
output_directory = os.getcwd() # results directory
download_file_extensions[0] = f".{download_file_extensions[0]}" # TODO make this clearer- adds leading . for 1st extension

def upload_files():
    return None

def launch_analysis():
    return None

def analysis_status():
    return None

def download_files():
    return None


def main():
    '''
    # Parse sample sheet to extract relevant sample information
    my_sample_sheet = ParseSampleSheet(ss_location)
    my_sample_sheet.read_in_sample_sheet()

    # Pull out a series of samples to upload to BaseSpace
    samples_to_upload = my_sample_sheet.identify_samples()

    # Identify the worksheet number which will be used as the project name in BaseSpace
    worksheet = my_sample_sheet.identify_worksheet()

    # Load the config file containing user-specific information and obtain the authentication token
    config = LoadConfiguration(config_file_path)
    authorisation = config.get_authentication_token()

    # Locate the fastqs associated with all samples
    all_fastqs = my_sample_sheet.locate_all_fastqs(samples_to_upload, results_directory)

    # Load and parse out variables from variables files associated with each sample
    all_variables = my_sample_sheet.load_all_variables(samples_to_upload, results_directory)

    # Pair samples- DNA sample is key, RNA sample to look up- if No RNA sample, it is None
    sample_pairs = my_sample_sheet.create_sample_pairs(all_variables)

    # Create a project in BaseSpace
    upload_file = FileUpload(authorisation, worksheet)
    project = upload_file.create_basespace_project()

    # For each sample on worksheet
    for sample_num, sample in enumerate(samples_to_upload, 1):
        print(f"Uploading sample {sample}")
        read_num = 0 # Cumulative tally
        len_reads = 0 # Same across all fastqs on the run
        # Create a sample inside the project in BaseSpace
        sample_metadata = upload_file.make_sample(sample, sample_num)
        sample_id = sample_metadata.get("sample_id")
        appsession_id = sample_metadata.get("appsession_id")
        # Pull out files associated with that particular sample
        fastq_files = all_fastqs.get(sample)
        # For each file associated with that sample
        for f in fastq_files:
            print(f"Uploading fastq {f}")
            # Identify if read 1 or read 2
            match_read = f.split("_")
            read = match_read[:][-2]
            # Extract required fastq information from R1- assume R2 is the same- paired end
            if read == "R1":
                fq_metadata = upload_file.get_fastq_metadata(f) # Returns (max read length, number of reads in fastq)
                if len_reads < fq_metadata[0]:
                    len_reads = fq_metadata[0]
                num_reads = fq_metadata[1]
                # Cumulative tally of read numbers for this sample
                read_num += num_reads
            # Create a file inside the sample in BaseSpace
            file_id = upload_file.make_file(f, sample_id)
            #Split the file into chunks for upload
            file_splitting = SplitFile(os.path.join(results_directory, sample, f))
            chunks = file_splitting.get_file_chunk_size()
            file_chunks_created = file_splitting.split_file(chunks)
            num_chunks_uploaded = 0
            for i, f_chunk in enumerate(file_chunks_created):
                # Calculate hash for file chunk
                md5_b64 = file_splitting.calc_md5_b64(f_chunk)
                md5_hex = file_splitting.calc_md5_hex(f_chunk)
                # Populate sample with file chunks
                chunk_num = i + 1
                file_part_uploaded_md5 = upload_file.upload_into_file(f_chunk, file_id, chunk_num, md5_b64)
                # Check MD5s match before and after upload
                if md5_hex != file_part_uploaded_md5:
                    raise Exception(f"MD5s do not match before and after file upload for file chunk {f_chunk}")
                # Delete file chunk after upload successful
                os.remove(f_chunk)
                num_chunks_uploaded += 1
            # Check all file parts uploaded
            if len(file_chunks_created) != num_chunks_uploaded:
                raise Exception(f"Not all file chunks successfully uploaded for file {f}")
            # Set file status to complete
            print(upload_file.set_file_upload_status(file_id, "complete"))
        # Update sample metadata
        upload_file.update_sample_metadata(sample, sample_num, sample_id, len_reads, read_num)
        # Mark file upload appsession as complete
        upload_file.finalise_appsession(appsession_id, sample)

    # Launch application
    launch = LaunchApp(authorisation, project, app_name, app_version)
    launch_smp = LaunchApp(authorisation, project, smp2_app_name, smp2_app_version)

    # Wait to allow biosample indexes to update (5 seconds)
    time.sleep(5)

    # Launch app for DNA, RNA pairs
    appsession_list = []
    for dna_sample in sample_pairs.keys():
        # Identify biosamples for upload
        dna_biosample_id = launch.get_biosamples(f"{worksheet}-{dna_sample}")
        rna_sample = sample_pairs.get(dna_sample)
        rna_biosample_id = launch.get_biosamples(f"{worksheet}-{rna_sample}")

        app_config = launch.generate_app_config(config_file_path, dna_biosample_id, rna_biosample_id)

        # Find specific application ID for application and version number
        launch.get_app_group_id()
        launch.get_app_id()

        # Launch application for DNA and RNA pair
        print(f"Launching {app_name} {app_version} for {dna_sample} and {rna_sample}")
        appsession_list.append(launch.launch_application(app_config))
        # TODO write appsession out to file to help with resuming?

    # Poll appsession status post launch- polling runs until appsession is complete
    for appsession in appsession_list:
        print(f"Polling status of application, appsession {appsession}")
        polling = PollAppsessionStatus(authorisation, appsession)
        print(polling.poll())  # Poll status of appsession #TODO add something to identify and print out for Aborted samples

        # Identify appresults
        appresults = polling.find_appresults()
        '''
    '''
        # Launch SMP2v3 app
        smp_app_config = launch_smp.generate_smp_app_config()
        launch_smp.get_app_group_id()
        launch_smp.get_app_id()
        '''

    #TODO temp testing- note this needs to be indented as app launch is per TST170 app completion?- figure out appropriate way
    launch_smp = LaunchApp("stuff", "project", smp2_app_name, smp2_app_version)
    # Get dataset ids
    print(launch_smp.generate_smp_app_config("11111", "22222"))

    '''
    # Download files within appresults- SMP2 app
    # Create directory for downloaded files where one does not exist
    if not os.path.isdir(os.path.join(output_directory, worksheet)):
        os.mkdir(os.path.join(output_directory, worksheet))
    # Iterate over all appresults- one per sample
    for sample, appresult in appresults.items():
        print(f"Downloading results for sample {sample}")
        # Make sample directory
        if not os.path.isdir(os.path.join(output_directory, worksheet, sample)):
            os.mkdir(os.path.join(output_directory, worksheet, sample))
        find_files = IdentifyFiles(appresult, ",.".join(download_file_extensions), authorisation)
        all_file_metadata = find_files.get_files_from_appresult()
        # Iterate over identified files of required file types
        file_download_success = []
        for fl in all_file_metadata:
            file_download = DownloadFiles(fl, os.path.join(output_directory, worksheet, sample), authorisation)
            file_download_success.append(file_download.download_files())
        if len(all_file_metadata) == len(file_download_success):
            print(f"All files successfully downloaded for sample {sample}, appresult {appresult}")
        else:
            print(f"Files may be missing for sample {sample}, appresult {appresult}. Please check.")

 print("Files downloaded for all samples and appresults")
'''
if __name__ == '__main__':
     main()