import os
import logging
import sys
import time
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
ss_location = os.getcwd() # point to archive/fastq/run id directory
results_directory = os.getcwd() # results directory
config_file_path = "/data/diagnostics/pipelines/CRUK/CRUK-2.0.0/" # TODO import version from illuminaQC- cmdline opt (argparse)
output_directory = os.getcwd() # results directory
# Adds the leading . for the first extension
download_file_extensions[0] = f".{download_file_extensions[0]}"


class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, log_record):
        return log_record.levelno <= self.__level


log = logging.getLogger()
log.setLevel(logging.DEBUG)

handler_out = logging.StreamHandler(sys.stdout)
handler_out.setLevel(logging.INFO)
handler_out.addFilter(MyFilter(logging.INFO))

handler_err = logging.StreamHandler(sys.stderr)
handler_err.setLevel(logging.WARNING)
handler_err.addFilter(MyFilter(logging.WARNING))

log.addHandler(handler_out)
log.addHandler(handler_err)


def upload_files(upload_file, sample, all_fastqs):
    file_upload_info = {}
    read_num = 0  # Cumulative tally
    len_reads = 0  # Same across all fastqs on the run
    # Create a sample inside the project in BaseSpace
    sample_metadata = upload_file.make_sample(sample)
    sample_id = sample_metadata.get("sample_id")
    appsession_id = sample_metadata.get("appsession_id")
    file_upload_info["sample_id"] = sample_id
    file_upload_info["appsession_id"] = appsession_id
    # Pull out files associated with that particular sample
    fastq_files = all_fastqs.get(sample)
    # For each file associated with that sample
    for f in fastq_files:
        print(f"Uploading fastq {f}")
        # Identify if read 1 or read 2
        match_read = f.split("_")
        read = match_read[:][-2] # Requires no underscores in file name, SMP2 v3 app also requires this
        # Extract required fastq information from R1- assume R2 is the same- paired end
        if read == "R1":
            fq_metadata = upload_file.get_fastq_metadata(f)  # Returns (max read length, number of reads in fastq)
            if len_reads < fq_metadata.get("len_reads"):
                len_reads = fq_metadata.get("len_reads")
            num_reads = fq_metadata.get("num_reads")
            # Cumulative tally of read numbers for this sample
            read_num += num_reads
        # Create a file inside the sample in BaseSpace
        file_id = upload_file.make_file(f, sample_id)
        # Split the file into chunks for upload
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
        file_upload_info["len_reads"] = len_reads
        file_upload_info["read_num"] = read_num
        # Set file status to complete
        log.info(upload_file.set_file_upload_status(file_id, "complete"))
    return file_upload_info


def launch_tst170_analysis(launch, worksheet_id, dna_sample, pairs_dict):
    # Identify biosamples for upload
    dna_biosample_id = launch.get_biosamples(f"{worksheet_id}-{dna_sample}")
    rna_sample = pairs_dict.get(dna_sample)
    rna_biosample_id = launch.get_biosamples(f"{worksheet_id}-{rna_sample}")

    # Create configuration for TST 170 app launch
    app_config = launch.generate_app_config(config_file_path, dna_biosample_id, rna_biosample_id)

    # Find specific application ID for application and version number of TST 170 app
    launch.get_app_group_id()
    launch.get_app_id()

    # Launch TST 170 application for DNA and RNA pair
    log.info(f"Launching {app_name} {app_version} for {dna_sample} and {rna_sample}")
    appsession = launch.launch_application(app_config)
    tst_170 = {"appsession": appsession, "dna_biosample_id": dna_biosample_id,
                                "rna_biosample_id": rna_biosample_id}
    return tst_170


def launch_smp_analysis(launch_smp, tst_values):
    # Get dataset ids using TST 170 appsession id and nucleotide biosample id
    dna_dataset_id = launch_smp.get_datasets(tst_values.get("appsession"), tst_values.get("dna_biosample_id"))
    rna_dataset_id = launch_smp.get_datasets(tst_values.get("appsession"), tst_values.get("rna_biosample_id"))
    # Create configuration for SMP2 v3 app launch
    smp_app_config = launch_smp.generate_smp_app_config(dna_dataset_id, rna_dataset_id)

    # Launch SMP2 v3
    smp_appsession = launch_smp.launch_application(smp_app_config)
    return smp_appsession


def download_files(authorisation, worksheet_id, sample, appresult, download_file_extensions=download_file_extensions):
    # Create directory for downloaded files where one does not exist
    if not os.path.isdir(os.path.join(output_directory, worksheet_id)):
        os.mkdir(os.path.join(output_directory, worksheet_id))
    # Make sample directory
    if not os.path.isdir(os.path.join(output_directory, worksheet_id, sample)):
        os.mkdir(os.path.join(output_directory, worksheet_id, sample))
    find_files = IdentifyFiles(appresult, ",.".join(download_file_extensions), authorisation)
    all_file_metadata = find_files.get_files_from_appresult()
    # Iterate over identified files of required file types
    file_download_success = []
    for fl in all_file_metadata:
        file_download = DownloadFiles(fl, os.path.join(output_directory, worksheet_id, sample), authorisation)
        file_download_success.append(file_download.download_files())
    if len(all_file_metadata) == len(file_download_success):
        file_dl_result = f"All files successfully downloaded for sample {sample}, appresult {appresult}"
    else:
        file_dl_result = f"Files may be missing for sample {sample}, appresult {appresult}. Please check."
    return file_dl_result


def main():
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
    # Write out sample pairs to log file for checking if needed
    log.warning(f"sample pairs are {sample_pairs}")

    # Create a project in BaseSpace
    upload_file = FileUpload(authorisation, worksheet)
    project = upload_file.create_basespace_project()
    log.info(f"Project {worksheet} created")
    log.warning(f"Project id from project name {worksheet} is {project}")

    # For each sample on worksheet
    for sample_num, sample in enumerate(samples_to_upload, 1):
        log.info(f"Uploading sample {sample}")
        sample_data = upload_files(upload_file, sample, all_fastqs)
        # Update sample metadata
        upload_file.update_sample_metadata(sample, sample_num, sample_data.get("sample_id"),
                                           sample_data.get("len_reads"), sample_data.get("read_num"))
        # Mark file upload appsession as complete
        upload_file.finalise_appsession(sample_data.get("appsession_id"), sample)

    # Wait to allow biosample indexes to update (5 seconds)
    time.sleep(5)

    # Launch application
    # Create launch app object for TST 170
    launch_tst = LaunchApp(authorisation, project, app_name, app_version)

    # Launch TST170 app for DNA, RNA pairs
    tst_170 = {}
    for dna_sample in sample_pairs.keys():
        tst_170_launch = launch_tst170_analysis(launch_tst, worksheet, dna_sample, sample_pairs)
        tst_170[dna_sample] = tst_170_launch
        # Write out to log file to provide data required to resume process from this point
        log.warning(f"{dna_sample}: {tst_170_launch}")

    # Poll appsession status of launched TST 170 app- polling runs until appsession is complete then launch SMP2 v3 app
    smp_appresults = {}
    for dna_sample, tst_values in tst_170.items():
        rna_sample = sample_pairs.get(dna_sample)
        appsession = tst_values.get("appsession")
        log.info(f"Polling status of TST 170 application, appsession {tst_values.get('appsession')}")
        polling = PollAppsessionStatus(authorisation, tst_values.get("appsession"))
        poll_result = polling.poll()  # Poll status of appsession
        log.info(f" TST 170 appsession {appsession} for samples {dna_sample} and {rna_sample} has finished with "
              f"status {poll_result}")

        if poll_result == "Fail":
            log.info(f"TST170 app for samples {dna_sample} and {rna_sample} has failed to"
                    f"complete. Investigate further through the BaseSpace website.")
            # Move on to the next pair's appsession
            continue

        # Launch SMP2v3 app as each pair completes analysis with the TST170 app
        # Create launch app object for SMP2 v3
        launch_smp = LaunchApp(authorisation, project, smp2_app_name, smp2_app_version)
        # Find specific application ID for application and version number of SMP2 app
        launch_smp.get_app_group_id()
        launch_smp.get_app_id()
        log.info(f"Launching {smp2_app_name} {smp2_app_version} for {dna_sample} and {sample_pairs.get(dna_sample)}")
        smp_appresults[dna_sample] = launch_smp_analysis(launch_smp, tst_values)


    # Poll appsession status of launched SMP2 v3 app- polling runs until appsession is complete then download files
    appresults_dict = {}
    for dna_sample, smp_appsession in smp_appresults.items():
        rna_sample = sample_pairs.get(dna_sample)
        log.info(f"Polling status of SMP2 v3 application, appsession {smp_appsession}")
        polling = PollAppsessionStatus(authorisation, smp_appsession)
        poll_result = polling.poll()  # Poll status of appsession
        log.info(f" SMP2 v3 appsession {smp_appsession} for sample {dna_sample} and {rna_sample} has finished with "
              f"status {poll_result}")

        if poll_result == "Fail":
            log.info(f"SMP2 v3 app for samples {dna_sample} and {rna_sample} has failed to "
                    f"complete. Investigate further through the BaseSpace website.")
            # Download  log files for help with troubleshooting
            failed_appresults = polling.find_appresults()
            if len(failed_appresults) == 1:
                log.info(download_files(authorisation, worksheet, dna_sample, failed_appresults[0], [".log"]))
            else:
                raise Exception(f"Expected 1 appresult for appsession {smp_appsession}, dna sample {dna_sample} "
                                f"but found {len(failed_appresults)}. File path to results could not be determined- "
                                f"please download files manually from BaseSpace")
            # Move on to the next pair's appsession
            continue

        # Appresults identifier required for file download
        appresults = polling.find_appresults()
        if len(appresults) == 1:
            appresults_dict[dna_sample] = appresults[0]
        else:
            raise Exception(f"Expected 1 appresult for appsession {smp_appsession}, dna sample {dna_sample} but found "
                            f"{len(appresults)}. File path to results could not be determined- please download files"
                            f"manually from BaseSpace")
    log.warning(appresults_dict)

    # Download files within appresults for which the SMP2 app successfully completed
    # Iterate over all appresults- one per dna sample successfully completed
    for dna_sample, appresult in appresults_dict.items():
        log.info(f"Downloading results for sample {dna_sample}")
        log.info(download_files(authorisation, worksheet, dna_sample, appresult))
    log.info("Files downloaded for all samples and appresults")


if __name__ == '__main__':
    main()
