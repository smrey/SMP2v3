import os
from parse_sample_sheet import ParseSampleSheet
from load_configuration import LoadConfiguration
from split_file import SplitFile
from file_upload import FileUpload
from launch_app import LaunchApp
from poll_appsession_status import PollAppsessionStatus
from identify_files_to_download import IdentifyFiles
from download_files import DownloadFiles

# testing file paths
ss_location = os.getcwd() # to be commandline arg1- os.path.join() useful
fastq_location = os.getcwd() # results directory
config_file_path = "/data/diagnostics/pipelines/CRUK/CRUK-2.0.0/" # TODO import version from illuminaQC
output_directory = os.getcwd() # results directory

download_file_extensions = ["vcf", "bam", "bai"] #TODO change from testing actual desired files ,.bam,.bai,.xlsx"
download_file_extensions[0] = f".{download_file_extensions[0]}" # TODO make this clearer- adds leading . for 1st extension

app_name = "TruSight Tumor 170"
app_version = "1.0.3"

def upload_files():
    return None



def launch_analysis():
    return None

def analysis_status():
    return None

def download_files():
    return None


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
    all_fastqs = my_sample_sheet.locate_all_fastqs(samples_to_upload, fastq_location)

    # Create a project in BaseSpace
    upload_file = FileUpload(authorisation, worksheet)
    project = upload_file.create_basespace_project()

    # For each sample on worksheet
    for ind, sample in enumerate(samples_to_upload):
        sample_num = ind + 1

        # Create a sample inside the project in BaseSpace
        sample_metadata = upload_file.make_sample(sample, sample_num)
        sample_id = sample_metadata.get("sample_id")
        appsession_id = sample_metadata.get("appsession_id")

        # Pull out files associated with that particular sample
        fastq_files = all_fastqs.get(sample)

        # For each file associated with that sample
        for f in fastq_files:
            # Create a file inside the sample in BaseSpace
            file_id = upload_file.make_file(f, sample_id)

            #Split the file into chunks for upload
            file_splitting = SplitFile(os.path.join(fastq_location, f))
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
            upload_file.set_file_upload_status(file_id, "complete")


        # Mark file upload appsession as complete
        upload_file.finalise_appsession(appsession_id, sample)

    # Launch application
    config_file_pth = "/Users/sararey/PycharmProjects/CRUK/" #tmp variable for testing

    launch = LaunchApp(authorisation, project, app_name, app_version)

    # Identify biosamples for upload
    dna_biosample_ids = []
    rna_biosample_ids = []
    for sample in samples_to_upload:
        biosample_id = launch.get_biosamples(f"{worksheet}-{sample}")
        #TODO logic to identify and separate DNA and RNA samples- finalise
        if sample.split("_")[-1] == "RNA":
            rna_biosample_ids.append(biosample_id)
        elif sample.split("_")[-1] == "DNA":
            dna_biosample_ids.append(biosample_id)
        else:
            raise Exception(f"Could not identify if sample {sample} is a DNA or an RNA sample")

    app_config = launch.generate_app_config(config_file_pth, dna_biosample_ids, rna_biosample_ids)

    # Find specific application ID for application and version number
    launch.get_app_group_id()
    launch.get_app_id()

    # Launch application for all DNA and RNA samples
    print(f"Launching {app_name} {app_version}")
    appsession = launch.launch_application(app_config)

    # Poll appsession status post launch- runs until appsession is complete
    print(f"Polling status of application")
    polling = PollAppsessionStatus(authorisation, appsession)
    polling.poll() # Poll status of appsession
    print("Appsession complete")

    # Identify appresults
    appresults = polling.find_appresults()

    # Download files within appresults
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

if __name__ == '__main__':
        main()