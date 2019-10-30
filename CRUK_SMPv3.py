"""
Description here
Default behaviour is to run the entire pipeline after launch from 1_IlluminaQC.sh via 1_launch_SMP2v3.sh without manual
intervention
Options provided to manually launch and resume the pipeline from various stages if required

"""
import os
import logging
import sys
import argparse
import textwrap
import time
from parse_sample_sheet import ParseSampleSheet
from load_configuration import LoadConfiguration
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


# Class to filter logging to restrict to one level only
class MyFilter(object):
    def __init__(self, level):
        self.__level = level

    def filter(self, log_record):
        return log_record.levelno <= self.__level


def get_args():
    '''
    :return:
    '''
    argument_parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent(
            '''
            summary:
            Uploads fastqs for the CRUK project to BaseSpace, launches two applications and downloads the required
            results for analysis and reporting
            '''))

    # Adds version and date updated
    argument_parser.add_argument(
        '-v', '--version', action='version',
        version=f"version: {__version__} updated on: {__updated__}")

    # Add arguments
    # REQUIRED: Path to config file containing authorisation information
    argument_parser.add_argument(
        'path_to_config_file', action='store',
        help=textwrap.dedent(
            '''
            File path to config file location containing authentication credentials. Set up according to template
            in bs.config.json.example. Config file must be named bs.config.json. REQUIRED.
            '''))

    # OPTIONAL: choices for resuming part way through the pipeline
    resuming_options = argument_parser.add_mutually_exclusive_group()
    resuming_options.add_argument(
        '-t', '--tst170', action='store_true', default=False,
        help=textwrap.dedent(
            '''
            Set this flag to start pipeline from the launch of the TST170 app (post upload of all files). OPTIONAL.
            '''))
    resuming_options.add_argument(
        '-s', '--smp2', action='store_true', default=False,
        help=textwrap.dedent(
            '''
            Set this flag to start pipeline from the launch of the SMP2v3 app (post completion of TST170 app). OPTIONAL.
            '''))
    resuming_options.add_argument(
        '-d', '--dl_files', action='store_true', default=False,
        help=textwrap.dedent(
            '''
            Set this flag to download required files for all DNA samples (will include RNA sample data paired with
            DNA sample where applicable) (post completion of SMP2v3 app). OPTIONAL.
            '''))

    return argument_parser.parse_args()


class CrukSmp:
    def __init__(self):
        self.config = LoadConfiguration(args.path_to_config_file)
        #self.authorisation = self.config.get_authentication_token
        self.worksheet = "" # TODO finish work here- required params for resuming
        self.sample_pairs = ""



    def launch_tst170_analysis(self, launch, worksheet_id, dna_sample, pairs_dict):
        '''
        :param launch:
        :param worksheet_id:
        :param dna_sample:
        :param pairs_dict:
        :return:
        '''
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

    def launch_smp_analysis(self, launch_smp, tst_values):
        '''
        :param launch_smp:
        :param tst_values:
        :return:
        '''
        # Get dataset ids using TST 170 appsession id and nucleotide biosample id
        dna_dataset_id = launch_smp.get_datasets(tst_values.get("appsession"), tst_values.get("dna_biosample_id"))
        rna_dataset_id = launch_smp.get_datasets(tst_values.get("appsession"), tst_values.get("rna_biosample_id"))
        # Create configuration for SMP2 v3 app launch
        smp_app_config = launch_smp.generate_smp_app_config(dna_dataset_id, rna_dataset_id)

        # Launch SMP2 v3
        smp_appsession = launch_smp.launch_application(smp_app_config)
        return smp_appsession

    def download_files(self, authorisation, worksheet_id, sample, appresult, download_file_extensions=download_file_extensions):
        '''
        :param authorisation:
        :param worksheet_id:
        :param sample:
        :param appresult:
        :param download_file_extensions:
        :return:
        '''
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

    def main(self):
        '''
        :return:
        '''
        if args.tst170:
            # Resume at launch of TST170 app
            print("trigger tst170")
        elif args.smp2:
            # Resume at launch of SMP2v3 app
            print("trigger smp2")
        elif args.dl_files:
            print("trigger download")

        #TODO Stuff required for every program execution
        # Load command line arguments- TODO WORK ON WHERE


        # Parse sample sheet to extract relevant sample information
        my_sample_sheet = ParseSampleSheet(os.getcwd())
        my_sample_sheet.read_in_sample_sheet()

        # Pull out a series of samples to upload to BaseSpace
        samples_to_upload = my_sample_sheet.identify_samples()

        # Identify the worksheet number which will be used as the project name in BaseSpace
        worksheet = my_sample_sheet.identify_worksheet()

        # Load the config file containing user-specific information and obtain the authentication token
        #config = LoadConfiguration(config_file_path)
        #authorisation = config.get_authentication_token()

        # Load and parse out variables from variables files associated with each sample
        all_variables = my_sample_sheet.load_all_variables(samples_to_upload, os.getcwd())

        # Pair samples- DNA sample is key, RNA sample to look up- if No RNA sample, it is None
        sample_pairs = my_sample_sheet.create_sample_pairs(all_variables)
        # Write out sample pairs to log file for checking if needed TODO WHERE
        log.warning(f"sample pairs are {sample_pairs}")

        # Locate the fastqs associated with all samples
        all_fastqs = my_sample_sheet.locate_all_fastqs(samples_to_upload, os.getcwd())

        # Create a project in BaseSpace- will not create if it already exists, but will still return project id
        upload = FileUpload(self.config.get_authentication_token, worksheet, samples_to_upload, all_fastqs)
        project = upload.create_basespace_project()
        log.info(f"Project {worksheet} created")
        log.warning(f"Project id from project name {worksheet} is {project}")

        # All logic for uploading files
        upload.upload_files()

        # Launch application
        # Create launch app object for TST 170
        launch_tst = LaunchApp(self.config.get_authentication_token, project, app_name, app_version)

        #TODO working here

        # Launch TST170 app for DNA, RNA pairs
        tst_170 = {}
        for dna_sample in sample_pairs.keys():
            tst_170_launch = self.launch_tst170_analysis(launch_tst, worksheet, dna_sample, sample_pairs)
            tst_170[dna_sample] = tst_170_launch
            # Write out to log file to provide data required to resume process from this point
            log.warning(f"{dna_sample}: {tst_170_launch}")

        # Poll appsession status of launched TST 170 app- polling runs until appsession is complete then launch SMP2 v3 app
        smp_appresults = {}
        for dna_sample, tst_values in tst_170.items():
            rna_sample = sample_pairs.get(dna_sample)
            appsession = tst_values.get("appsession")
            log.info(f"Polling status of TST 170 application, appsession {tst_values.get('appsession')}")
            polling = PollAppsessionStatus(self.config.get_authentication_token, tst_values.get("appsession"))
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
            launch_smp = LaunchApp(self.config.get_authentication_token, project, smp2_app_name, smp2_app_version)
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
            polling = PollAppsessionStatus(self.config.get_authentication_token, smp_appsession)
            poll_result = polling.poll()  # Poll status of appsession
            log.info(f" SMP2 v3 appsession {smp_appsession} for sample {dna_sample} and {rna_sample} has finished with "
                  f"status {poll_result}")

            if poll_result == "Fail":
                log.info(f"SMP2 v3 app for samples {dna_sample} and {rna_sample} has failed to "
                        f"complete. Investigate further through the BaseSpace website.")
                # Download  log files for help with troubleshooting
                failed_appresults = polling.find_appresults()
                if len(failed_appresults) == 1:
                    log.info(download_files(self.config.get_authentication_token, worksheet, dna_sample, failed_appresults[0], [".log"]))
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
            log.info(download_files(self.config.get_authentication_token, worksheet, dna_sample, appresult)) #TODO ??
        log.info("Files downloaded for all samples and appresults")


if __name__ == '__main__':

    __version__ = '2.0.0'
    __updated__ = "Date here"

    # file paths
    #ss_location = os.getcwd()  # results directory
    #results_directory = os.getcwd()  # results directory
    output_directory = os.getcwd()  # results directory
    # Adds the leading . for the first extension
    download_file_extensions[0] = f".{download_file_extensions[0]}"

    # Set up logger
    log = logging.getLogger(__name__) # TODO Share this logger across the project
    log.setLevel(logging.DEBUG)
    handler_out = logging.StreamHandler(sys.stdout)
    handler_out.setLevel(logging.INFO)
    handler_out.addFilter(MyFilter(logging.INFO))
    handler_err = logging.StreamHandler(sys.stderr)
    handler_err.setLevel(logging.WARNING)
    handler_err.addFilter(MyFilter(logging.WARNING))
    log.addHandler(handler_out)
    log.addHandler(handler_err)

    args = get_args()
    cr = CrukSmp()
    cr.main()
