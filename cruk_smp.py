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
from parse_sample_sheet import ParseSampleSheet
from load_configuration import LoadConfiguration
from file_upload import FileUpload
from launch_app import LaunchApp
from file_downloader import FileDownloader
from config import app_name
from config import app_version
from config import smp2_app_name
from config import smp2_app_version


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

        # Upload fastq files
        upload.upload_files()

        # Launch application
        # Create launch app object for TST 170
        # IMPORTANT NOTE: Only processes paired data
        launch_tst = LaunchApp(self.config.get_authentication_token, worksheet, project, app_name,
                               app_version, sample_pairs)

        tst_170 = launch_tst.launch_tst170_pairs()  # TODO Dump data to temp file (was tst170 items)

        # TODO- for resuming from smp2 applaunch and from file download will need to load the launchapp object
        # Create launch app object for SMP2 v3
        launch_smp = LaunchApp(self.config.get_authentication_token, worksheet, project, smp2_app_name,
                               smp2_app_version, sample_pairs, tst_170)
        # Poll the tst 170 appsessions until completion, then launch smp2 app
        smp_appsession = launch_smp.poll_tst170_launch_smp2()
        # Poll the smp appsessions until completion
        smp_appresults = launch_smp.poll_smp2()

        # Download all required files
        file_downloader = FileDownloader(self.config.get_authentication_token, smp_appresults, worksheet)


if __name__ == '__main__':

    __version__ = '2.0.0'
    __updated__ = "01/11/2019"

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
