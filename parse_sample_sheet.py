# Parse sample sheet to extract sampleid (col 1- header Sample_ID), sample name (col 2- header Sample_Name),
# worksheet id (col 3- header Sample_plate)

import pandas as pd
import glob
import os

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1
# Assumes IlluminaQC script has run- change path if demuxed fastqs will be located elsewhere
fastq_location = "/Users/sararey/Documents/cruk_test_data/rawFQs/"


class ParseSampleSheet:
    def __init__(self, sample_sheet_dir):
        self.sample_sheet_dir = sample_sheet_dir
        self.sample_sheet_dataframe = pd.DataFrame()


    def read_in_sample_sheet(self):
        '''
        :param ss: the Illumina sample sheet
        :return: a dataframe containing the sample-related information from the Illumina sample sheet
        '''
        # Locate index of header for sample information in sample sheet
        header_index = False
        with open(self.sample_sheet_dir) as sample_sheet:
            for index, line in enumerate(sample_sheet):
                if line.startswith("Sample_ID"):
                    header_index = index
        ss_data = pd.read_csv(ss_location, header=0, skiprows=header_index)
        # Delete empty rows- use second column to handle case where a space may be entered into the first column
        self.sample_sheet_dataframe = ss_data.dropna(subset=["Sample_Name"])
        return self.sample_sheet_dataframe


    def identify_samples(self):
        # Extract sample identifiers (column 1 of sample sheet)
        sample_ids = self.sample_sheet_dataframe["Sample_ID"]  # Select samples as a series object
        return sample_ids


    def identify_worksheet(self):
        '''
        :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
        :return: a string of the worksheet identifier for the run
        '''
        worksheet_id = self.sample_sheet_dataframe["Sample_Plate"].unique().tolist()[0]  # Only one entry in list if there is one worksheet- assumed
        return worksheet_id


    def locate_all_fastqs(self, samples, fq_loc):
        '''
        :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
        :param fq_loc:
        :return:
        '''
        # Create dictionary to hold information about fastqs
        sample_fastqs_dict = {}
        # Iterate over all sample identifiers
        for index_sample in samples.iteritems():
            sample = index_sample[1] # row labels not required, data in first column of series
            # Create list of all fastqs matching sample id- all for upload into <sample>- pre-requisite to app launch
            sample_fastqs_list = (glob.glob(fq_loc + sample + '*' + 'fastq.gz'))
            sample_fastqs_dict[sample] = sample_fastqs_list
        return sample_fastqs_dict
