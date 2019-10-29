import pandas as pd
import glob
import os


class ParseSampleSheet:
    def __init__(self, sample_sheet_dir):
        self.sample_sheet_dir = sample_sheet_dir
        self.sample_sheet_dataframe = pd.DataFrame()

    def read_in_sample_sheet(self):
        '''
        :param ss: the Illumina sample sheet
        :return: a dataframe containing the sample-related information from the Illumina sample sheet
        '''
        # Locate index of header for sample information in sample sheet- assumes sample sheet name is SampleSheet.csv
        open_sample_sheet = os.path.join(self.sample_sheet_dir, "SampleSheet.csv")
        header_index = False
        with open(open_sample_sheet) as sample_sheet:
            for index, line in enumerate(sample_sheet):
                if line.startswith("Sample_ID"):
                    header_index = index
        ss_data = pd.read_csv(open_sample_sheet, header=0, skiprows=header_index)
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
        # Only one entry in list if there is one worksheet- assumed
        worksheet_id = self.sample_sheet_dataframe["Sample_Plate"].unique().tolist()[0]
        return worksheet_id

    @staticmethod
    def locate_all_fastqs(samples, results_dir):
        '''
        :param ss_df: the sample-related information from the Illumina sample sheet as a data frame
        :param fq_loc:
        :return:
        '''
        # Create list for error handling when there are no fastqs found for a sample
        no_fastqs = []
        # Create dictionary to hold information about fastqs
        sample_fastqs_dict = {}
        # Iterate over all sample identifiers
        for index_sample in samples.iteritems():
            sample = index_sample[1] # row labels not required, data in first column of series
            # Create list of all fastqs matching sample id- all for upload into <sample>- pre-requisite to app launch
            sample_fastqs_list = (glob.glob(os.path.join(results_dir, sample, sample) + '*' + 'fastq.gz'))
            sample_fastqs_dict[sample] = sample_fastqs_list
            if not sample_fastqs_list:
                no_fastqs.append(sample)
        if no_fastqs:
            raise Exception(f"No fastqs found for sample or samples: {no_fastqs}")
        return sample_fastqs_dict

    @staticmethod
    def load_all_variables(samples, results_dir):
        # Create dictionary to hold variables data
        sample_variables_dict = {}
        for index_sample in samples.iteritems():
            sample = index_sample[1]  # row labels not required, data in first column of series
            # Create dictionary to hold key-value pairs
            variables_dict = {}
            with open(os.path.join(results_dir, sample, sample) + ".variables") as vf:
                for line in vf:
                    split_line = line.split("=")
                    try:
                        variables_dict[split_line[0]] = split_line[1].rstrip()
                    except IndexError:
                        # Skip lines where there is no '='
                        pass
                sample_variables_dict[sample] = variables_dict
        return sample_variables_dict

    @staticmethod
    def create_sample_pairs(variables):
        dna_dict = {}
        rna_dict = {}
        for k, v in variables.items():
            if v.get("sampleType") == "DNA":
                dna_dict[v.get("pairs")] = v.get("sampleId")
            elif v.get("sampleType") == "RNA":
                rna_dict[v.get("pairs")] = v.get("sampleId")
        pairs_dict = {}
        for patient_name in dna_dict.keys():
            if not patient_name == "null":
                pairs_dict[dna_dict.get(patient_name)] = rna_dict.get(patient_name, None)
        return pairs_dict


