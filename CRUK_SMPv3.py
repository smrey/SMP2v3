
from parse_sample_sheet import ParseSampleSheet
from split_file import SplitFile

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1
file_to_split = "/Users/sararey/Documents/cruk_test_data/rawFQs/NA12877-A1_S1_L001_R1_001.fastq.gz"


def main():
    # Parse sample sheet to extract relevant sample information
    my_sample_sheet = ParseSampleSheet(ss_location)
    my_sample_sheet.read_in_sample_sheet()

    # Pull out a series of samples to upload to BaseSpace
    samples_to_upload = my_sample_sheet.identify_samples()

    # Identify the worksheet number which will be used as the project name in BaseSpace
    worksheet = my_sample_sheet.identify_worksheet()

    # Locate the fastqs associated with each sample
    #fastqs = locate_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    #configs = load_config_file(config_file_pth)
    #auth = 'Bearer ' + configs.get("authenticationToken") #TODO This could be better as a global variable?

    file_splitting = SplitFile(file_to_split)
    file_size = file_splitting.file_size()
    chunks = file_splitting.get_file_chunk_size()
    print(chunks)
    print(file_splitting.split_file(chunks))




if __name__ == '__main__':
        main()