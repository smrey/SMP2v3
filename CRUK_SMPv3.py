
from parse_sample_sheet import ParseSampleSheet
from load_configuration import LoadConfiguration
from split_file import SplitFile
from file_upload import FileUpload

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1
fastq_location = "/Users/sararey/Documents/cruk_test_data/rawFQs/"
config_file_path = "/Users/sararey/PycharmProjects/CRUK/"

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
    fastqs = my_sample_sheet.locate_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    config = LoadConfiguration(config_file_path)
    authorisation = config.get_authentication_token()

    # Create a project in BaseSpace
    upload_file = FileUpload(authorisation)
    project = upload_file.create_basespace_project(worksheet)


    #file_to_split = "/Users/sararey/Documents/cruk_test_data/CellLine_Mixture_DNA_SmallVariants.genome.vcf" # testing

    # For each sample on worksheet
    for sample in samples_to_upload:
        print(sample)

    '''
    # Create a sample inside the project in BaseSpace
    file_details = upload_file.make_sample()

    # For each file associated with that sample
    file_splitting = SplitFile(file_to_split)
    chunks = file_splitting.get_file_chunk_size()
    print(chunks)
    num_file_chunks_written = file_splitting.split_file(chunks)

    md5_dict = {}
    # files appended numbers with md5 hashes for upload
    for i in range(num_file_chunks_written):
        hash = file_splitting.calc_md5(f"{file_to_split}_{i + 1}")
        md5_dict[i + 1] = hash

        # upload file in here to save iterating twice (once to populate and once to read dictionary?)



    print(md5_dict)
'''


if __name__ == '__main__':
        main()