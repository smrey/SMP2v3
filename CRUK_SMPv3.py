import os
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

    # Locate the fastqs associated with all samples
    all_fastqs = my_sample_sheet.locate_all_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    config = LoadConfiguration(config_file_path)
    authorisation = config.get_authentication_token()

    # Create a project in BaseSpace
    upload_file = FileUpload(authorisation)
    project = upload_file.create_basespace_project(worksheet)

    #file_to_split = "/Users/sararey/Documents/cruk_test_data/CellLine_Mixture_DNA_SmallVariants.genome.vcf" # testing

    # For each sample on worksheet
    for ind, sample in enumerate(samples_to_upload):
        sample_num = ind + 1

        # Create a sample inside the project in BaseSpace
        sample_metadata = upload_file.make_sample(sample, sample_num)
        sample_id = sample_metadata.get("sample_id")
        appsession_id = sample_metadata.get("appsession_id")
        print(sample_id)

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
            print(file_chunks_created)

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

            # Check all files uploaded
            if len(file_chunks_created) != num_chunks_uploaded:
                raise Exception(f"Not all file chunks successfully uploaded for file {f}")

            # Set file status to complete
            upload_file.set_file_upload_status(file_id, "complete") #TODO this is giving an error- bad request

            # Mark appsession as complete
            upload_file.finalise_appsession(appsession_id, f)


        #break
        # Finalise details of sample- metadata additions if required
        upload_file.finalise_sample_data(sample_id) #TODO add accurate figures for this


if __name__ == '__main__':
        main()