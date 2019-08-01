
from parse_sample_sheet import ParseSampleSheet

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1


def main():
    # Parse sample sheet to extract relevant sample information
    my_sample_sheet = ParseSampleSheet(ss_location)
    my_sample_sheet.read_in_sample_sheet()

    # Pull out a series of samples to upload to BaseSpace
    samples_to_upload = my_sample_sheet.identify_samples()

    # Identify the worksheet number which will be used as the project name in BaseSpace
    worksheet = my_sample_sheet.identify_worksheet()

    # Locate the fastqs associated with each sample
    fastqs = locate_fastqs(samples_to_upload, fastq_location)

    # Load the config file containing user-specific information and obtain the authentication token
    configs = load_config_file(config_file_pth)
    auth = 'Bearer ' + configs.get("authenticationToken") #TODO This could be better as a global variable?

    # Create project and return BaseSpace project identifier
    ##project = create_basespace_project(worksheet, auth) # Note can save results to a different project through the gui

    # Create appresults to store files and return BaseSpace appresults identifier
    #appresults_dictionary = create_appresults(samples_to_upload, worksheet, project, auth)

    # Upload files into appresults -
    #file_upload(appresults_dictionary, fastqs, auth)



    #use existing appresult for testing
    ##response = requests.get(v1_api + "/projects/" + project + "/appresults",
                             ##headers={"Authorization": auth},
                             ##allow_redirects=True)
    ##print(response.json().get('Response'))
    #initiate_upload("/Users/sararey/Documents/cruk_test_data/rawFQs/NA12877-A1_S1_L001_R1_001.fastq.gz", "234764918", auth)






if __name__ == '__main__':
        main()