
import glob
class FileUpload:

    def __init__(self, p):
        self.p = p

    def locate_fastqs(self, samples, fq_loc):
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
            sample_fastqs_list = (glob.glob(fq_loc + sample + '*'))
            sample_fastqs_dict[sample] = sample_fastqs_list
        return sample_fastqs_dict