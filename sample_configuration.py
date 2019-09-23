

class SampleConfiguration:

    def __init__(self, sample):
        self.sample = sample

    def parse_variables_file(self, variables_file):
        with open(variables_file) as vf:
            for line in vf:
                print(line)



    def locate_sample_fastqs(self):
        #TODO Implement if required
        return None
