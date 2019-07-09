# Parse sample sheet to extract sampleid (col 1- header Sample_ID), sample name (col 2- header Sample_Name),
# worksheet id (col 3- header Sample_plate)

import pandas as pd

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1


def read_in_sample_sheet(ss):
    '''
    :param ss:
    :return:
    '''
    header_index = False
    with open(ss_location) as sample_sheet:
        for index, line in enumerate(sample_sheet):
            if line.startswith("Sample_ID"):
                header_index = index
    ss_data = pd.read_csv(ss_location, header=0, skiprows=header_index)
    # delete empty rows
    return ss_data


def main():
    print(read_in_sample_sheet(ss_location))


if __name__ == '__main__':
        main()