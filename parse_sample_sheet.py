# Parse sample sheet to extract sampleid (col 1- header Sample_ID), sample name (col 2- header Sample_Name),
# worksheet id (col 3- header Sample_plate)

ss_location = "/Users/sararey/Documents/cruk_test_data/SampleSheet.csv" # to be commandline arg1

with open(ss_location) as sample_sheet:
    for line in sample_sheet:
        print(line)