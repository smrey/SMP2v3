#!/bin/bash
#PBS -l walltime=120:00:00
#PBS -l ncpus=12
set -euo pipefail
PBS_O_WORKDIR=(`echo $PBS_O_WORKDIR | sed "s/^\/state\/partition1//" `)
cd $PBS_O_WORKDIR

#Description: CRUK SMP2v3 Illumina TST170 Pipeline (Illumina paired-end). Not for use with other library preps/ experimental conditions.
#Author: Sara Rey, All Wales Medical Genetics Lab
#Mode: BY_SAMPLE
version="2.0.0"

# Directory structure required for pipeline
#
# /data
# â””â”€â”€ results
#     â””â”€â”€ seqId
#         â”œâ”€â”€ panel1
#         â”‚Â Â  â”œâ”€â”€ sample1
#         â”‚Â Â  â”œâ”€â”€ sample2
#         â”‚Â Â  â””â”€â”€ sample3
#         â””â”€â”€ panel2
#             â”œâ”€â”€ sample1
#             â”œâ”€â”€ sample2
#             â””â”€â”€ sample3
#
# Script 1 runs in sample folder, requires fastq files split by lane

countQCFlagFails() {
    #count how many core FASTQC tests failed
    grep -E "Basic Statistics|Per base sequence quality|Per tile sequence quality|Per sequence quality scores|Per base N content" "$1" | \
    grep -v ^PASS | \
    grep -v ^WARN | \
    wc -l | \
    sed 's/^[[:space:]]*//g'
}

#load sample & pipeline variables
. *.variables
. /data/diagnostics/pipelines/SomaticAmplicon/SomaticAmplicon-"$version"/"$panel"/"$panel".variables

### Preprocessing ###

#record FASTQC pass/fail
rawSequenceQuality=PASS

# Extract QC data
for fastqPair in $(ls "$sampleId"_S*.fastq.gz | cut -d_ -f1-3 | sort | uniq); do

    #parse fastq filenames
    laneId=$(echo "$fastqPair" | cut -d_ -f3)
    read1Fastq=$(ls "$fastqPair"_R1_*fastq.gz)
    read2Fastq=$(ls "$fastqPair"_R2_*fastq.gz)
    unzippedRead1Fastq=${read1Fastq%.*}
    unzippedRead2Fastq=${read2Fastq%.*}

    #fastqc
    /share/apps/fastqc-distros/fastqc_v0.11.5/fastqc -d /state/partition1/tmpdir --threads 12 --extract "$read1Fastq"
   /share/apps/fastqc-distros/fastqc_v0.11.5/fastqc -d /state/partition1/tmpdir --threads 12 --extract "$read2Fastq"

    mv "$unzippedRead1Fastq"_fastqc/summary.txt "$unzippedRead1Fastq"_fastqc.txt
    mv "$unzippedRead2Fastq"_fastqc/summary.txt "$unzippedRead2Fastq"_fastqc.txt

    #check FASTQC output
    if [ $(countQCFlagFails "$unzippedRead1Fastq"_fastqc.txt) -gt 0 ] || [ $(countQCFlagFails "$unzippedRead2Fastq"_fastqc.txt) -gt 0 ]; then
        rawSequenceQuality=FAIL
    fi

    #clean up
    rm "unzippedRead1Fastq".fastq "$unzippedRead2Fastq".fastq *_fastqc.zip
    rm -r "$unzippedRead1Fastq"_fastqc "$unzippedRead2Fastq"_fastqc
done


source /home/sr/anaconda2/bin/activate SMP2v3

# Run CRUK SMP2v3 pipeline
#python CRUK_SMPv3.py "$version" # Edit depending on argparse options

source /home/sr/anaconda2/bin/deactivate

### Generate Combined QC File ###
python /data/diagnostics/scripts/merge_qc_files.py .