# CRUK Pipeline
***
## Introduction
Pipeline to 

Processes:

* Identify fastqs on an Illumina SampleSheet.csv
* Pair samples according to the common identifier for DNA and RNA from the same source in the pairs variable 
* Launch the Illumina TruSight Tumor 170 app for each pair
* Once the TruSight 


***
## Installation
This software uses Python 3.7.

It has dependencies which are recommended to be installed using a Conda environment. 
Install Conda:

```
https://docs.conda.io/projects/conda/en/latest/user-guide/install/download.html
```

Clone the repository:

```
git clone https://github.com/AWGL/cruk_pipeline
```

Create Conda environment:

```
conda env create cruk_env.yml
```

If required edit the Python config file to launch the correct versions of the apps:


Edit ```config.py```

Set up the JSON config file with the correct authentication token:

Edit ```bs.config.json.example``` and save as ```bs.config.json```


The code can then be run after the Conda environment is activated:

```
python cruk_smp.py /path/to/JSON/config/file/
```
***
## Usage
The pipeline should be automatically initiated after sequencing by IlluminaQC (see https://github.com/AWGL/IlluminaQC). This launches the script ```1_launch_SMP2v3.sh``` which aggregates QC data for all samples on the run and launches the script ```cruk_smp.py```.

To run the pipeline independently of this process, 