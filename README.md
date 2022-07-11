## Introduction

This is reproducible code for our paper, [Identifying who has long COVID in the USA: a machine learning approach using N3C data](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(22)00048-6/fulltext), which uses data from the National COVID Cohort Collaborative’s (N3C) EHR repository to identify potential long-COVID patients. If you use this code, please cite:

    Pfaff ER, Girvin AT, Bennett TD, et al. Identifying who has long COVID in the USA: a machine learning approach using N3C data. Lancet Digital Health. 4(7),E532-E541. doi:10.1016/S2589-7500(22)00048-6

Note that the code on the main branch of this repository has been updated since the publication of the paper, and will continue to be updated. For a snapshot of the code as it stood when the paper was published, please use the "published_paper_code" branch.

## Purpose of this code
This code is designed to identify possible long COVID patients using electronic health record data as input. As of 7/11/2022, our feature table engineering code and our pretrained model are available in this repository. The model and its intent are described in detail in the paper linked above.

## Prerequisites
In order to run this code, you will need:
* EHR data in the OMOP data model
* At least some COVID positive patients in your data, indicated through positive PCR or antigen tests (LOINC-coded) or U07.1 diagnosis codes.
* The ability to run Python against your OMOP data model

The SQL code in this repository is written in the Spark SQL dialect. If you have a different RDBMS, most of the SQL will work but you will likely need to swap out a few functions here and there. The Python code in this repository is written in PySpark. As PySpark is not very common, we will provide a pandas translation soon (see Future version notes).

## Running our code
This repository is intended to be run in a stepwise fashion, using the numbered folders. (I.e., first run all the scripts in 1_, then 2_, and so forth.) Each numbered folder has its own README inside with additional details.

## Future version notes
In our next update, we will: 
* release code to enable users to retrain the model on their own data, rather than using ours.
* provide a pandas translation of the PySpark model code for ease of use
* provide a list of Python packages/versions in the "Prerequsite" section above


