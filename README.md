# N3C Long COVID (PASC) model

## Introduction

This repository contains reproducible code for computational phenotyping (automated EHR-based identification) of PASC (Post-Acute Sequelae of COVID-19) or Long COVID, developed for use in N3C (National Clinical Cohort Collaborative, formerly known as the National COVID Cohort Collaborative). 

The first version, described in [Identifying who has long COVID in the USA: a machine learning approach using N3C data](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(22)00048-6/fulltext), relied on the identification of a specific COVID index date for the determination of whether or not a patient had PASC. This paper may be cited as:

    Pfaff ER, Girvin AT, Bennett TD, et al. Identifying who has long COVID in the USA: a machine learning approach using N3C data. Lancet Digital Health. 4(7),E532-E541. doi:10.1016/S2589-7500(22)00048-6

The second version, described in [Re-engineering a machine learning phenotype to adapt to the changing COVID-19 landscape: a machine learning modelling study from the N3C and RECOVER consortia](https://www.thelancet.com/journals/landig/article/PIIS2589-7500(25)00069-X/fulltext), was updated based on the changing circumstances surrounding COVID-19 surveillance. It does not rely on the identification of a specific COVID index date for the determination of whether or not a patient had PASC. This paper may be cited as:

    Crosskey M, McIntee T, Preiss S, et al. Re-engineering a machine learning phenotype to adapt to the changing COVID-19 landscape: a machine learning modelling study from the N3C and RECOVER consortia. Lancet Digital Health. 7(8). [doi:10.1016/S2589-7500(22)00048-6](https://doi.org/10.1016/j.landig.2025.100887)

Note that the code on the main branch of this repository has been updated since the publication of the paper, and will continue to be updated. The "published_paper_code" branch presents a fixed snapshot of the code as it existed contemporary to the publication of the first paper. The "second_paper_code" branch presents a fixed snapshot of the code as it existed contemporary to the publication of the second paper.

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


