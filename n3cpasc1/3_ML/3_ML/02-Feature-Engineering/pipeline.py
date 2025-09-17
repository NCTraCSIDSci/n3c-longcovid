import pyspark.sql.functions as F
import sklearn
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.26301130-35b1-4abc-a265-9aa9aa4afe4a"),
    remove_excluded_conditions=Input(rid="ri.foundry.main.dataset.3278f1dc-f77c-4dcf-8f29-f666d2538c6e")
)
def just_cases(remove_excluded_conditions):

    
    df = remove_excluded_conditions

    df = df[df["patient_group"].isin('CASE_NONHOSP', 'CASE_HOSP')]

    # Set everything to 1 if it is present, 0 otherwise (don't treat 3 diagnoses of fatigue as different from 1). EXAMINE THIS ASSUMPTION
    #df = df.withColumn("post_dx_count", F.when(df["post_dx_count"] > 0, 1).otherwise(0))

    df = df.withColumn("condition_concept_name", F.lower(F.regexp_replace(df["condition_concept_name"], "[^A-Za-z_0-9]", "_" )))
    df = df.withColumn("high_level_condition", F.lower(F.regexp_replace(df["high_level_condition"], "[^A-Za-z_0-9]", "_" )))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d9d84ca-d6f8-4ca3-aa18-48e7bb68abe0"),
    drug_features=Input(rid="ri.foundry.main.dataset.1d25ada4-dcaf-4437-ac22-d830ccb7a5e0")
)
def pivot_drugs(drug_features):
    df = drug_features

    df = df.withColumn("ingredient", F.regexp_replace(df["ingredient"], r'[^A-Za-z0-9]', '_') )
    df = df.groupby("person_id").pivot("ingredient").agg(F.max("post_only_med").alias("post_only_med"))
    df = df.fillna(0)
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5cac5244-f76c-4c70-84a5-eef337acd6e5"),
    just_cases_clean_rollup=Input(rid="ri.foundry.main.dataset.3bf4799d-a693-49c6-9b44-e3d732038f98")
)
def pivot_just_cases( just_cases_clean_rollup):
    just_cases_clean_rollup = just_cases_clean_rollup
 
    df = just_cases_clean_rollup

    df = df.withColumn("high_level_condition", F.lower(F.regexp_replace(df["high_level_condition"], "[^A-Za-z_0-9]", "_" )))

    # make the conditions the columns, and the maximum value (max = 1) of the diagnosis as the value
    df = df.groupby("person_id", "patient_group").pivot("high_level_condition").agg(F.max("greater_in_post").alias("greater_in_post"), F.max("only_in_post").alias("only_in_post") )

    # 

    
    df = df.fillna(0)
    df = df.repartition(1)
    return df

