from pyspark.sql import functions as F
import re


# Read in the file containing the list of model features, one per line
# returns cols_for_model, which is used in several other functions
def read_model_columns():

    f = open('feature_list.txt', 'r')
    lines = f.readlines()
    cols_for_model = [l.strip() for l in lines]
    f.close()
    return cols_for_model



def pivot_dx(dx_df, cols_for_model):

    # Filter only to dx used in model and then pivot
    # This greatly improves performance as both spark and pandas do poorly with very wide datasets

    dx_df = dx_df.filter(dx_df["high_level_condition"].isin(cols_for_model))    
    dx_df = dx_df.groupby("person_id").pivot("high_level_condition").agg(F.max("greater_in_post").alias("greater_in_post"))
    
    # the absence of a diagnosis record means it is neither greater in post or only in post
    dx_df = dx_df.fillna(0)

    return dx_df


def pivot_meds(med_df, cols_for_model):
    
    # Filter only to meds used in the canonical all patients model and then pivot
    # This greatly improves performance as both spark and pandas do poorly with very wide datasets
    
    med_df = med_df.filter(med_df["ingredient"].isin(cols_for_model))    
    med_df = med_df.groupby("person_id").pivot("ingredient").agg(F.max("post_only_med").alias("post_only_med"))
    
    # if there is no row for a patient:drug combination, there will be nulls in the pivot.  This is converted to 0 to represent the absence of a drug exposure.
    med_df = med_df.fillna(0)

    return med_df


def build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post):

    count_dx = count_dx_pre_and_post

    df = add_labels.join(med_df, on="person_id",  how="left")
    df = df.join(dx_df, on='person_id', how='left')
    df = df.join(count_dx, on='person_id', how='left')


    # Some patients in the condition data aren't in the drug dataset
    # meaning they don't have any drugs in the relevant period 
    df = df.fillna(0)


    result = df
    
    drop_cols = []
    cols = result.columns
    for c in cols:

        # drop ALL the race and ethnicity columns
        if re.match('^race_', c) or re.match('^ethn', c):
            drop_cols.append(c)

        # Among the sex columns, keep only male
        if re.match('^sex_', c) and c != 'sex_male':
            drop_cols.append(c)

    # drop the 'no' versions of disease history, keeping the 'yes' versions
    # drop disorder by body site - too vague
    drop_cols.extend(["diabetes_ind_no", "kidney_ind_no", "chf_ind_no", "chronicpulm_ind_no", "patient_group", "disorder_by_body_site"])

    result = result.drop(*drop_cols)


    return result
