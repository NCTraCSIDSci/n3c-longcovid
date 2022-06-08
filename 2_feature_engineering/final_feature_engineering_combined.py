
# STEP 1: pre_post_med_final_distinct #####################################################################################

from pyspark.sql import functions as F

# Output pyspark dataframe:  pre_post_med_final_distinct
# Input  pyspark dataframe:  pre_post_med_clean

def build_pre_post_med_final_distinct(pre_post_med_clean):
    df = pre_post_med_clean
    df = df.withColumn('post_only_med', F.when(df['pre_med_count'] > 0, 0).otherwise(1))

    # Not currently using this feature in the mmodel
    df = df.withColumn('more_in_post',  F.when(df['post_med_count'] > df['pre_med_count'], 1).otherwise(0))

    df = df.select(df['person_id'], df['ancestor_drug_concept_name'].alias('ingredient'), df['post_only_med'])
    df = df.distinct()

    df = df.withColumn("ingredient", F.lower(F.regexp_replace(df["ingredient"], "[^A-Za-z_0-9]", "_" )))
    
    return df




# STEP 2: add_labels ##################################################################################### 

from pyspark.sql import functions as F
import pandas as pd

# Output pyspark dataframe:  add_labels
# Input  pyspark dataframe:  SparkContext, pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients

# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.SparkContext.html

def build_add_labels(ctx, pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients):


    # total patients

    # There are patients in the meds table that are not in the dx table and vice versa
    # This transform generates the full list of patients in either table (the union)
    # Along with the fields that are defined for every patient (sex, group, age etc)


    df_unique_med_rows = pre_post_med_count_clean.select(['person_id', 'sex', 'patient_group', 'apprx_age', 'race', 'ethn', 'diabetes_ind', 'kidney_ind', 'CHF_ind', 'ChronicPulm_ind', 'tot_long_data_days', 'op_post_visit_ratio', 'ip_post_visit_ratio']).distinct()
    
    df_unique_dx_rows = pre_post_dx_count_clean.select(['person_id', 'sex', 'patient_group', 'apprx_age', 'race', 'ethn', 'diabetes_ind', 'kidney_ind', 'CHF_ind', 'ChronicPulm_ind', 'tot_long_data_days', 'op_post_visit_ratio', 'ip_post_visit_ratio']).distinct()

    # pyspark union is equivalent to union all in SQL
    df = df_unique_med_rows.union(df_unique_dx_rows).distinct()
    
    # Encode dummy variables
    def cleanup(s):
        if s.lower() in ('unknown', 'gender unknown', 'no matching concept'):
            return 'unknown'
        else:
            return s


    df = df.toPandas()

    df['sex'] = [cleanup(s) for s in df['sex']]

    # encode these caterogical variables as binaries
    df = pd.get_dummies(df, columns=["sex","race","ethn","diabetes_ind","kidney_ind", "CHF_ind", "ChronicPulm_ind"])
    df = df.rename(columns = lambda c: str.lower(c.replace(" ", "_")))
    df = ctx.spark_session.createDataFrame(df)

    
    # Add Labels
    final_cols = df.columns
    final_cols.extend(["long_covid", "hospitalized"])

    df = df.join(long_covid_patients, on='person_id', how='left')
    
    # Join with the long covid clinic data to build our labels (long_covid)
    df = df.withColumn("long_covid", F.when(df["min_long_covid_date"].isNotNull(), 1).otherwise(0))
    df = df.withColumn("hospitalized", F.when(df["patient_group"] == 'CASE_HOSP', 1).otherwise(0))


    df = df.select(final_cols)
    

    return df



# STEP 3:  pre_post_more_in_dx_calc #####################################################################################


# Output pyspark dataframe:  pre_post_dx_more_in_post_calcpre_post_dx_more_in_post_calc
# Input  pyspark dataframe:  pre_post_dx


def build_pre_post_more_in_dx_calc(pre_post_dx):
    
    result = pre_post_dx
    result = result.withColumn('post_only_dx', 
                                F.when(result['pre_dx_count'] > 0, 0)
                                .otherwise(1)
                              )
    
    result = result.withColumn('more_in_post',
                                F.when(result['post_dx_count'] > result['pre_dx_count'], 1)
                                .otherwise(0)
                              )

    
    return resultdef process(pre_post_dx):
    


# STEP 4:  condition_rollup #####################################################################################

# Output pyspark dataframe:  condition_rollup
# Input  pyspark dataframe:  long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept
# NOTE: concept and concept_ancestor are standard OMOP vocabulary tables



def build_condition_rollup(long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept):
   
    pp = pre_post_dx_count_clean.alias('pp')
    ca = concept_ancestor.alias('ca')
    ct = concept.alias('ct')
    lc = long_covid_patients.alias('lc')

    df = pp.join(lc, on='person_id', how='inner')
    df = df.join(ca, on=[df.condition_concept_id == ca.descendant_concept_id], how='inner')
    df = df.join(ct, on=[df.ancestor_concept_id  == ct.concept_id], how='inner')

    df = df.filter( ~df['concept_name'].isin(
                                                ['General problem AND/OR complaint',
                                                'Disease',
                                                'Sequelae of disorders classified by disorder-system',
                                                'Sequela of disorder',
                                                'Sequela',
                                                'Recurrent disease',
                                                'Problem',
                                                'Acute disease',
                                                'Chronic disease',
                                                'Complication'
                                                ]))
    
    generic_codes = ['finding', 'disorder of', 'by site', 'right', 'left']

    for gc in generic_codes:
        df = df.filter( ~F.lower(ct.concept_name).like('%' + gc + '%') )
        
        if gc not in ['right', 'left']:
            df = df.filter( ~F.lower(pp.condition_concept_name).like('%' + gc + '%') )

    df = df.filter(ca.min_levels_of_separation.between(0,2))


    
    df = df.groupby(['ct.concept_name', 'pp.condition_concept_name', 'pp.condition_concept_id', 
                    'ca.min_levels_of_separation', 'ca.max_levels_of_separation']).agg(F.countDistinct('pp.person_id').alias('ptct_training'))

    df = df.withColumnRenamed('concept_name', 'parent_concept_name')
    df = df.withColumnRenamed('condition_concept_name', 'child_concept_name')
    df = df.withColumnRenamed('min_levels_of_separation', 'min_hops_bt_parent_child')
    df = df.withColumnRenamed('max_levels_of_separation', 'max_hops_bt_parent_child')
    df = df.withColumnRenamed('condition_concept_id', 'child_concept_id')

    return df



# STEP 5: parent_condition_rollup ######################################################################################

# Output pyspark dataframe:  parent_condition_rollup
# Input  pyspark dataframe:  condition_rollup


from pyspark.sql import functions as F

def build_parent_condition_rollup(condition_rollup):
    df = condition_rollup
    df = df.groupby('parent_concept_name').agg(F.sum('ptct_training').alias('total_pts'))
    df = df[df['total_pts'] >= 3]
    
    return df



# STEP 6: final_rollup ######################################################################################

# Output pyspark dataframe:  final_rollup
# Input  pyspark dataframe:  parent_conditions, condition_rollup

def process(parent_conditions, condition_rollup):
    pc = parent_conditions
    dm = condition_rollup

    df = pc.join(dm, on='parent_concept_name', how='inner')
    df = df.select(['parent_concept_name', 'child_concept_name']).distinct()

    return df




# STEP 7: add_alt_rollup ######################################################################################

from pyspark.sql import functions as F


# Output pyspark dataframe:  add_alt_rollup
# Input  pyspark dataframe:  pre_post_dx_more_in_post, final_rollups

def build_add_alt_rollup(pre_post_dx_more_in_post, final_rollups):

    pre_post_dx_final = pre_post_dx_more_in_post

    condition = [pre_post_dx_final['condition_concept_name'] == final_rollups['child_concept_name'] ]
    
    df = pre_post_dx_final.join(final_rollups.select(['child_concept_name', 'parent_concept_name']), how='left', on=condition)
    
    df = df.drop('child_concept_name')
    df = df.withColumnRenamed('parent_concept_name', 'high_level_condition')

    return df


# STEP 8: pre_post_dx_final ######################################################################################


from pyspark.sql import functions as F


# Output pyspark dataframe:  pre_post_dx_final
# Input  pyspark dataframe:  add_alt_rollup


def build_pre_post_dx_final(add_alt_rollup):
    
    df = add_alt_rollup
    df = df.filter(df['high_level_condition'] != 'EXCLUDE')
    df = df.filter(df['high_level_condition'].isNotNull())
    df = df.filter(~F.lower(df['high_level_condition']).like('%covid%') )
    df = df.filter(~F.lower(df['high_level_condition']).like('%coronav%') )

    df = df.filter(~F.upper(df['high_level_condition']).like('%post_infectious_disorder%') )
    

    df = df[df["patient_group"].isin('CASE_NONHOSP', 'CASE_HOSP')]
    df = df.withColumn("condition_concept_name", F.lower(F.regexp_replace(df["condition_concept_name"], "[^A-Za-z_0-9]", "_" )))
    df = df.withColumn("high_level_condition", F.lower(F.regexp_replace(df["high_level_condition"], "[^A-Za-z_0-9]", "_" )))


    # Pre-post_dx_final
    # Multiple conditions map to each high_level_condition, so we sum there here
    df = df.groupby(["high_level_condition", "person_id", "patient_group"]).agg(
                            F.sum('pre_dx_count').alias('pre_dx_count_sum'), 
                            F.sum('post_dx_count').alias('post_dx_count_sum')
                            )
    df = df.filter(df["high_level_condition"].isNotNull())

    # does the condition occur more often in after the covid acute phase?
    df = df.withColumn('greater_in_post', F.when(df['post_dx_count_sum'] > df['pre_dx_count_sum'], 1).otherwise(0))

    # does the condition ONLY occur after the covid acute phase?
    df = df.withColumn('only_in_post', F.when( ((df['pre_dx_count_sum'] == 0) & (df['post_dx_count_sum'] > 0)), 1).otherwise(0) )
    ###
    
    return df

# STEP 9: count_dx_pre_and_post ######################################################################################

from pyspark.sql import functions as F


# Output pyspark dataframe:  count_dx_pre_and_post
# Input  pyspark dataframe:  pre_post_dx_final


def build_count_dx_pre_and_post(pre_post_dx_final):

    
    # sum the total pre and post dx (of any kind, for use later in a filter)

    df = pre_post_dx_final

    total_pre = df.groupby('person_id').agg(F.sum('pre_dx_count_sum').alias('total_pre_dx'))
    
    total_post = df.groupby('person_id').agg(F.sum('post_dx_count_sum').alias('total_post_dx'))

    distinct_people = df.select('person_id').distinct()

    result = distinct_people.join(total_pre, on='person_id', how='left')
    result = result.join(total_post, on='person_id', how='left')

    return result


 # STEP 10: feature_table_all_patients ######################################################################################


# Output pyspark dataframe:  count_dx_pre_and_post
# Input  pyspark dataframe:  pre_post_dx_final

def compute(training_data, add_labels, pre_post_med_final, pre_post_dx_final, count_dx_pre_and_post):

    # Filter only to meds used in the canonical all patients model and then pivot
    med_df = utils.pivot_meds(pre_post_med_final, training_data)
    
    dx_df = utils.pivot_dx(pre_post_dx_final, training_data)

    result = utils.build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post)

    return result



