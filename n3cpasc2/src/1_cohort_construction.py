from pyspark.sql import functions as F
from pyspark.sql import Window

def covid_cohort(Pasc_all_patients_fact_day_combine):
    """
    This function is used to generate an intermediate table for the cohort_and_idx and infection_dates tables.


    """

    ###############################################
    ## Pull cols for COVID Cohort Identification ##
    ###############################################

    all_patients_visit_table = Pasc_all_patients_fact_day_combine.select('person_id', 'date', 'PCR_AG_Pos', 'LL_COVID_diagnosis', 'PAX1_NIRMATRELVIR', 'PAX2_RITONAVIR', 'PAXLOVID', 'REMDISIVIR', 'LL_Long_COVID_diagnosis', 'B94_8', 'LL_MISC') \
        .where(F.col('date') >= "2020-03-01")

    variable1 = "PAX1_NIRMATRELVIR"
    variable2 = "PAX2_RITONAVIR"
    same_day_variable = "same_day_occurrence_nirm_rit"
    all_patients_visit_table = all_patients_visit_table.withColumn(same_day_variable, F.when((F.col(variable1)==1)&(F.col(variable2)==1), 1).otherwise(0))
    variable3 = "PAXLOVID"
    variable4 = same_day_variable
    either_variable = "either_pax_or_same_day_nirm_rit"
    all_patients_visit_table = all_patients_visit_table.withColumn(either_variable, F.when((F.col(variable3)==1)|(F.col(variable4)==1), 1).otherwise(0))
    all_patients_visit_table = all_patients_visit_table.drop('PAX1_NIRMATRELVIR', 'PAX2_RITONAVIR', 'same_day_occurrence_nirm_rit', 'PAXLOVID')

    ##################################
    ## Get index based on Test/Diag ##
    ##################################

    pcr_df = all_patients_visit_table.where(F.col('PCR_AG_Pos') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('COVID_first_PCR_or_AG_lab_positive'))

    dx_df = all_patients_visit_table.where(F.col('LL_COVID_diagnosis') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('COVID_first_diagnosis_date'))

    long_covid_df = all_patients_visit_table.where(F.col('LL_Long_COVID_diagnosis') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('Long_COVID_first_diagnosis_date'))

    B94_8_df = all_patients_visit_table.where(F.col('B94_8') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('B94_8_first_diagnosis_date'))

    pax_df = all_patients_visit_table.where(F.col('either_pax_or_same_day_nirm_rit') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('first_pax_date'))

    rem_df = all_patients_visit_table.where(F.col('REMDISIVIR') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('first_rem_date'))

    MISC_df = all_patients_visit_table.where(F.col('LL_MISC') == 1) \
        .groupby('person_id') \
        .agg(F.min('date').alias('MISC_first_diagnosis_date'))

    ######################################
    ## Combine all possible index dates ##
    ######################################

    df = pcr_df.join(dx_df, 'person_id', 'outer')
    df = df.join(long_covid_df, 'person_id', 'outer')
    df = df.join(B94_8_df, 'person_id', 'outer')
    df = df.join(pax_df, 'person_id', 'outer')
    df = df.join(rem_df, 'person_id', 'outer')
    df = df.join(MISC_df, 'person_id', 'outer')

    ##############################
    ## Find Earliest Index Date ##
    ##############################

    #add a column for the earlier of the diagnosis and lab test dates for all confirmed covid patients
    df = df.withColumn("COVID_first_poslab_or_diagnosis_date", F.least(df.COVID_first_PCR_or_AG_lab_positive, df.COVID_first_diagnosis_date))

    return df

def infection_dates(covid_cohort,  Pasc_all_patients_fact_day_combine):
    """
    This is generated from the cohort and fact table, and is the end 
    """

    all_patients_visit_table = Pasc_all_patients_fact_day_combine.select('person_id', 'date', 'PCR_AG_Pos')

    cohort_df = covid_cohort.select('person_id', 'COVID_first_poslab_or_diagnosis_date')

    # Label index infection as 0
    index_infection_df = cohort_df.withColumnRenamed('COVID_first_poslab_or_diagnosis_date', 'infection_date') \
        .withColumn('infection_number', F.lit(0)) \
        .select('person_id', 'infection_date', 'infection_number')

    ##################################
    # Find Eligible Reinfection Dates
    ##################################

    reinfection_day_threshold = 60

    # Get all positive test dates
    positives_df = all_patients_visit_table \
        .where(F.col("PCR_AG_Pos") == 1)
    print("Got all positive test dates")

    # Limit to persons in COHORT
    positives_df = cohort_df.join(positives_df, how='left', on='person_id')

    # Calculate date difference with earliest diagnosis (not always PCR_AG_Pos)
    reinfection_df = positives_df.withColumn("days_postest_from_diagnosis", F.datediff(F.col("date"), \
    F.col("COVID_first_poslab_or_diagnosis_date"))) \
    .where(F.col("days_postest_from_diagnosis") > reinfection_day_threshold)

    w = Window.partitionBy('person_id')
    reinfection_df = reinfection_df.select('person_id', 'date').withColumn('infection_date', \
                    F.min('date').over(w))
    reinfection_df = reinfection_df.withColumn('infection_number', F.lit(1))

    # Join index infection with first reinfection
    r_df = reinfection_df.select('person_id', 'infection_date', 'infection_number') \
        .union(index_infection_df)

    print("Calculated First Reinfection")

    # Loop over new reinfection dates and find new reinfections (only PCR_AG_Pos)
    onward = True
    i = 2
    while onward:
        print("Calcuted Reinfection " + str(i))
        reinfection_df = positives_df.join(r_df \
        .where(F.col("infection_number") == (i - 1)), on='person_id', how='left')

        # Calculate date difference with previous reinfection
        reinfection_df = reinfection_df.withColumn("days_postest_from_reinfection", F.datediff(F.col("date"), \
        F.col("infection_date"))) \
        .where(F.col("days_postest_from_reinfection") > reinfection_day_threshold)

        # End loop when no more eligible refinfections
        if not reinfection_df.count() > 0:
            break

        w = Window.partitionBy('person_id')
        reinfection_df = reinfection_df.select('person_id', 'date').withColumn('infection_date', \
                                                F.min('date').over(w))
        reinfection_df = reinfection_df.withColumn('infection_number', F.lit(i))

        r_df = r_df.union(reinfection_df.select('person_id', 'infection_date', 'infection_number')).drop_duplicates()
        i += 1

    print("Completed Calculation of Subsequent Reinfections")

    return r_df

def cohort_and_idx(covid_cohort):
    """
    
    """
    df = covid_cohort
    df = df.withColumnRenamed('COVID_first_PCR_or_AG_lab_positive','pos_test_idx')
    df = df.withColumnRenamed('COVID_first_diagnosis_date','u07_any_idx')
    # Below line has been updated to fix the "null date" bug in v185:
    df = df.withColumn('pax_or_rem_idx',F.when(df['first_pax_date'] < df['first_rem_date'],df['first_pax_date']).otherwise(F.coalesce(df['first_rem_date'], df['first_pax_date'])))
    
    # # add the minimum
    df = df.withColumn('cur_min',F.when(
        df['pos_test_idx'].isNotNull() & 
        ((df['pos_test_idx'] < df['u07_any_idx']) | df['u07_any_idx'].isNull()),
        df['pos_test_idx']).otherwise(df['u07_any_idx']))

    
    df = df.withColumn('RECOVER_covid_index_date',F.when(
        df['cur_min'].isNotNull() & 
        ((df['cur_min'] < df['pax_or_rem_idx']) | df['pax_or_rem_idx'].isNull()),
        df['cur_min']).otherwise(df['pax_or_rem_idx']))
        
    df = df[['person_id','pos_test_idx','u07_any_idx','pax_or_rem_idx', 'RECOVER_covid_index_date']]
    df = df.filter(df['RECOVER_covid_index_date'].isNotNull())
    return df

def blackout_dates(cohort_and_idx, infection_dates):
    # Black out the time around infection dates:
    infection_based = (
        cohort_and_idx
        .join(infection_dates, on="person_id", how="inner")
        .select(
            F.col("person_id"),
            F.col("infection_date"),
            F.date_add(F.col("infection_date"), -7).alias("blackout_begin"),
            F.date_add(F.col("infection_date"), 28).alias("blackout_end")
        )
    )
    # Also black out time around inferred infections based on positive test dates, u07 diagnoses, and pax / rem prescriptions.
    diag_rx_based = (
        cohort_and_idx
        .filter(
            F.col("pos_test_idx").isNotNull() |
            F.col("u07_any_idx").isNotNull() |
            F.col("pax_or_rem_idx").isNotNull()
        )
        .withColumn("infection_date", F.array_min(F.array("pos_test_idx", "u07_any_idx", "pax_or_rem_idx")))
        .withColumn("blackout_begin", F.date_add(F.col("infection_date"), -7))
        .withColumn("blackout_end", F.date_add(F.col("infection_date"), 28))
        .select("person_id", "infection_date", "blackout_begin", "blackout_end")
    )
    union_df = infection_based.unionByName(diag_rx_based).distinct()
    return union_df

def basic_cohort(recover_release_person, cohort_and_idx, window_spans):
    df = recover_release_person[['person_id','year_of_birth','gender_concept_name']]
    df2 = cohort_and_idx[['person_id']]
    df = df2.join(df, on='person_id')

    df = df.join(window_spans[['window_name','window_start']],how='outer')
    df = df.withColumn('window_age', F.year(df['window_start']) - df.year_of_birth)

    df = df.withColumnRenamed('gender_concept_name','sex')
    df = df.drop('year_of_birth')
    df = df.drop('window_start')
    df = df.withColumnRenamed('window_name','window')

    df = df.filter(df['window_age'] >= 18)
    df = df.filter(df['window_age'] < 100)
    # Below line is used for faster testing cycles. If uncommented in a release branch, something is wrong.
    # df = df.limit(100000)
    return df

def labels(recover_release_condition_occurrence, 
           basic_cohort, 
           recover_release_observation):
    """
    Generate labels for the training set.
    Labels are defined as having either:
        2+ U09.9 diagnoses,
        2+ B94.8 diagnoses, 
        2+ LC clinic visits.
    The label date is the earliest of:
        The first date of U09.9 diagnosis (if 2+ U09.9 diagnoses),
        The first date of B94.8 diagnosis (if 2+ B94.8 diagnoses),
        The first date of LC clinic visit (if 2+ LC clinic visits).
    The LC clinic visit code is 2004207791. This is not a standard OMOP concept.
    U09.9 is 705076.
    B94.8 is 36714927.
    If none of these three codes are present 2+ times for any person, this will break the pipeline."""
    u99_b948_df = recover_release_condition_occurrence \
        .filter(
            (F.col("condition_concept_id").isin(705076, 36714927)) &
            (F.col("condition_start_date") >= "2020-02-01")
        )

    skinny_conditions = basic_cohort \
        .join(
            u99_b948_df, on="person_id", how="inner"
        ) \
        .select(
            F.col("person_id"),
            F.col("condition_concept_id"),
            F.col("condition_start_date")
        ) \
        .distinct()

    u09_df = skinny_conditions \
        .filter(
            F.col("condition_concept_id") == 705076
        ) \
        .withColumnRenamed("condition_start_date", "u09_start_date") \
        .select("person_id", "u09_start_date") \
        .distinct()

    b948_df = skinny_conditions \
        .filter(
            F.col("condition_concept_id") == 36714927
        ) \
        .withColumnRenamed("condition_start_date", "b948_start_date") \
        .select("person_id", "b948_start_date") \
        .distinct()

    U099_b948_counts_and_mins = basic_cohort \
        .join(u09_df, on="person_id", how="left") \
        .join(b948_df, on="person_id", how="left") \
        .groupBy("person_id") \
        .agg(
            F.count("u09_start_date").alias("num_u09s"),
            F.min("u09_start_date").alias("min_u09_date"),
            F.count("b948_start_date").alias("num_b948s"),
            F.min("b948_start_date").alias("min_b948_date")
        ) \
        .filter(
            (F.col("num_u09s") > 0) | (F.col("num_b948s") > 0)
        )

    # LC clinic visits
    LC_obs_df = recover_release_observation \
        .filter(
            F.col("observation_concept_id") == 2004207791
        ) \
        .select("person_id","observation_date") \
        .distinct()

    LC_clinic_visits = basic_cohort \
        .join(
            LC_obs_df, on="person_id", how="inner"
        ) \
        .groupBy("person_id") \
        .agg(
            F.min("observation_date").alias("TRAIN_min_lc_clinic_date"),
            F.count("observation_date").alias("TRAIN_num_lc_clinic_visits")
        )

    df = U099_b948_counts_and_mins
    df = df.join(LC_clinic_visits, on='person_id', how='outer')
    df = df.na.fill(value=0,subset=["TRAIN_num_lc_clinic_visits", "num_u09s","num_b948s"])
    df = df.withColumn('label',(df['TRAIN_num_lc_clinic_visits'] >= 2) | (df['num_u09s'] >= 2))
    df = df.withColumn('label_date',F.when(df['TRAIN_num_lc_clinic_visits'] >= 2,df['TRAIN_min_lc_clinic_date']).otherwise(df['min_u09_date']))
    df = df.filter(df['label'])
    return df
    
def post_visit_counts_in_windows(basic_cohort, blackout_dates, recover_release_microvisit_to_macrovisit, window_spans):
    # join the cohort with the blackout dates
    df = basic_cohort
    df = df.groupBy('person_id').count()
    bd = blackout_dates
    df = df.join(bd, on='person_id',how='left')
    df = df[['person_id','blackout_begin','blackout_end']]
    
    # trim the microvisit to the bare minimum required for output to save memory where possible
    mic = recover_release_microvisit_to_macrovisit
    mic = mic[['person_id',
               'visit_occurrence_id',
               'visit_start_date',
               # 'visit_concept_name',
               # 'macrovisit_start_date',
               # 'macrovisit_end_date', 
               'likely_hospitalization']]
    
    # join blackout dates to find all the blacked out visit_occurrence_id's
    df = df.join(mic, on='person_id')
    df = df.filter(df['visit_start_date'] >= df['blackout_begin'])
    df = df.filter(df['visit_start_date'] <= df['blackout_end'])
    df = df[['visit_occurrence_id']]

    # anti join to rid ourselves of the blacked out visits
    visits_and_hosps_for_cohort = mic.join(df, on='visit_occurrence_id', how='leftanti')

    cohort_with_windows = basic_cohort.join(
        window_spans,
        basic_cohort["window"] == window_spans["window_name"],
        how="inner"
    )

    postv_NH = visits_and_hosps_for_cohort.filter(F.col("likely_hospitalization").isNull())
    nonhosp_join = cohort_with_windows.join(
        postv_NH,
        (cohort_with_windows["person_id"] == postv_NH["person_id"]) &
        (postv_NH["visit_start_date"] >= cohort_with_windows["window_start"]) &
        (postv_NH["visit_start_date"] <= cohort_with_windows["window_end"]),
        how="left"
    )
    nonhosp_join = nonhosp_join.drop(postv_NH["person_id"])

    postv_H = visits_and_hosps_for_cohort.filter(F.col("likely_hospitalization") == 1)
    hosp_join = cohort_with_windows.join(
        postv_H,
        (cohort_with_windows["person_id"] == postv_H["person_id"]) &
        (postv_H["visit_start_date"] >= cohort_with_windows["window_start"]) &
        (postv_H["visit_start_date"] <= cohort_with_windows["window_end"]),
        how="left"
    )
    hosp_join = hosp_join.drop(postv_H["person_id"])
    

    nonhosp_counts = nonhosp_join.groupBy("person_id", "window") \
        .agg(F.countDistinct("visit_start_date").alias("post_nonhosp_visit_count"))

    hosp_counts = hosp_join.groupBy("person_id", "window") \
        .agg(F.countDistinct("visit_start_date").alias("post_hosp_count"))

    result = nonhosp_counts.join(
        hosp_counts,
        on=["person_id", "window"],
        how="outer"
    )
    return result
