from pyspark.sql import functions as F


def conditions_for_the_cohort(basic_cohort, blackout_dates, recover_release_condition_occurrence, recover_release_observation):
    # Transform observation data
    obs_transformed = recover_release_observation.select(
        F.concat(F.lit("OBS-"), F.col("observation_id")).alias("condition_occurrence_id"),
        F.col("person_id"),
        F.col("observation_date").alias("condition_start_date"),
        F.col("observation_concept_id").alias("condition_concept_id"),
        F.col("observation_concept_name").alias("condition_concept_name")
    )

    # Select relevant columns from condition_occurrence
    cond_selected = recover_release_condition_occurrence.select(
        "condition_occurrence_id",
        "person_id",
        "condition_start_date",
        "condition_concept_id",
        "condition_concept_name"
    )

    # Union and drop duplicates
    observations_and_conditions = obs_transformed.union(cond_selected)
    
    # join the cohort with the blackout dates
    df = basic_cohort
    df = df.groupBy('person_id').count()
    bd = blackout_dates
    df = df.join(bd, on='person_id',how='left')
    df = df[['person_id','blackout_begin','blackout_end']]
    
    # trim the conditions to the bare minimum required for output to save memory where possible
    cond = observations_and_conditions
    cond = cond[['person_id','condition_occurrence_id','condition_start_date','condition_concept_id','condition_concept_name']]
    
    # join blackout dates to find all the blacked out condition_occurrence_id's
    df = df.join(cond, on='person_id')
    df = df.filter(df['condition_start_date'] >= df['blackout_begin'])
    df = df.filter(df['condition_start_date'] <= df['blackout_end'])
    df = df[['condition_occurrence_id']]

    # anti join to rid ourselves of the blacked out visits
    out = cond.join(df, on='condition_occurrence_id', how='leftanti')
    return out

def selected_features(conditions_for_the_cohort, 
                    concept_ancestor, 
                    recover_release_concept,
                    cutoff_value = 25_000,
                    ancestor_blacklist = [
                        '4322976', #Procedure
                        '4254480' #Education
                    ],
                    descendant_blacklist = [
                        '36714927', # Sequelae of infectious disease
                        '37311061', # COVID-19
                        '4041283', # General finding of observation of patient
                        '4100065', # Disease due to Coronaviridae
                        '432453', # General clinical state finding
                        '4025368', # General information qualifier
                        '4203722', # Patient encounter procedure
                        '4086921', # Situation with explicit context
                        '4152283', # Main spoken language
                        '4269989', # Health-related behavior finding
                        '4062626', # Immunization education
                        '4137274', # Discharge to establishment
                        '4283657', # Sexual orientation
                        '4195981', # Medical records review
                        '4189457', # Clinical finding absent
                        '4110772', # Gender identity finding
                        '441482', # Administrative reason for encounter
                        '4130003', # Documentation procedure
                        '4307376', # Final inpatient visit with instructions at discharge
                        '441840', # Clinical finding
                        '4043042', # Sexually active
                        '4180628', # Disorder of body system
                        '4174707', # Dietary management surveillance
                        '4247398', # Health status
                        '43021202', # Sexually active with men
                        '4119499' # Not for resuscitation                    
                        ]):
    # MARKED FOR EXPORT
    #Parameters
    # Create full feature blacklist:
    blacklisted_features = concept_ancestor.filter(
        (F.col("ancestor_concept_id").isin(ancestor_blacklist)) |
        (F.col("descendant_concept_id").isin(descendant_blacklist))
    )
    # Generate SNOMEDgraph
    rr1 = recover_release_concept \
        .filter(
            (F.col("vocabulary_id") == "SNOMED") &
            (F.col("standard_concept") == "S") &
            (F.col("domain_id").isin("Condition", "Observation"))
        ) \
        .select(
            F.col("concept_id").alias("ancestor_concept_id"),
            F.col("concept_name").alias("anc_concept_name")
        )

    rr2 = recover_release_concept \
        .filter(
            (F.col("vocabulary_id") == "SNOMED") &
            (F.col("standard_concept") == "S") &
            (F.col("domain_id").isin("Condition", "Observation"))
        ) \
        .select(
            F.col("concept_id").alias("descendant_concept_id"),
            F.col("concept_name").alias("des_concept_name")
        )

    concept_ancestor_selected = concept_ancestor.select(
        "ancestor_concept_id",
        "descendant_concept_id",
        "min_levels_of_separation",
        "max_levels_of_separation"
    )

    SNOMEDgraph = concept_ancestor_selected \
        .join(rr1, "ancestor_concept_id") \
        .join(rr2, "descendant_concept_id")

    
    cond_counts = conditions_for_the_cohort.groupBy(['condition_concept_id']).count()
    # Use the ancestor table to aggregate each ancestor over the maximum count of each of its descendants

    df = cond_counts.join(SNOMEDgraph, cond_counts['condition_concept_id'] == SNOMEDgraph['descendant_concept_id'])
    df = df.groupBy(['ancestor_concept_id']).agg(F.sum('count').alias('sum_desc_count'))
    ancestor_counts = SNOMEDgraph.join(df, on='ancestor_concept_id')
    
    # first find all ancestors above cutoff value
    filtered_counts = ancestor_counts.filter(ancestor_counts['sum_desc_count'] >= cutoff_value)
    
    # find the minimum level of separation required to meet the cutoff_value
    df = filtered_counts
    df = df.join(cond_counts, df['descendant_concept_id']==cond_counts['condition_concept_id'])
    df = df.groupBy(['descendant_concept_id']).agg(F.min('min_levels_of_separation').alias('min_sep'))
    df = df.withColumnRenamed('descendant_concept_id', 'descendant_concept_id2')

    # now we add in all the ancestors at that level of separation
    df = df.join(filtered_counts, (filtered_counts['descendant_concept_id'] == df['descendant_concept_id2']) &
                                    (filtered_counts['min_levels_of_separation'] == df['min_sep']))
    
    # now wrangle some column names and add the descendant counts
    df = df.join(cond_counts, df['descendant_concept_id']==cond_counts['condition_concept_id'])
    df = df[['descendant_concept_id','des_concept_name','ancestor_concept_id','anc_concept_name','min_levels_of_separation','count','sum_desc_count']]
    df = df.withColumnRenamed('count','descendant_count')
    df = df.withColumnRenamed('sum_desc_count','ancestor_count')
    df = df.join(blacklisted_features,'descendant_concept_id', how = 'left_anti')
    df = df.join(blacklisted_features.withColumnRenamed('descendant_concept_id','ancestor_concept_id'),'ancestor_concept_id',how = 'left_anti')

    return df
    
def condition_features(conditions_for_the_cohort, selected_features, window_spans, basic_cohort):
    selected_features = selected_features
    df = conditions_for_the_cohort
    df = df.join(selected_features, df['condition_concept_id'] == selected_features['descendant_concept_id'])
    df = df[['person_id','condition_start_date','ancestor_concept_id','anc_concept_name']]
    df = df.withColumnRenamed('ancestor_concept_id','condition_concept_id')
    df = df.withColumnRenamed('anc_concept_name','condition_concept_name')
    df = df.select('person_id',F.col('condition_concept_id').alias('concept_id'),'condition_start_date').distinct()
    cohort_with_windows = basic_cohort.join(
            window_spans,
            how = 'inner',
            on = basic_cohort['window']==window_spans['window_name']
        )
    conditions_alt = cohort_with_windows.join(
            df,
            how = 'inner',
            on = (df['person_id'] == cohort_with_windows['person_id']) &
            (df["condition_start_date"] >= cohort_with_windows["window_start"]) &
            (df["condition_start_date"] <= cohort_with_windows["window_end"])
        )
    conditions_alt = conditions_alt \
        .drop(cohort_with_windows['person_id']) \
        .select('person_id','window','concept_id') \
        .distinct()
    return conditions_alt

