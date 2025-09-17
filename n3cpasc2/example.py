# Example of a script running the source.
import n3cpasc2
# This sets the windows. Parameters can be adjusted as necessary.
window_spans = generate_window_spans(start = (2020,1),
                                     end = (2024,6),
                                     window_length = 100)
concept_table = pd.read_csv('concept_small.csv')
# For reasons of size, the following columns were disincluded from concept_small:
concept_table['vocabulary_id'] = 'SNOMED'
concept_table['standard_concept'] = 'S'
concept_ancestor_table = pd.read_csv('concept_ancestor_small.csv')
condition_df = pd.read_csv('synthetic_concept_seeds_small.csv')
observation_df = pd.read_csv('synthetic_concept_seeds_small.csv')

# Below code generates synthetic data. 
# In actual use, replace with proper data loading.
person_table = generate_person_table()
visit_table = generate_visit_table(person_df = person_table)
condition_table = generate_condition_table(visit_df = visit_table, concept_df = condition_df)
observation_table = generate_observation_table(visit_df = visit_table, concept_df = observation_df)
fact_table = generate_fact_table(visit_df = visit_table)


# First stage: Create the cohort and assemble the COVID-relevant data
fact_spark = spark.createDataFrame(fact_table)
person_spark = spark.createDataFrame(person_table)
visit_spark = spark.createDataFrame(visit_table)
condition_spark = spark.createDataFrame(condition_table)
observation_spark = spark.createDataFrame(observation_table)
window_spark = spark.createDataFrame(window_spans)

covid_cohort_df = covid_cohort(fact_spark)
infection_dates_df = infection_dates(covid_cohort_df,
                                        fact_spark)
cohort_and_idx_df = cohort_and_idx(covid_cohort_df)
blackout_dates_df = blackout_dates(cohort_and_idx_df,
                                   infection_dates_df)
basic_cohort_df = basic_cohort(person_spark,
                               cohort_and_idx_df,
                               window_spark)
labels_df = labels(condition_spark,
                   basic_cohort_df,
                   observation_spark)
post_visit_counts_in_windows_df = post_visit_counts_in_windows(basic_cohort_df,
                                                               blackout_dates_df,
                                                               visit_spark,
                                                               window_spark)

# Second stage: Take the pool of observations and conditions. Combine and condense.
concept_spark = spark.createDataFrame(concept_table)
concept_ancestor_spark = spark.createDataFrame(concept_ancestor_table)

conditions_for_the_cohort_df = conditions_for_the_cohort(basic_cohort_df,
                                                         blackout_dates_df,
                                                         condition_spark,
                                                         observation_spark)
# Appropriate cutoff value for selected features really varies.                                                         
selected_features_df = selected_features(conditions_for_the_cohort_df,
                                         concept_ancestor_spark,
                                         concept_spark,
                                         cutoff_value = 500)
condition_features_df = condition_features(conditions_for_the_cohort_df,
                                           selected_features_df,
                                           window_spark,
                                           basic_cohort_df)


# Third stage: Training.
training_set_df = training_set(basic_cohort_df,
                               labels_df,
                               window_spark)
feature_downselect_df = feature_downselect(training_set_df,
                                           selected_features_df,
                                           condition_features_df,
                                           threshold = 0.00001)
train_data_full_df = train_data_full(training_set_df,
                                     post_visit_counts_in_windows_df,
                                     feature_downselect_df,
                                     condition_features_df)
model_train_data_df = train_data_full_df.filter(~training_set_df['hold_for_val'])
model_val_set_df = train_data_full_df.filter(train_data_full_df['hold_for_val'])

# Now actually train the model:
pasc_trained_model = pasc_train_model(model_train_data_df)

# Fourth stage: Application. 
processed_data_full_df = process_data_full(basic_cohort_df,
                                             post_visit_counts_in_windows_df,
                                             feature_downselect_df,
                                             condition_features_df)
# In practice, it was necessary to divide the full data into chunks with subsets of the patient body.
model_scores_full_df = model_scores(processed_data_full_df,
                                    pasc_trained_model)
# Accuracy metrics and performance are calculated based on the validation set.
# Note, however, that the negatives of the validation set are not necessarily known true negatives.
# See https://doi.org/10.1016/j.landig.2025.100887 for details.
model_scores_val_df = model_scores(model_val_set_df,
                                   pasc_trained_model)
# These model score tables are the end product of the pipeline. 
# Each patient has a model score for each window, including windows where they lack data.
# In general, for analysis, we tend to identify patients as having PASC based on their maximum score.
# A single "date of long COVID" is not always clear in this approach.
# In general (see again the paper above), the date associated with the max score seemed best to us.
# YMMV.
