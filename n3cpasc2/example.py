# Example of a script running the source.
import n3cpasc2
# This sets the windows. Parameters can be adjusted as necessary.
window_spans = generate_window_spans(start = (2020,1),
                                     end = (2024,6),
                                     window_length = 100)
concept_table = pd.read_csv('concept.csv')
concept_ancestor_table = pd.read_csv('concept_ancestor.csv')
condition_df = pd.read_csv('condition_seeds.csv')
observation_df = pd.read_csv('observation_seeds.csv')

# Below code generates synthetic data. 
# In actual use, replace with proper data loading.
person_table = generate_person_table()
visit_table = generate_visit_table(person_df = person_table)
condition_table = generate_condition_table(visit_df = visit_table)
observation_table = generate_observation_table(visit_df = visit_table)
fact_table = generate_fact_table(visit_df = visit_table)

# First stage: Create the cohort and assemble the COVID-relevant data.
covid_cohort_df = covid_cohort(fact_table)
infection_dates_df = infection_dates(covid_cohort_df,
                                        fact_table)
cohort_and_idx_df = cohort_and_idx(covid_cohort_df)
blackout_dates_df = blackout_dates(cohort_and_idx_df,
                                   infection_dates_df)
basic_cohort_df = basic_cohort(person_table,
                               cohort_and_idx_df,
                               window_spans)
labels_df = labels(condition_table,
                   basic_cohort_df,
                   observation_table)
post_visit_counts_in_windows_df = post_visit_counts_in_windows(basic_cohort_df,
                                                               blackout_dates_df,
                                                               visit_table,
                                                               window_spans)

# Second stage: Take the pool of observations and conditions. Combine and condense.
conditions_for_the_cohort_df = conditions_for_the_cohort(basic_cohort_df,
                                                         blackout_dates_df,
                                                         condition_table,
                                                         observation_table)
selected_features_df = selected_features(conditions_for_the_cohort_df,
                                         concept_ancestor_table,
                                         concept_table,
                                         cutoff_value = 25_000)
condition_features_df = condition_features(basic_cohort_df,
                                           conditions_for_the_cohort_df,
                                           selected_features_df,
                                           window_spans)

# Third stage: Create the training set for the model from the cohort, labels, and features. 
training_set_df = training_set(basic_cohort_df,
                               labels_df,
                               window_spans)
feature_downselect_df = feature_downselect(training_set_df,
                                           selected_features_df,
                                           condition_features_df)
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

