### Step 1

import re
def build_pre_post_med_final_distinct(pre_post_med_clean):
    df = pre_post_med_clean
    df['post_only_med'] = df.apply(lambda row: 0 if row['pre_med_count'] > 0 else 1, axis=1)
    df['more_in_post'] = df.apply(lambda row: 1 if row['post_med_count'] > row['pre_med_count'] else 0, axis=1)
    df = df[["person_id", "ancestor_drug_concept_name", "post_only_med"]]
    df.columns = df.columns.str.replace('ancestor_drug_concept_name', 'ingredient')
    df = df.drop_duplicates()
    df['ingredient'] = df['ingredient'].apply(lambda x: re.sub(r"[^A-Za-z_0-9]", '_', x))
    df['ingredient'] = df['ingredient'].apply(lambda x: x.lower())

    return df

pre_post_med_final_distinct = build_pre_post_med_final_distinct(pre_post_med_count_clean)

### Step 2

def build_add_labels(pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients):

    df_unique_med_rows = pre_post_med_count_clean[['person_id', 'sex', 'patient_group', 'apprx_age', 'race',
                                                   'ethn', 'tot_long_data_days', 'op_post_visit_ratio', 'ip_post_visit_ratio']]
    df_unique_med_rows = df_unique_med_rows.drop_duplicates()

    df_unique_dx_rows = pre_post_dx_count_clean[['person_id', 'sex', 'patient_group', 'apprx_age', 'race',
                                                 'ethn', 'tot_long_data_days', 'op_post_visit_ratio',
                                                 'ip_post_visit_ratio']]
    df_unique_dx_rows = df_unique_dx_rows.drop_duplicates()

    df = pd.concat([df_unique_med_rows, df_unique_dx_rows]).drop_duplicates().reset_index(drop=True)

    # Encode dummy variables
    def cleanup(s):
        if s.lower() in ('unknown', 'gender unknown', 'no matching concept'):
            return 'unknown'
        else:
            return s

    df['sex'] = [cleanup(s) for s in df['sex']]

    # encode these caterogical variables as binaries
    df = pd.get_dummies(df, columns=["sex","race","ethn"])
    df = df.rename(columns = lambda c: str.lower(c.replace(" ", "_")))

     # Add Labels
    df['long_covid'] = 0
    df['hospitalized'] = 0

    df = df.merge(long_covid_patients, on='person_id', how='left')

    df['long_covid'] = df.apply(lambda row: 1 if not pd.isna(row['min_long_covid_date']) else 0, axis=1)
    df['hospitalized'] = df.apply(lambda row: 1 if row['patient_group'] == 'CASE_HOSP' else 0, axis=1)

    return df

add_labels = build_add_labels(pre_post_dx_count_clean, pre_post_med_count_clean, long_covid_patients)

#### Step 3

def build_pre_post_more_in_dx_calc(pre_post_dx):
    result = pre_post_dx
    result['post_only_dx'] = result.apply(lambda row: 0 if row['pre_dx_count'] > 0 else 1, axis=1)
    result['more_in_post'] = result.apply(lambda row: 1 if row['post_dx_count'] > row['pre_dx_count'] else 0, axis=1)
    return result

pre_post_dx_more_in_post_calc = build_pre_post_more_in_dx_calc(pre_post_dx_count_clean)

concept_table_fetch_query = f"""SELECT * FROM concept co"""

concept = pd.read_gbq(concept_table_fetch_query, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")

concept_ancestor_table_fetch_query = f"""SELECT * FROM concept_ancestor co"""

concept_ancestor = pd.read_gbq(concept_ancestor_table_fetch_query, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")

#### Step 4

def build_condition_rollup(long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept):
    pp = pre_post_dx_count_clean
    ca = concept_ancestor
    ct = concept
    lc = long_covid_patients

    df = pd.merge(pp, lc, on='person_id')
    df = pd.merge(df, ca, left_on='condition_concept_id', right_on='descendant_concept_id', how='inner')
    df = pd.merge(df, ct, left_on='ancestor_concept_id', right_on='concept_id', how='inner')

    df = df[~df.concept_name.isin(['General problem AND/OR complaint',
                                                'Disease',
                                                'Sequelae of disorders classified by disorder-system',
                                                'Sequela of disorder',
                                                'Sequela',
                                                'Recurrent disease',
                                                'Problem',
                                                'Acute disease',
                                                'Chronic disease',
                                                'Complication'
                                                ])]

    generic_codes = ['finding', 'disorder of', 'by site', 'right', 'left']

    for gc in generic_codes:
        df = df[~df["concept_name"].str.lower().str.contains(gc)]

        if gc not in ['right', 'left']:
            df = df[~df["condition_concept_name"].str.lower().str.contains(gc)]

    df = df[df.min_levels_of_separation.between(0,2)]

    df = df.groupby(['concept_name','condition_concept_name', 'condition_concept_id',
                    'min_levels_of_separation', 'max_levels_of_separation']).agg(ptct_training=('person_id', 'nunique')).reset_index()

    df.rename(columns = {'concept_name':'parent_concept_name', 'condition_concept_name': 'child_concept_name',
                         'condition_concept_id': 'child_concept_id',
                        'min_levels_of_separation': 'min_hops_bt_parent_child',
                         'max_levels_of_separation': 'max_hops_bt_parent_child'}, inplace = True)

    return df

condition_rollup = build_condition_rollup(long_covid_patients, pre_post_dx_count_clean, concept_ancestor, concept)

#### Step 5

def build_parent_condition_rollup(condition_rollup):
    df = condition_rollup
    df = df.groupby(['parent_concept_name']).agg(total_pts=('ptct_training', 'sum')).reset_index()
    df = df[df['total_pts'] >= 3]
    return df

parent_condition_rollup = build_parent_condition_rollup(condition_rollup)

##### Step 6

def process(parent_conditions, condition_rollup):
    pc = parent_conditions
    dm = condition_rollup

    df = pd.merge(pc, dm, on='parent_concept_name')
    df = df[['parent_concept_name', 'child_concept_name']].drop_duplicates()

    return df

final_rollup = process(parent_condition_rollup, condition_rollup)

#### Step 7

def build_add_alt_rollup(pre_post_dx_more_in_post, final_rollups):
    pre_post_dx_final = pre_post_dx_more_in_post
    df = pd.merge(pre_post_dx_final, final_rollups, left_on='condition_concept_name', right_on='child_concept_name', how='left')
    del df['child_concept_name']
    df.rename(columns = {'parent_concept_name': 'high_level_condition'}, inplace = True)
    return df

add_alt_rollup = build_add_alt_rollup(pre_post_dx_more_in_post_calc, final_rollup)

#### Step 8

def build_pre_post_dx_final(add_alt_rollup):
    df = add_alt_rollup
    df = df[df['high_level_condition'] != 'EXCLUDE']
    df = df[df['high_level_condition'].notnull()]
    df = df[~df["high_level_condition"].str.lower().str.contains("covid")]
    df = df[~df["high_level_condition"].str.lower().str.contains("coronav")]
    df = df[~df["high_level_condition"].str.lower().str.contains("post_infectious_disorder")]
    df = df[df["patient_group"].isin(['CASE_NONHOSP', 'CASE_HOSP'])]

    df['condition_concept_name'] = df['condition_concept_name'].apply(lambda x: re.sub(r"[^A-Za-z_0-9]", '_', x))
    df['condition_concept_name'] = df['condition_concept_name'].apply(lambda x: x.lower())

    df['high_level_condition'] = df['high_level_condition'].apply(lambda x: re.sub(r"[^A-Za-z_0-9]", '_', x))
    df['high_level_condition'] = df['high_level_condition'].apply(lambda x: x.lower())

    df = df.groupby(['high_level_condition', 'person_id', 'patient_group']).agg(
        pre_dx_count_sum=('pre_dx_count', 'sum'),
        post_dx_count_sum=('post_dx_count', 'sum')).reset_index()

    df = df[df['high_level_condition'].notnull()]

    # does the condition occur more often in after the covid acute phase?
    df['greater_in_post'] = df.apply(lambda row: 1 if row['post_dx_count_sum'] > row['pre_dx_count_sum'] else 0, axis=1)

    # does the condition ONLY occur after the covid acute phase?
    df['only_in_post'] = df.apply(lambda row: 1 if (row['pre_dx_count_sum'] == 0 & row['post_dx_count_sum'] > 0) else 0, axis=1)

    return df

pre_post_dx_final = build_pre_post_dx_final(add_alt_rollup)

#### Step 9

def build_count_dx_pre_and_post(pre_post_dx_final):

    df = pre_post_dx_final
    total_pre = df.groupby(['person_id']).agg(total_pre_dx=('pre_dx_count_sum', 'sum')).reset_index()
    total_post = df.groupby(['person_id']).agg(total_post_dx=('post_dx_count_sum', 'sum')).reset_index()

    distinct_people = pd.DataFrame(df.person_id.unique(), columns=['person_id'])

    result = pd.merge(distinct_people, total_pre, on='person_id', how="left")
    result = pd.merge(result, total_post, on='person_id', how="left")

    return result

count_dx_pre_and_post = build_count_dx_pre_and_post(pre_post_dx_final)

##### Step 10

cols_for_model = ['difficulty_breathing', 'apprx_age', 'fatigue', 'op_post_visit_ratio', 'dyspnea', 'ip_post_visit_ratio', 'albuterol', 'hospitalized', 'sex_male', 'fluticasone', 'palpitations', 'mental_disorder', 'uncomplicated_asthma', 'chronic_pain', 'malaise', 'chronic_fatigue_syndrome', 'formoterol', 'tachycardia', 'metabolic_disease', 'chest_pain', 'inflammation_of_specific_body_organs', 'impaired_cognition', 'diarrhea', 'acetaminophen', 'dyssomnia', 'anxiety_disorder', 'cough', 'anxiety', 'muscle_pain', 'interstitial_lung_disease', 'migraine', 'degenerative_disorder', 'viral_lower_respiratory_infection', 'promethazine', 'deficiency_of_micronutrients', 'asthma', 'disorder_characterized_by_pain', 'apixaban', 'lesion_of_lung', 'inflammation_of_specific_body_systems', 'breathing_related_sleep_disorder', 'chronic_nervous_system_disorder', 'iopamidol', 'loss_of_sense_of_smell', 'amitriptyline', 'sleep_disorder', 'pain_of_truncal_structure', 'neurosis', 'headache', 'tracheobronchial_disorder', 'communication_disorder', 'amnesia', 'hypoxemia', 'lower_respiratory_infection_caused_by_sars_cov_2', 'bleeding', 'amoxicillin', 'disorder_due_to_infection', 'chronic_sinusitis', 'pain_in_lower_limb', 'furosemide', 'buspirone', 'vascular_disorder', 'memory_impairment', 'insomnia', 'budesonide', 'prednisone', 'pneumonia_caused_by_sars_cov_2', 'clavulanate', 'dizziness', 'neuropathy', 'iron_deficiency_anemia_due_to_blood_loss', 'estradiol', 'ceftriaxone', 'shoulder_joint_pain', 'sexually_active', 'abdominal_pain', 'skin_sensation_disturbance', 'ketorolac', 'depressive_disorder', 'hyperlipidemia', 'chronic_kidney_disease_due_to_hypertension', 'spondylosis', 'vascular_headache', 'fibrosis_of_lung', 'acute_respiratory_disease', 'chronic_cough', 'osteoporosis', 'lorazepam', 'connective_tissue_disorder_by_body_site', 'adjustment_disorder', 'benzonatate', 'shoulder_pain', 'mineral_deficiency', 'obesity', 'epinephrine', 'dependence_on_enabling_machine_or_device', 'dependence_on_respiratory_device', 'inflammation_of_specific_body_structures_or_tissue', 'spironolactone', 'cholecalciferol', 'heart_disease', 'pain', 'major_depression__single_episode', 'meloxicam', 'hydrocortisone', 'collagen_disease', 'headache_disorder', 'hypoxemic_respiratory_failure', 'morphine', 'cardiac_arrhythmia', 'seborrheic_keratosis', 'gabapentin', 'dulaglutide', 'hypertensive_disorder', 'effusion_of_joint', 'moderate_persistent_asthma', 'morbid_obesity', 'seborrheic_dermatitis', 'rbc_count_low', 'blood_chemistry_abnormal', 'acute_digestive_system_disorder', 'sars_cov_2__covid_19__vaccine__mrna_spike_protein', 'influenza_b_virus_antigen', 'pulmonary_function_studies_abnormal', 'sleep_apnea', 'abnormal_presence_of_protein', 'sodium_chloride', 'atropine', 'aspirin', 'cognitive_communication_disorder', 'metronidazole', 'ethinyl_estradiol', 'gadopentetate_dimeglumine', 'traumatic_and_or_non_traumatic_injury_of_anatomical_site', 'colchicine', 'anomaly_of_eye', 'oxycodone', 'osteoarthritis', 'complication_of_pregnancy__childbirth_and_or_the_puerperium', 'allergic_rhinitis', 'dizziness_and_giddiness', 'genitourinary_tract_hemorrhage', 'duloxetine', 'bipolar_disorder', 'vitamin_disease', 'respiratory_obstruction', 'genuine_stress_incontinence', 'chronic_disease_of_respiratory_systemx', 'traumatic_and_or_non_traumatic_injury', 'drug_related_disorder', 'nortriptyline', 'involuntary_movement', 'knee_pain', 'peripheral_nerve_disease', 'gastroesophageal_reflux_disease_without_esophagitis', 'mupirocin', 'fluconazole', 'pure_hypercholesterolemia', 'kidney_disease', 'injury_of_free_lower_limb', 'glaucoma', 'backache', 'tachyarrhythmia', 'myocarditis', 'nitrofurantoin', 'prediabetes', 'sodium_acetate', 'apnea', 'losartan', 'radiology_result_abnormal', 'pantoprazole', 'hemoglobin_low', 'mixed_hyperlipidemia', 'mass_of_soft_tissue', 'levonorgestrel', 'omeprazole', 'allergic_disposition', 'metformin', 'fentanyl', 'spinal_stenosis_of_lumbar_region', 'cyst', 'soft_tissue_lesion', 'altered_bowel_function', 'skin_lesion', 'triamcinolone', 'pain_in_upper_limb', 'acute_respiratory_infections', 'neck_pain', 'guaifenesin', 'disorders_of_initiating_and_maintaining_sleep', 'loratadine', 'vitamin_b12', 'hypercholesterolemia', 'potassium_chloride', 'arthropathy', 'chronic_kidney_disease_due_to_type_2_diabetes_mellitus', 'disease_of_non_coronary_systemic_artery', 'soft_tissue_injury', 'cytopenia']

def pivot_dx(dx_df, cols_for_model):

    dx_df = dx_df[dx_df['high_level_condition'].isin(cols_for_model)]
    dx_df = dx_df.pivot_table(columns = 'high_level_condition', index = 'person_id', values = 'greater_in_post', aggfunc = max).reset_index()
    dx_df = dx_df.fillna(0)
    return dx_df

def pivot_meds(med_df, cols_for_model):
    med_df = med_df[med_df['ingredient'].isin(cols_for_model)]
    med_df = med_df.pivot_table(columns = 'ingredient', index = 'person_id', values = 'post_only_med', aggfunc = max).reset_index()
    med_df = med_df.fillna(0)
    return med_df

def build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post):

    count_dx = count_dx_pre_and_post
    df = add_labels.merge(med_df, on="person_id",  how="left")
    df = df.merge(dx_df, on='person_id', how='left')
    df = df.merge(count_dx, on='person_id', how='left').reset_index()

    for i in [add_labels.shape[1]+1, df.shape[1]]:
        df.iloc[i].fillna(0)

    result = df

    drop_cols = []
    cols = result.columns
    for c in cols:
        if re.findall('^race_', c) or re.match('^ethn', c):
            drop_cols.append(c)
        if re.findall('^sex_', c) and c != 'sex_male':
            drop_cols.append(c)

    drop_cols.extend([ "patient_group"])

    result.drop(drop_cols, axis=1, inplace=True)
    return result

final_feature_table = build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post)

# P3, Step 10

def compute(training_data, add_labels, pre_post_med_final, pre_post_dx_final, count_dx_pre_and_post):
    med_df = pivot_meds(pre_post_med_final, training_data)

    dx_df = pivot_dx(pre_post_dx_final, training_data)

    result = build_final_feature_table(med_df, dx_df, add_labels, count_dx_pre_and_post)

    return result

dx_df = pivot_dx(pre_post_dx_final, cols_for_model)

med_df = pivot_meds(pre_post_med_final_distinct, cols_for_model)

count_dx_pre_and_post = compute(cols_for_model, add_labels, pre_post_med_final_distinct, pre_post_dx_final, count_dx_pre_and_post)

## Min long covid date can be datetime column in the dataframe. In case it is datetime follow these steps to convert it to string since the model expects this column to be object.

count_dx_pre_and_post['min_long_covid_date'] = pd.to_datetime(count_dx_pre_and_post['min_long_covid_date'])
count_dx_pre_and_post['min_long_covid_date'] = count_dx_pre_and_post['min_long_covid_date'].dt.strftime('%Y-%m-%d')
count_dx_pre_and_post['min_long_covid_date'] = count_dx_pre_and_post['min_long_covid_date'].fillna('0000-00-00')
count_dx_pre_and_post['min_long_covid_date'] = count_dx_pre_and_post['min_long_covid_date'].astype('string')

## We had to follow these steps in AOU to remove the columns (features) not in N3C model and add the ones that are missing.

n3c_features = ["person_id",
        "long_covid",
        "difficulty_breathing",
        "apprx_age",
        "fatigue",
        "op_post_visit_ratio",
        "dyspnea",
        "ip_post_visit_ratio",
        "albuterol",
        "hospitalized",
        "sex_male",
        "fluticasone",
        "palpitations",
        "mental_disorder",
        "uncomplicated_asthma",
        "chronic_pain",
        "malaise",
        "chronic_fatigue_syndrome",
        "formoterol",
        "tachycardia",
        "metabolic_disease",
        "chest_pain",
        "inflammation_of_specific_body_organs",
        "impaired_cognition",
        "diarrhea",
        "acetaminophen",
        "dyssomnia",
        "anxiety_disorder",
        "cough",
        "anxiety",
        "muscle_pain",
        "interstitial_lung_disease",
        "migraine",
        "degenerative_disorder",
        "viral_lower_respiratory_infection",
        "promethazine",
        "deficiency_of_micronutrients",
        "asthma",
        "disorder_characterized_by_pain",
        "apixaban",
        "lesion_of_lung",
        "inflammation_of_specific_body_systems",
        "breathing_related_sleep_disorder",
        "chronic_nervous_system_disorder",
        "iopamidol","loss_of_sense_of_smell",
        "amitriptyline",
        "sleep_disorder",
        "pain_of_truncal_structure",
        "neurosis",
        "headache",
        "tracheobronchial_disorder",
        "communication_disorder",
        "amnesia",
        "hypoxemia",
        "lower_respiratory_infection_caused_by_sars_cov_2",
        "bleeding",
        "amoxicillin",
        "disorder_due_to_infection",
        "chronic_sinusitis",
        "pain_in_lower_limb",
        "furosemide",
        "buspirone",
        "vascular_disorder",
        "memory_impairment",
        "insomnia",
        "budesonide",
        "prednisone",
        "pneumonia_caused_by_sars_cov_2",
        "clavulanate",
        "dizziness",
        "neuropathy",
        "iron_deficiency_anemia_due_to_blood_loss",
        "estradiol",
        "ceftriaxone",
        "shoulder_joint_pain",
        "sexually_active",
        "abdominal_pain",
        "skin_sensation_disturbance",
        "ketorolac",
        "depressive_disorder",
        "hyperlipidemia",
        "chronic_kidney_disease_due_to_hypertension",
        "spondylosis",
        "vascular_headache",
        "fibrosis_of_lung",
        "acute_respiratory_disease",
        "chronic_cough",
        "osteoporosis",
        "lorazepam",
        "connective_tissue_disorder_by_body_site",
        "adjustment_disorder",
        "benzonatate",
        "shoulder_pain",
        "mineral_deficiency",
        "obesity",
        "epinephrine",
        "dependence_on_enabling_machine_or_device",
        "dependence_on_respiratory_device",
        "inflammation_of_specific_body_structures_or_tissue",
        "spironolactone",
        "cholecalciferol",
        "heart_disease",
        "pain",
        "major_depression__single_episode",
        "meloxicam",
        "hydrocortisone",
        "collagen_disease",
        "headache_disorder",
        "hypoxemic_respiratory_failure",
        "morphine",
        "cardiac_arrhythmia",
        "seborrheic_keratosis",
        "gabapentin",
        "dulaglutide",
        "hypertensive_disorder",
        "effusion_of_joint",
        "moderate_persistent_asthma",
        "morbid_obesity",
        "seborrheic_dermatitis",
        "rbc_count_low",
        "blood_chemistry_abnormal",
        "acute_digestive_system_disorder",
        "sars_cov_2__covid_19__vaccine__mrna_spike_protein",
        "influenza_b_virus_antigen",
        "pulmonary_function_studies_abnormal",
        "sleep_apnea",
        "abnormal_presence_of_protein",
        "sodium_chloride",
        "atropine",
        "aspirin",
        "cognitive_communication_disorder",
        "metronidazole",
        "ethinyl_estradiol",
        "gadopentetate_dimeglumine",
        "traumatic_and_or_non_traumatic_injury_of_anatomical_site",
        "colchicine",
        "anomaly_of_eye",
        "oxycodone",
        "osteoarthritis",
        "complication_of_pregnancy__childbirth_and_or_the_puerperium",
        "allergic_rhinitis",
        "dizziness_and_giddiness",
        "genitourinary_tract_hemorrhage",
        "duloxetine",
        "bipolar_disorder",
        "vitamin_disease",
        "respiratory_obstruction",
        "genuine_stress_incontinence",
        "chronic_disease_of_respiratory_system",
        "traumatic_and_or_non_traumatic_injury",
        "drug_related_disorder",
        "nortriptyline",
        "involuntary_movement",
        "knee_pain",
        "peripheral_nerve_disease",
        "gastroesophageal_reflux_disease_without_esophagitis",
        "mupirocin",
        "fluconazole",
        "pure_hypercholesterolemia",
        "kidney_disease",
        "injury_of_free_lower_limb",
        "glaucoma",
        "backache",
        "tachyarrhythmia",
        "myocarditis",
        "nitrofurantoin",
        "prediabetes",
        "sodium_acetate",
        "apnea",
        "losartan",
        "radiology_result_abnormal",
        "pantoprazole",
        "hemoglobin_low",
        "mixed_hyperlipidemia",
        "mass_of_soft_tissue",
        "levonorgestrel",
        "omeprazole",
        "allergic_disposition",
        "metformin",
        "fentanyl",
        "spinal_stenosis_of_lumbar_region",
        "cyst",
        "soft_tissue_lesion",
        "altered_bowel_function",
        "skin_lesion",
        "triamcinolone",
        "pain_in_upper_limb",
        "acute_respiratory_infections",
        "neck_pain",
        "guaifenesin",
        "disorders_of_initiating_and_maintaining_sleep",
        "loratadine",
        "vitamin_b12",
        "hypercholesterolemia",
        "potassium_chloride",
        "arthropathy",
        "chronic_kidney_disease_due_to_type_2_diabetes_mellitus",
        "disease_of_non_coronary_systemic_artery",
        "soft_tissue_injury",
        "cytopenia"
]

# This step makes sure we add any of the feature columns missing in AOU data

for feature in n3c_features:
    if feature in count_dx_pre_and_post.columns:
        count_dx_pre_and_post[feature] = count_dx_pre_and_post[feature].fillna(0)
    else:
        count_dx_pre_and_post[feature] = 0

test_data = count_dx_pre_and_post.copy()

for c in test_data.columns:
    if c not in n3c_features:
        test_data.drop([c], axis=1, inplace=True)


