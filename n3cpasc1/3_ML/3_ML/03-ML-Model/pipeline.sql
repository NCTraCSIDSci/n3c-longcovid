

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3ac52a88-c634-4472-9f45-a0e65f783978"),
    Cov_pos_507=Input(rid="ri.foundry.main.dataset.4687ec8e-e217-4060-9edd-6f739b8acca2"),
    pivot_just_cases=Input(rid="ri.foundry.main.dataset.5507fb0e-9ecc-4bfb-8bc6-9e7ce90cf20e")
)

SELECT  /* REPARTITION(1) */ 
        c.*,
        CASE
            WHEN p.person_id IS NOT NULL THEN 1
            ELSE 0
        END AS long_covid,
        CASE
            WHEN c.patient_group = 'CASE_HOSP' THEN 1
            ELSE 0
        END AS hospitalized
        

FROM pivot_just_cases c
LEFT JOIN Cov_pos_507 p
ON c.person_id = p.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ccf935e2-8e36-48ea-ba14-06315a30bbbb")
)
SELECT DISTINCT person_id, case_sex, case_apprx_age, case_race, case_ethn, case_diab_ind, case_kid_ind, case_CHF_ind, case_chronicPulm_ind
FROM pre_post_dx_finalSELECT DISTINCT person_id, case_sex, case_apprx_age, case_race, case_ethn, case_diab_ind, case_kid_ind, case_CHF_ind, case_chronicPulm_ind
FROM pre_post_dx_final

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4b035e8a-a890-43d8-aa7c-e917363ed7be"),
    pre_post_med_final=Input(rid="ri.foundry.main.dataset.d02b67e0-3876-49e2-9e14-ce1f437e1dfc")
)
SELECT DISTINCT person_id, ancestor_drug_concept_name AS ingredient, post_only_med
FROM pre_post_med_final

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7b2b5050-aa66-42f4-b71f-7bd0eb2c5178"),
    add_demographic_dummies=Input(rid="ri.foundry.main.dataset.18e0073c-afb6-488b-9cec-e84d4d11d4c0")
)
SELECT *
FROM add_demographic_dummies
WHERE long_covid = 0 AND hospitalized = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.733f3868-da85-437c-acd8-16a98f97d9cb"),
    add_demographic_dummies=Input(rid="ri.foundry.main.dataset.18e0073c-afb6-488b-9cec-e84d4d11d4c0")
)
SELECT *
FROM add_demographic_dummies
WHERE long_covid = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2d1bb3d3-5089-45ba-8056-25f4406d8731"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.1f93e07f-f1e0-40b6-8566-e3da6fbd3be4")
)
SELECT ancestor_drug_concept_name, ancestor_drug_concept_id, count(distinct person_id) as pt_count
FROM pre_post_med_count_clean
GROUP BY ancestor_drug_concept_name, ancestor_drug_concept_id
HAVING count(distinct person_id) >= (select count(distinct person_id) as tot_ct from pre_post_med_count_clean)*0.01

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.18df8bcd-94e4-48fb-aaf7-82b9bbc54af6"),
    add_demographic_dummies=Input(rid="ri.foundry.main.dataset.18e0073c-afb6-488b-9cec-e84d4d11d4c0")
)
SELECT *
FROM add_demographic_dummies
WHERE long_covid = 0 AND hospitalized = 0

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d02b67e0-3876-49e2-9e14-ce1f437e1dfc"),
    meds_above_threshold=Input(rid="ri.foundry.main.dataset.2d1bb3d3-5089-45ba-8056-25f4406d8731"),
    pre_post_med_count_clean=Input(rid="ri.foundry.main.dataset.1f93e07f-f1e0-40b6-8566-e3da6fbd3be4")
)
SELECT pp.*
FROM pre_post_med_count_clean pp
INNER JOIN meds_above_threshold m
ON pp.ancestor_drug_concept_id = m.ancestor_drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3b982b44-45a0-4a0f-9818-581257467976"),
    union_data=Input(rid="ri.foundry.main.dataset.98b5ce44-c2ef-412d-8ffe-8fe8cd55d81f")
)
SELECT long_covid, hospitalized, COUNT(*) AS num_patients
FROM union_data
GROUP BY long_covid, hospitalized

@transform_pandas(
    Output(rid="ri.vector.main.execute.0c372e11-bbd6-4327-ac4d-d1fda8ccc5c9"),
    add_label=Input(rid="ri.foundry.main.dataset.3ac52a88-c634-4472-9f45-a0e65f783978")
)
SELECT patient_group, long_covid, COUNT(*) AS row_count
FROM add_label
GROUP BY patient_group, long_covid

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8e3588f7-bc6b-4dcb-ac82-9f9ab3982fdd"),
    drop_features=Input(rid="ri.foundry.main.dataset.013af07a-4b21-4df6-9272-01b39ab728b8")
)
SELECT *
FROM drop_features
WHERE random > 0.77

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fb1fd621-c97c-4e66-a2f5-c3f0793dfabc"),
    drop_features=Input(rid="ri.foundry.main.dataset.013af07a-4b21-4df6-9272-01b39ab728b8")
)
SELECT *
FROM drop_features
WHERE random <= 0.77

