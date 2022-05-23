

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0a8e744f-741b-491d-9f1e-fa900c2b96ca"),
    COVID_Lab_Index=Input(rid="ri.foundry.main.dataset.89707410-4bd5-4ca8-937a-b9563c5120e0"),
    Dx_Index=Input(rid="ri.foundry.main.dataset.03324468-383c-4a77-b7e5-b8ab25d47004"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875")
)
SELECT person_id, min(idx_dt) as min_covid_dt
FROM
(
SELECT did.person_id, DX_INDEX_DT as idx_dt
FROM Dx_Index did LEFT JOIN death de ON did.person_id = de.person_id
WHERE de.person_id is null

UNION

SELECT did.person_id, PCR_INDEX_DT as idx_dt
FROM COVID_Lab_Index did LEFT JOIN death de ON did.person_id = de.person_id
WHERE de.person_id is null
)
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1ddeab0d-5e35-480a-b4e9-bd12ee04be9f"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members c
where c.concept_set_name in ('[DATOS - Charlson] Congestive Heart Failure') and is_most_recent_version = 'true'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6b3aff9b-f312-4d09-98ae-32a057ee10e7"),
    CHF_Concepts=Input(rid="ri.foundry.main.dataset.1ddeab0d-5e35-480a-b4e9-bd12ee04be9f"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT co.person_id, count(distinct co.condition_start_date) as cpc_dx_count, min(co.condition_start_date) as earliest_dx
FROM condition_occurrence co JOIN CHF_Concepts dc ON co.condition_concept_id = dc.concept_id
GROUP BY co.person_id
HAVING count(distinct co.condition_start_date) >=2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.89707410-4bd5-4ca8-937a-b9563c5120e0"),
    COVID_Tests=Input(rid="ri.foundry.main.dataset.d03da9d5-e9b3-4f95-8313-ce691ab0d190"),
    Positive_Results=Input(rid="ri.foundry.main.dataset.c8056f3f-e7a7-4aee-9297-2585f9c01b3f"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
--patients with positive PCR tests and an index date (earliest positive result)
SELECT m.person_id, min(m.measurement_date) as PCR_INDEX_DT
FROM measurement m JOIN COVID_Tests p on m.measurement_concept_id = p.concept_id
    JOIN Positive_Results pr on m.value_as_concept_id = pr.concept_id
WHERE data_partner_id IN (507,213,569)
GROUP BY m.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d03da9d5-e9b3-4f95-8313-ce691ab0d190"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT concept_id 
FROM concept_set_members
WHERE concept_set_name in ('ATLAS SARS-CoV-2 rt-PCR and AG', 'CovidAmbiguous') AND is_most_recent_version = True

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.745aefe9-8486-446b-8314-fca62d275708"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members c
where c.concept_set_name in ('[DATOS - Charlson] chronic pulmonary disease') and is_most_recent_version = 'true'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bcdbca64-b2e4-444e-b284-8d164f9d46e2"),
    Chronic_Pulm_Concepts=Input(rid="ri.foundry.main.dataset.745aefe9-8486-446b-8314-fca62d275708"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT co.person_id, count(distinct co.condition_start_date) as cpc_dx_count, min(co.condition_start_date) as earliest_dx
FROM condition_occurrence co JOIN Chronic_Pulm_Concepts dc ON co.condition_concept_id = dc.concept_id
GROUP BY co.person_id
HAVING count(distinct co.condition_start_date) >=2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9f552100-0ba0-4b4d-81a2-c355a687dcdc"),
    CHF_Patients=Input(rid="ri.foundry.main.dataset.6b3aff9b-f312-4d09-98ae-32a057ee10e7"),
    Chronic_Pulm_Patients=Input(rid="ri.foundry.main.dataset.bcdbca64-b2e4-444e-b284-8d164f9d46e2"),
    Diabetes_Patients=Input(rid="ri.foundry.main.dataset.8434a22a-5190-4c75-8c97-1be96520e2c3"),
    Kidney_Patients=Input(rid="ri.foundry.main.dataset.8cebd881-10f5-4c4b-bc35-3766c41deca8"),
    Ninety_Days_Out=Input(rid="ri.foundry.main.dataset.30c478a8-fa04-4dfa-bbc9-a9c4b72af30f")
)
SELECT distinct 
    vc.person_id, 
    (2021 - vc.year_of_birth) as apprx_age, 
    vc.gender_concept_name as sex, 
    vc.race_concept_name as race, 
    vc.ethnicity_concept_name as ethn, 
    vc.data_partner_id as site_id,
    case when dp.person_id is not null then 'Yes' else 'No' end as diabetes_ind,
    case when kp.person_id is not null then 'Yes' else 'No' end as kidney_ind,
    case when chf.person_id is not null then 'Yes' else 'No' end as CHF_ind,
    case when chr.person_id is not null then 'Yes' else 'No' end as ChronicPulm_ind,
    min_covid_dt
FROM Ninety_Days_Out vc 
    LEFT JOIN Diabetes_Patients dp ON vc.person_id = dp.person_id and vc.min_covid_dt > dp.earliest_dx
    LEFT JOIN Kidney_Patients kp ON vc.person_id = kp.person_id and vc.min_covid_dt > kp.earliest_dx
    LEFT JOIN CHF_Patients chf ON vc.person_id = chf.person_id and vc.min_covid_dt > chf.earliest_dx
    LEFT JOIN Chronic_Pulm_Patients chr ON vc.person_id = chr.person_id and vc.min_covid_dt > chr.earliest_dx
WHERE 
    (2021 - vc.year_of_birth) is not null 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d525f36b-2b63-44e5-b3b4-26865990af76"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members c
where c.concept_set_name in ('Type 1 diabetes mellitus','Type 2 diabetes mellitus') and is_most_recent_version = 'true'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8434a22a-5190-4c75-8c97-1be96520e2c3"),
    Diabetes_Concepts=Input(rid="ri.foundry.main.dataset.d525f36b-2b63-44e5-b3b4-26865990af76"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT co.person_id, count(distinct co.condition_start_date) as dm_dx_count, min(co.condition_start_date) as earliest_dx
FROM condition_occurrence co JOIN Diabetes_Concepts dc ON co.condition_concept_id = dc.concept_id
GROUP BY co.person_id
HAVING count(distinct co.condition_start_date) >=2

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.234ad98b-9878-453a-bd23-b44b9690bfdb"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
)
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient' and standard_concept = 'S'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.681cb4f9-b553-46d4-9db7-a0a1b41ca957"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
SELECT d.*
FROM drug_exposure d JOIN Feature_Table_Builder f ON d.person_id = f.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03324468-383c-4a77-b7e5-b8ab25d47004"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
--patients with U07.1 and an index date (first dx date)
SELECT co.person_id, min(co.condition_start_date) as DX_INDEX_DT
FROM condition_occurrence co JOIN microvisit_to_macrovisit_lds m ON co.person_id = m.person_id
WHERE co.condition_concept_id = 37311061 and co.data_partner_id IN (507,213,569) and m.visit_concept_id IN (9203,9201,4207294,262,581379,581385,8717)
GROUP BY co.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9493c3e2-1249-48a1-a409-73c13b452d43"),
    dxs_for_melissa=Input(rid="ri.foundry.main.dataset.0e6f4d69-8297-4b4d-b27b-4656bd42875d"),
    parent_concept_counts=Input(rid="ri.foundry.main.dataset.5389a75e-d6e4-4230-9ff2-a15e84cdeddf")
)
SELECT distinct dm.parent_concept_name, dm.child_concept_name
FROM parent_concept_counts pc JOIN dxs_for_melissa dm ON pc.parent_concept_name = dm.parent_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    Collect_the_Cohort=Input(rid="ri.foundry.main.dataset.9f552100-0ba0-4b4d-81a2-c355a687dcdc"),
    Hospitalized_Cases=Input(rid="ri.foundry.main.dataset.cf9967bd-2e5f-4594-af7f-d3fff97979ca"),
    NonHosp_Cases=Input(rid="ri.foundry.main.dataset.d0d5b95a-9568-44dd-bea2-a14015a17a4b"),
    long_covid_clinic_dates=Input(rid="ri.foundry.main.dataset.01fc5667-fdc0-4577-902a-a44159435fb8"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
SELECT ottbl.*, nvl(count(distinct visit_start_date),0) as post_visits_count, datediff(ottbl.post_window_end_dt,ottbl.post_window_start_dt) as tot_post_days, nvl(count(distinct visit_start_date),0)/datediff(ottbl.post_window_end_dt,ottbl.post_window_start_dt) as post_visits_per_pt_day
FROM 
(
SELECT distinct 
hc.*, 
datediff(max(mm.visit_start_date),min(mm.visit_start_date)) as tot_long_data_days, 
date_add(hc.min_covid_dt,-365) as pre_window_start_dt, 
date_add(hc.min_covid_dt, -45) as pre_window_end_dt, 
date_add(hc.min_covid_dt, 45) as post_window_start_dt, 
case when lc.first_LC_clin_dt < date_add(hc.min_covid_dt, 45) then date_add(hc.min_covid_dt, 45)
    else nvl(date_add(lc.first_LC_clin_dt,-1), date_add(hc.min_covid_dt, 365)) end as post_window_end_dt, 
'CASE_HOSP' as patient_group,
case when lc.first_LC_clin_dt is not null then 'TRAINING' else 'TEST' end as patient_type

FROM Hospitalized_Cases hc JOIN manifest msh ON hc.site_id = msh.data_partner_id
    JOIN microvisit_to_macrovisit_lds mm ON hc.person_id = mm.person_id
    LEFT JOIN long_covid_clinic_dates lc ON hc.person_id = lc.person_id
GROUP BY
hc.person_id, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.diabetes_ind, hc.kidney_ind, hc.CHF_ind, hc.ChronicPulm_ind, hc.site_id, hc.min_covid_dt, date_add(hc.min_covid_dt,-365), date_add(hc.min_covid_dt, -45), date_add(hc.min_covid_dt, 45), date_add(hc.min_covid_dt, 365), lc.first_LC_clin_dt 

UNION

SELECT distinct 
hc.*, 
datediff(max(mm.visit_start_date),min(mm.visit_start_date)) as tot_long_data_days, 
date_add(hc.min_covid_dt,-365) as pre_window_start_dt, 
date_add(hc.min_covid_dt, -45) as pre_window_end_dt, 
date_add(hc.min_covid_dt, 45) as post_window_start_dt, 
case when lc.first_LC_clin_dt < date_add(hc.min_covid_dt, 45) then date_add(hc.min_covid_dt, 45)
    else nvl(date_add(lc.first_LC_clin_dt,-1), date_add(hc.min_covid_dt, 365)) end as post_window_end_dt, 
'CASE_NONHOSP' as patient_group,
case when lc.first_LC_clin_dt is not null then 'TRAINING' else 'TEST' end as patient_type

FROM NonHosp_Cases hc JOIN manifest msh ON hc.site_id = msh.data_partner_id
        JOIN microvisit_to_macrovisit_lds mm ON hc.person_id = mm.person_id
        LEFT JOIN long_covid_clinic_dates lc ON hc.person_id = lc.person_id
GROUP BY
hc.person_id, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.diabetes_ind, hc.kidney_ind, hc.CHF_ind, hc.ChronicPulm_ind, hc.site_id, hc.min_covid_dt, date_add(hc.min_covid_dt,-365), date_add(hc.min_covid_dt, -45), date_add(hc.min_covid_dt, 45), date_add(hc.min_covid_dt, 365), lc.first_LC_clin_dt
) ottbl LEFT JOIN microvisit_to_macrovisit_lds mmpost 
    ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and 
        ottbl.post_window_end_dt and mmpost.macrovisit_id is null)

WHERE tot_long_data_days > 1
GROUP BY ottbl.person_id, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.diabetes_ind, ottbl.kidney_ind, ottbl.CHF_ind, ottbl.ChronicPulm_ind, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt, ottbl.post_window_start_dt, ottbl.post_window_end_dt, ottbl.patient_group, ottbl.patient_type

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cf9967bd-2e5f-4594-af7f-d3fff97979ca"),
    Collect_the_Cohort=Input(rid="ri.foundry.main.dataset.9f552100-0ba0-4b4d-81a2-c355a687dcdc"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
--hospitalization close to index date
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, b.diabetes_ind, b.kidney_ind, b.CHF_ind, b.ChronicPulm_ind, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisit_to_macrovisit_lds mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between date_add(b.min_covid_dt, -14) and date_add(b.min_covid_dt, 14)

UNION

--hospitalization with a U07.1
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, b.diabetes_ind, b.kidney_ind, b.CHF_ind, b.ChronicPulm_ind, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisit_to_macrovisit_lds mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8cebd881-10f5-4c4b-bc35-3766c41deca8"),
    Renal_Concepts=Input(rid="ri.foundry.main.dataset.e40d009b-ef46-421d-b092-ad6b522d767b"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT co.person_id, count(distinct co.condition_start_date) as dm_dx_count, min(co.condition_start_date) as earliest_dx
FROM condition_occurrence co JOIN Renal_Concepts dc ON co.condition_concept_id = dc.concept_id
GROUP BY co.person_id
HAVING count(distinct co.condition_start_date) >=2 --not sure if this is good to have for kidney or not

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.909454e8-7d22-44a5-8f27-cf4b322fc649"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members c
where c.concept_set_name IN ('[DATOS] [4CE Severity] PAO2','[DATOS] Ferritin','90986-1 (Serum Potassium)','Alanine aminotransferase (ALT component)','Albumin (component)','Arterial oxygen saturation v2 Atlas# 879','Aspartate aminotransferase (AST component)','Bilirubin - direct&conjugated','Bilirubin - total','Bilirubin-indirect','Blood urea nitrogen (component)','Blood urea nitrogen / creatinine (component)','Chloride (component)','CRP C reactive protein','D-Dimer Atlas#886','Erythrocyte Sedimentation Rate v2 ATLAS #925','Ferritin (component)','fibrinogen','Glucose (component)','hemoglobin Atlas# 873','Lymphocytes (component)','Lymphocytes/100 leukocytes (component)','Mean arterial pressure v2 atlas#875','N3C BNP','N3C IL-6 v2 ATLAS #942','N3C Lactate dehydrogenase','N3C Lactate v2 ATLAS #928','N3C NT pro BNP ATLAS #937','N3C pH of Blood ATLAS #927','N3C Procalcitonin v2 ATLAS #926','N3C Troponin All Types ATLAS #862','N3C white blood cell count v2 ATLAS #934','nc3 a1c on atlas 829','neutrophils absolute Atlas# 895','Neutrophils% atlas# 894','Platelets (component v2)','Serum Creatinine Atlas 670 20200803','Sodium (Component)') and is_most_recent_version = 'true'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a6f60ab6-946c-4d49-a715-3726d90364ab"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    Lab_Concepts=Input(rid="ri.foundry.main.dataset.909454e8-7d22-44a5-8f27-cf4b322fc649"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
--limiting only to Richard's and Andrew's "important lab set" for roll-up purposes
SELECT d.*, lc.concept_set_name
FROM measurement d JOIN Feature_Table_Builder f ON d.person_id = f.person_id
    JOIN Lab_Concepts lc ON lc.concept_id = d.measurement_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.30c478a8-fa04-4dfa-bbc9-a9c4b72af30f"),
    Sufficient_Hx_v1=Input(rid="ri.foundry.main.dataset.eb26827f-e0cc-46c5-b2fa-050499d3d907"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT sh.person_id, sh.min_covid_dt, pr.gender_concept_name, pr.year_of_birth, pr.race_concept_name, pr.ethnicity_concept_name, pr.data_partner_id, datediff(msh.run_date,sh.min_covid_dt) as days_since_covid
FROM Sufficient_Hx_v1 sh
    JOIN person pr ON sh.person_id = pr.person_id
    JOIN manifest msh ON pr.data_partner_id = msh.data_partner_id
WHERE datediff(msh.run_date,sh.min_covid_dt) >= 90 and pr.year_of_birth <= 2003

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d0d5b95a-9568-44dd-bea2-a14015a17a4b"),
    Collect_the_Cohort=Input(rid="ri.foundry.main.dataset.9f552100-0ba0-4b4d-81a2-c355a687dcdc"),
    Hospitalized_Cases=Input(rid="ri.foundry.main.dataset.cf9967bd-2e5f-4594-af7f-d3fff97979ca")
)
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, b.diabetes_ind, b.kidney_ind, b.CHF_ind, b.ChronicPulm_ind, site_id, min_covid_dt
FROM Collect_the_Cohort b
WHERE b.person_id NOT IN (select hc.person_id from Hospitalized_Cases hc)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a4feda72-64be-4985-8a55-657749888cbe"),
    features_or_query=Input(rid="ri.foundry.main.dataset.a5009cc7-ae24-4962-8122-1b73d41fadb1")
)
SELECT *
FROM features_or_query

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c8056f3f-e7a7-4aee-9297-2585f9c01b3f"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT concept_id 
FROM concept_set_members
WHERE concept_set_name in ('ResultPos') AND is_most_recent_version = True

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e40d009b-ef46-421d-b092-ad6b522d767b"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT *
FROM concept_set_members c
where c.concept_set_name = '[DATOS - Charlson] Renal Disease' and is_most_recent_version = 'true'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eb26827f-e0cc-46c5-b2fa-050499d3d907"),
    All_Index_Dates=Input(rid="ri.foundry.main.dataset.0a8e744f-741b-491d-9f1e-fa900c2b96ca"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
SELECT distinct aid.*
FROM All_Index_Dates aid 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.276991c0-5fc2-45e1-8f40-cba5d371ffe0"),
    All_Index_Dates=Input(rid="ri.foundry.main.dataset.0a8e744f-741b-491d-9f1e-fa900c2b96ca"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
--make sure all patients have at least 180 days of history with their site
--make sure that visit has a dx code
SELECT ai.person_id, ai.min_covid_dt, msh.run_date, min(vo.visit_start_date) as min_visit_dt, datediff(msh.run_date, min(vo.visit_start_date)) as HX_DAYS
FROM All_Index_Dates ai JOIN microvisit_to_macrovisit_lds vo ON ai.person_id = vo.person_id
    JOIN manifest msh ON vo.data_partner_id = msh.data_partner_id
    JOIN condition_occurrence co ON vo.visit_occurrence_id = co.visit_occurrence_id
GROUP BY ai.person_id, ai.min_covid_dt, msh.run_date
HAVING datediff(ai.min_covid_dt, min(vo.visit_start_date)) >= datediff(msh.run_date, ai.min_covid_dt) OR datediff(ai.min_covid_dt, min(vo.visit_start_date)) >= 180

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ac3110b2-69d1-411b-83ad-140a8edce7e3"),
    DrugConcepts=Input(rid="ri.foundry.main.dataset.234ad98b-9878-453a-bd23-b44b9690bfdb"),
    Drugs_for_These_Patients=Input(rid="ri.foundry.main.dataset.681cb4f9-b553-46d4-9db7-a0a1b41ca957"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c")
)
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id, ds.drug_concept_id as original_drug_concept_id, ds.drug_concept_name as original_drug_concept_name, dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
--using only the portion of concept_ancestor where the ancestors are rxnorm ingredients and are standard concepts.
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id --the ingredients are the ancestors
--if a med for one of our patients is a descendent of one of those ingredients (presumably all but should verify, we will capture that here.)
    JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id --the original meds are the descendents 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0e6f4d69-8297-4b4d-b27b-4656bd42875d"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_ancestor=Input(rid="ri.foundry.main.dataset.c5e0521a-147e-4608-b71e-8f53bcdbe03c"),
    long_covid_clinic_dates=Input(rid="ri.foundry.main.dataset.01fc5667-fdc0-4577-902a-a44159435fb8"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.e225d916-8639-4b63-80d3-03ec344e3fa2")
)
SELECT ct.concept_name as parent_concept_name, pp.condition_concept_name as child_concept_name, ca.min_levels_of_separation as min_hops_bt_parent_child, ca.max_levels_of_separation as max_hops_bt_parent_child, pp.condition_concept_id as child_concept_id, count(distinct pp.person_id) as ptct_training, ((count(distinct pp.person_id))/386)*100 as perc_pt_training
FROM pre_post_dx_count_clean pp JOIN long_covid_clinic_dates lc on lc.person_id = pp.person_id
    JOIN concept_ancestor ca ON pp.condition_concept_id = ca.descendant_concept_id
    JOIN concept ct ON ca.ancestor_concept_id = ct.concept_id
WHERE ct.concept_name NOT IN (
'General problem AND/OR complaint',
'Disease',
'Sequelae of disorders classified by disorder-system',
'Sequela of disorder',
'Sequela',
'Recurrent disease',
'Problem',
'Acute disease',
'Chronic disease',
'Complication'
) 
and (lower(ct.concept_name) NOT LIKE '%finding%' or lower(pp.condition_concept_name) LIKE '%finding%')
and (lower(ct.concept_name) NOT LIKE '%disorder of%' or lower(pp.condition_concept_name) LIKE '%disorder of%')
and (lower(ct.concept_name) NOT LIKE '%by site%' or lower(pp.condition_concept_name) LIKE '%by site%')
and (lower(ct.concept_name) NOT LIKE '%right%')
and (lower(ct.concept_name) NOT LIKE '%left%')
and ca.min_levels_of_separation between 0 and 2 
group by ct.concept_name, pp.condition_concept_name, pp.condition_concept_id, ca.min_levels_of_separation, ca.max_levels_of_separation

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3526f8b6-e558-423f-8bc3-226861c96913"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    long_covid_clinic_dates=Input(rid="ri.foundry.main.dataset.01fc5667-fdc0-4577-902a-a44159435fb8"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
SELECT distinct feat.*
FROM long_covid_clinic_dates lc JOIN Feature_Table_Builder feat ON lc.person_id = feat.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6d3cfa8d-65f5-4b47-b9c6-e96d1f205cda"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    feature_set_flagged_only=Input(rid="ri.foundry.main.dataset.3526f8b6-e558-423f-8bc3-226861c96913")
)
SELECT *
FROM Feature_Table_Builder
WHERE person_id NOT IN (select person_id from feature_set_flagged_only)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a5009cc7-ae24-4962-8122-1b73d41fadb1"),
    or1_calc=Input(rid="ri.foundry.main.dataset.3f6e64c2-01b0-4a31-906d-2bfd35195bf7"),
    or2_calc=Input(rid="ri.foundry.main.dataset.e7885361-97f3-40a4-a0da-be3bee24279e"),
    shap_importance=Input(rid="ri.foundry.main.dataset.43fc08ff-2d7a-4139-84f5-0575fb60c83f")
)
SELECT concept,
       concept                     AS subgroup,
       Round(total_odds, 3)        AS total_odds,
       Round(total_upper_ci, 3)    AS total_upper_ci,
       Round(total_lower_ci, 3)    AS total_lower_ci,
       Round(hosp_odds, 3)         AS hosp_odds,
       Round(hosp_upper_ci, 3)     AS hosp_upper_ci,
       Round(hosp_lower_ci, 3)     AS hosp_lower_ci,
       Round(nonhosp_odds, 3)      AS nonhosp_odds,
       Round(nonhosp_upper_ci, 3)  AS nonhosp_upper_ci,
       Round (nonhosp_lower_ci, 3) AS nonhosp_lower_ci,
       imp.*
FROM   or2_calc
       LEFT JOIN shap_importance AS imp
              ON Lower(concept) = Lower(imp.feature)
UNION
SELECT concept,
       subgroup,
       Round(total_odds, 3)        AS total_odds,
       Round(total_upper_ci, 3)    AS total_upper_ci,
       Round(total_lower_ci, 3)    AS total_lower_ci,
       Round(hosp_odds, 3)         AS hosp_odds,
       Round(hosp_upper_ci, 3)     AS hosp_upper_ci,
       Round(hosp_lower_ci, 3)     AS hosp_lower_ci,
       Round(nonhosp_odds, 3)      AS nonhosp_odds,
       Round(nonhosp_upper_ci, 3)  AS nonhosp_upper_ci,
       Round (nonhosp_lower_ci, 3) AS nonhosp_lower_ci,
       imp.*
FROM   or1_calc
       LEFT JOIN shap_importance AS imp
              ON Lower(concept) = Lower(imp.feature);  

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6f664bc4-0316-4d43-aab8-f589b38106fc"),
    pre_post_labs_clean=Input(rid="ri.foundry.main.dataset.28b39a7e-00ec-4d2f-8c62-a9e82ad899d5")
)
SELECT concept_set_name, count(*) as lab_ct
FROM pre_post_labs_clean
group by concept_set_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fe0a4ada-27af-448d-ac24-40e0fd1da81d"),
    Labs_for_These_Sites=Input(rid="ri.foundry.main.dataset.a6f60ab6-946c-4d49-a715-3726d90364ab")
)
SELECT concept_set_name, harmonized_unit_concept_id, avg(harmonized_value_as_number) as pop_avg, stddev(harmonized_value_as_number) as stddev
FROM Labs_for_These_Sites
WHERE harmonized_value_as_number is not null
GROUP BY concept_set_name, harmonized_unit_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.01fc5667-fdc0-4577-902a-a44159435fb8"),
    long_covid_person_ids_w_dates=Input(rid="ri.foundry.main.dataset.7bcbe56e-76b0-49fa-9bd2-231ce6ee7ad9")
)
SELECT person_id, min(long_covid_clinic_date) as first_LC_clin_dt
FROM long_covid_person_ids_w_dates
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5389a75e-d6e4-4230-9ff2-a15e84cdeddf"),
    dxs_for_melissa=Input(rid="ri.foundry.main.dataset.0e6f4d69-8297-4b4d-b27b-4656bd42875d")
)
SELECT parent_concept_name, sum(ptct_training) as tot_pts
FROM dxs_for_melissa
GROUP BY parent_concept_name
HAVING sum(ptct_training) >= 3

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1bf49a88-75c2-4342-9c01-33bac887dad7"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.28a8e6b8-602f-436c-92db-67e75cbea129"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    drugRollUp=Input(rid="ri.foundry.main.dataset.ac3110b2-69d1-411b-83ad-140a8edce7e3")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d531ecd7-cf2e-4048-ae72-77dd74f81e7e"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    Labs_for_These_Sites=Input(rid="ri.foundry.main.dataset.a6f60ab6-946c-4d49-a715-3726d90364ab")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ distinct feat.*, co.concept_set_name, co.measurement_date, co.visit_occurrence_id, co.harmonized_value_as_number, co.harmonized_unit_concept_id
FROM Feature_Table_Builder feat JOIN Labs_for_These_Sites co ON feat.person_id = co.person_id and co.measurement_date between feat.post_window_start_dt and feat.post_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3294451a-a32c-4d4f-b66b-19bded51895a"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ feat.*, co.condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN condition_occurrence co ON feat.person_id = co.person_id and co.condition_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bc1ab4c5-1f00-4c49-9df8-cb93ba9acab8"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    drugRollUp=Input(rid="ri.foundry.main.dataset.ac3110b2-69d1-411b-83ad-140a8edce7e3")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.871ffc93-bf00-4388-97d4-19da9a159cab"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    Labs_for_These_Sites=Input(rid="ri.foundry.main.dataset.a6f60ab6-946c-4d49-a715-3726d90364ab")
)
SELECT /*+ BROADCAST(Feature_Table_Builder) */ distinct feat.*, co.concept_set_name, co.measurement_date, co.visit_occurrence_id, co.harmonized_value_as_number, co.harmonized_unit_concept_id
FROM Feature_Table_Builder feat JOIN Labs_for_These_Sites co ON feat.person_id = co.person_id and co.measurement_date between feat.pre_window_start_dt and feat.pre_window_end_dt

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e225d916-8639-4b63-80d3-03ec344e3fa2"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    pre_post_dx_counts=Input(rid="ri.foundry.main.dataset.0d44eec5-22a1-477c-af48-ec9dbcb13c32"),
    tot_ip_days_calc=Input(rid="ri.foundry.main.dataset.781d40d8-a312-4df5-b93a-274087038ded")
)
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.diabetes_ind, feat.kidney_ind, feat.CHF_ind, feat.ChronicPulm_ind, feat.tot_long_data_days, feat.post_visits_per_pt_day as op_post_visit_ratio, (nvl(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT 
nvl(pre_person_id, post_person_id) as person_id, nvl(pre_patient_group, post_patient_group) as patient_group, nvl(pre_condition_concept_name, post_condition_concept_name) as condition_concept_name, nvl(pre_condition_concept_id, post_condition_concept_id) as condition_concept_id, nvl(pre_dx_count, 0) as pre_dx_count, nvl(post_dx_count, 0) as post_dx_count
FROM pre_post_dx_counts) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d44eec5-22a1-477c-af48-ec9dbcb13c32"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    post_condition=Input(rid="ri.foundry.main.dataset.1bf49a88-75c2-4342-9c01-33bac887dad7"),
    pre_condition=Input(rid="ri.foundry.main.dataset.3294451a-a32c-4d4f-b66b-19bded51895a")
)
SELECT 
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.condition_concept_name as pre_condition_concept_name, pretbl.condition_concept_id as pre_condition_concept_id, pretbl.count_type as pre_count_type, pretbl.dx_count as pre_dx_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.condition_concept_name as post_condition_concept_name, posttbl.condition_concept_id as post_condition_concept_id, posttbl.count_type as post_count_type, posttbl.dx_count as post_dx_count
FROM 
(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
FROM Feature_Table_Builder feat 
    JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    JOIN pre_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY 
feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id) pretbl

FULL JOIN

(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'post count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as dx_count
FROM Feature_Table_Builder feat 
    JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    JOIN post_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY 
feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id) posttbl ON pretbl.person_id = posttbl.person_id AND pretbl.condition_concept_name = posttbl.condition_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7dc39da0-c297-42d6-ba31-74104528e1be"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    post_labs=Input(rid="ri.foundry.main.dataset.d531ecd7-cf2e-4048-ae72-77dd74f81e7e"),
    pre_labs=Input(rid="ri.foundry.main.dataset.871ffc93-bf00-4388-97d4-19da9a159cab")
)
SELECT 
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.concept_set_name as pre_lab_concept_set_name, pretbl.count_type as pre_lab_type, pretbl.min_val as pre_min_val, pretbl.max_val as pre_max_val, pretbl.avg_val as pre_avg_val,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.concept_set_name as post_lab_concept_set_name, posttbl.count_type as post_lab_type, posttbl.min_val as post_min_val, posttbl.max_val as post_max_val, posttbl.avg_val as post_avg_val
FROM 
(SELECT feat.person_id, feat.patient_group, prc.concept_set_name, 'pre' as count_type, min(prc.harmonized_value_as_number) as min_val, max(prc.harmonized_value_as_number) as max_val, avg(prc.harmonized_value_as_number) as avg_val
FROM Feature_Table_Builder feat 
    JOIN pre_labs prc ON feat.person_id = prc.person_id
GROUP BY 
feat.person_id, feat.patient_group, prc.concept_set_name) pretbl

FULL JOIN

(SELECT feat.person_id, feat.patient_group, prc.concept_set_name, 'post' as count_type, min(prc.harmonized_value_as_number) as min_val, max(prc.harmonized_value_as_number) as max_val, avg(prc.harmonized_value_as_number) as avg_val
FROM Feature_Table_Builder feat 
    JOIN post_labs prc ON feat.person_id = prc.person_id
GROUP BY 
feat.person_id, feat.patient_group, prc.concept_set_name) posttbl 

ON pretbl.person_id = posttbl.person_id AND pretbl.concept_set_name = posttbl.concept_set_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.28b39a7e-00ec-4d2f-8c62-a9e82ad899d5"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    pre_post_labs=Input(rid="ri.foundry.main.dataset.7dc39da0-c297-42d6-ba31-74104528e1be"),
    tot_ip_days_calc=Input(rid="ri.foundry.main.dataset.781d40d8-a312-4df5-b93a-274087038ded")
)
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.diabetes_ind, feat.kidney_ind, feat.CHF_ind, feat.ChronicPulm_ind, feat.tot_long_data_days, feat.post_visits_per_pt_day as op_post_visit_ratio, (nvl(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT 
nvl(pre_person_id, post_person_id) as person_id, nvl(pre_patient_group, post_patient_group) as patient_group, nvl(pre_lab_concept_set_name, post_lab_concept_set_name) as concept_set_name, nvl(pre_min_val, -999) as pre_min_val, nvl(post_min_val, -999) as post_min_val, nvl(pre_max_val, -999) as pre_max_val, nvl(post_max_val, -999) as post_max_val, nvl(pre_avg_val, -999) as pre_avg_val, nvl(post_avg_val, -999) as post_avg_val
FROM pre_post_labs) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8dee751-5f74-435f-9c2a-d2617532923d"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    post_drugs=Input(rid="ri.foundry.main.dataset.28a8e6b8-602f-436c-92db-67e75cbea129"),
    pre_drugs=Input(rid="ri.foundry.main.dataset.bc1ab4c5-1f00-4c49-9df8-cb93ba9acab8")
)
SELECT 
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.ancestor_drug_concept_name as pre_ancestor_drug_concept_name, pretbl.ancestor_drug_concept_id as pre_ancestor_drug_concept_id, pretbl.count_type as pre_count_type, pretbl.med_count as pre_med_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.ancestor_drug_concept_name as post_ancestor_drug_concept_name, posttbl.ancestor_drug_concept_id as post_ancestor_drug_concept_id, posttbl.count_type as post_count_type, posttbl.med_count as post_med_count
FROM 
(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
FROM Feature_Table_Builder feat 
    JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY 
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id) pretbl

FULL JOIN

(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
FROM Feature_Table_Builder feat 
    JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY 
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id) posttbl ON pretbl.person_id = posttbl.person_id AND pretbl.ancestor_drug_concept_name = posttbl.ancestor_drug_concept_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.158f1334-0395-4ba7-b2f3-7ecba87280a7"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    pre_post_med_count=Input(rid="ri.foundry.main.dataset.b8dee751-5f74-435f-9c2a-d2617532923d"),
    tot_ip_days_calc=Input(rid="ri.foundry.main.dataset.781d40d8-a312-4df5-b93a-274087038ded")
)
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.diabetes_ind, feat.kidney_ind, feat.CHF_ind, feat.ChronicPulm_ind, feat.tot_long_data_days, feat.post_visits_per_pt_day as op_post_visit_ratio, (nvl(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT 
nvl(pre_person_id, post_person_id) as person_id, nvl(pre_patient_group, post_patient_group) as patient_group, nvl(pre_ancestor_drug_concept_name, post_ancestor_drug_concept_name) as ancestor_drug_concept_name, nvl(pre_ancestor_drug_concept_id, post_ancestor_drug_concept_id) as ancestor_drug_concept_id, nvl(pre_med_count, 0) as pre_med_count, nvl(post_med_count, 0) as post_med_count
FROM pre_post_med_count) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ec39ea28-dc01-4143-ab29-31a4c52f4f56"),
    feature_set_flagged_only=Input(rid="ri.foundry.main.dataset.3526f8b6-e558-423f-8bc3-226861c96913"),
    feature_set_notflagged_only=Input(rid="ri.foundry.main.dataset.6d3cfa8d-65f5-4b47-b9c6-e96d1f205cda"),
    people_with_sufficient_history=Input(rid="ri.foundry.main.dataset.1bfa259c-dd2d-4d5c-a219-09251a050100")
)
SELECT f.person_id,
    f.apprx_age,
    case when 
        f.apprx_age between 18 and 35 then '18-35'
        when f.apprx_age between 36 and 45 then '36-45'
        when f.apprx_age between 46 and 55 then '46-55'
        when f.apprx_age between 56 and 65 then '56-65'
        when f.apprx_age >=66 then '66+'
        else 'unknown' end as age_group,
    case when f.race IN ('Asian Indian','Chinese','Filipino','Japanese','Korean','Vietnamese') then 'Asian' 
        when f.race = 'Black or African American' then 'Black'
        when f.race IN ('Hispanic','More than one race','Multiple race','Other','Non-white','Other Race') then 'Other'
        when f.race IN ('No information','No matching concept','Unknown racial group','Refuse to answer') then 'Unknown'
        when f.race IN ('Other Pacific Islander','Polynesian') then 'Native Hawaiian or Other Pacific Islander' 
        else f.race end as race_norm,
    case when f.ethn IN ('No information','No matching concept','Other','Patient ethnicity unknown') then 'Unknown'
        else f.ethn end as ethn_norm,
case when f.sex IN ('AMBIGUOUS','No matching concept','Unknown') then 'UNKNOWN'
    else f.sex end as sex_norm,
    f.diabetes_ind,
    f.kidney_ind,
    f.CHF_ind,
    f.ChronicPulm_ind,
    f.patient_group as Hosp_ind,
    f.tot_long_data_days as longitu_data_avail,
    f.post_visits_per_pt_day,
    case when f.patient_group = 'CASE_HOSP' then 'Long-COVID Clinic HOSP' 
        when f.patient_group = 'CASE_NONHOSP' then 'Long-COVID Clinic NONHOSP' end as theGroup
FROM feature_set_flagged_only f JOIN people_with_sufficient_history p ON f.person_id = p.person_id

UNION

SELECT f.person_id,
f.apprx_age,
case when 
        f.apprx_age between 18 and 35 then '18-35'
        when f.apprx_age between 36 and 45 then '36-45'
        when f.apprx_age between 46 and 55 then '46-55'
        when f.apprx_age between 56 and 65 then '56-65'
        when f.apprx_age >=66 then '66+'
        else 'unknown' end as age_group,
case when f.race IN ('Asian Indian','Chinese','Filipino','Japanese','Korean','Vietnamese') then 'Asian' 
        when f.race = 'Black or African American' then 'Black'
        when f.race IN ('Hispanic','More than one race','Multiple race','Other','Non-white','Other Race') then 'Other'
        when f.race IN ('No information','No matching concept','Unknown racial group','Refuse to answer') then 'Unknown'
        when f.race IN ('Other Pacific Islander','Polynesian') then 'Native Hawaiian or Other Pacific Islander' 
        else f.race end as race_norm,
case when f.ethn IN ('No information','No matching concept','Other','Patient ethnicity unknown') then 'Unknown'
        else f.ethn end as ethn_norm,
case when f.sex IN ('AMBIGUOUS','No matching concept','Unknown') then 'UNKNOWN'
    else f.sex end as sex_norm,
    f.diabetes_ind,
    f.kidney_ind,
    f.CHF_ind,
    f.ChronicPulm_ind,
    f.patient_group as Hosp_ind,
    f.tot_long_data_days as longitu_data_avail,
    f.post_visits_per_pt_day,
    case when f.patient_group = 'CASE_HOSP' then 'NOT Long-COVID Clinic HOSP' 
        when f.patient_group = 'CASE_NONHOSP' then 'NOT Long-COVID Clinic NONHOSP' end as theGroup
FROM feature_set_notflagged_only f JOIN people_with_sufficient_history p ON f.person_id = p.person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.ab906a7b-bc6a-4269-941b-2696e5637001"),
    dxs_for_melissa=Input(rid="ri.foundry.main.dataset.0e6f4d69-8297-4b4d-b27b-4656bd42875d")
)
SELECT count(distinct child_concept_name)
FROM dxs_for_melissa

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.481cd491-9e31-4b6a-8fdf-2853831bc9ce"),
    FINAL_rollups=Input(rid="ri.foundry.main.dataset.9493c3e2-1249-48a1-a409-73c13b452d43")
)
SELECT *
FROM roll_ups_for_andrew

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c364f676-e9c1-4fcb-8c25-9c322f6c8734"),
    people_with_sufficient_history=Input(rid="ri.foundry.main.dataset.1bfa259c-dd2d-4d5c-a219-09251a050100"),
    pts_for_tbl1=Input(rid="ri.foundry.main.dataset.ec39ea28-dc01-4143-ab29-31a4c52f4f56")
)
SELECT t.person_id                              AS p_person_id,
       cast (p.apprx_age AS DOUBLE)             AS p_apprx_age,
       cast (p.tot_long_data_days AS DOUBLE)    AS p_tot_long_data_days,
       cast (p.op_post_visit_ratio AS DOUBLE)   AS p_op_post_visit_ratio,
       cast (p.ip_post_visit_ratio AS DOUBLE)   AS p_ip_post_visit_ratio,
       cast (p.sex_male AS DOUBLE)              AS p_sex_male,
       cast (p.diabetes_ind_yes AS DOUBLE)      AS p_diabetes_ind_yes,
       cast (p.kidney_ind_yes AS DOUBLE)        AS p_kidney_ind_yes,
       cast (p.chf_ind_yes AS DOUBLE)           AS p_chf_ind_yes,
       cast (p.chronicpulm_ind_yes AS DOUBLE)   AS p_chronicpulm_ind_yes,
       cast (p.total_pre_dx AS DOUBLE)          AS p_total_pre_dx,
       cast (p.total_post_dx AS DOUBLE)         AS p_total_post_dx,
       p.*
FROM   pts_for_tbl1 as t
JOIN   people_with_sufficient_history as p
ON     t.person_id = p.person_id ; 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dacbe0f6-d0cc-4173-a77a-5d791b48a039"),
    setup_features_or=Input(rid="ri.foundry.main.dataset.c364f676-e9c1-4fcb-8c25-9c322f6c8734")
)
SELECT CASE
         WHEN p_apprx_age BETWEEN 18 AND 25 THEN '18-25'
         WHEN p_apprx_age BETWEEN 26 AND 45 THEN '26-45'
         WHEN p_apprx_age BETWEEN 46 AND 55 THEN '46-55'
         WHEN p_apprx_age BETWEEN 56 AND 65 THEN '56-65'
         WHEN p_apprx_age >= 66 THEN '66+'
         ELSE 'unknown'
       END                           AS apprx_age,
       Float (p_op_post_visit_ratio) AS op_post_visit_ratio,
       Float (p_ip_post_visit_ratio) AS ip_post_visit_ratio,
       long_covid,
       hospitalized
FROM   setup_features_or  

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.43fc08ff-2d7a-4139-84f5-0575fb60c83f"),
    shap_imp_hosp=Input(rid="ri.foundry.main.dataset.b4ed5f02-4b32-4dae-95fa-3c4844edc22c"),
    shap_imp_nohosp=Input(rid="ri.foundry.main.dataset.b4ed5f02-4b32-4dae-95fa-3c4844edc22c"),
    shap_imp_total=Input(rid="ri.foundry.main.dataset.b4ed5f02-4b32-4dae-95fa-3c4844edc22c")
)
SELECT total.feature,
    total.importance AS total_imp,
    nohosp.importance AS nohosp_imp,
    hosp.importance AS hosp_imp
FROM shap_imp_total as total
    JOIN shap_imp_nohosp as nohosp
    on nohosp.feature = total.feature
    JOIN shap_imp_hosp as hosp
    on hosp.feature = total.feature

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f43ebb66-cf1e-497c-aaa6-b32ca1dec325"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.e225d916-8639-4b63-80d3-03ec344e3fa2")
)
SELECT pp.condition_concept_id, pp.condition_concept_name, count(distinct pp.person_id) as dxct
FROM pre_post_dx_count_clean pp JOIN Feature_Table_Builder feat ON pp.person_id = feat.person_id
WHERE pp.post_dx_count > 0 and feat.patient_type = 'TRAINING'
GROUP BY pp.condition_concept_id, pp.condition_concept_name
HAVING count(distinct pp.person_id) > 3

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.781d40d8-a312-4df5-b93a-274087038ded"),
    Feature_Table_Builder=Input(rid="ri.foundry.main.dataset.451054d1-907b-4b7e-87af-a827ace78b95"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
SELECT 
    person_id, post_window_start_dt, post_window_end_dt, sum(LOS) as tot_ip_days
FROM (
SELECT distinct feat.person_id, 
feat.post_window_start_dt,
feat.post_window_end_dt,
mm.macrovisit_start_date, 
mm.macrovisit_end_date, 
(datediff(mm.macrovisit_end_date,mm.macrovisit_start_date) + 1) as LOS
FROM Feature_Table_Builder feat JOIN microvisit_to_macrovisit_lds mm ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt

@transform_pandas(
    Output(rid="ri.vector.main.execute.58d1cca3-a668-4e07-aead-00b09748920d"),
    long_covid_clinic_dates=Input(rid="ri.foundry.main.dataset.01fc5667-fdc0-4577-902a-a44159435fb8"),
    pre_post_dx_count_clean=Input(rid="ri.foundry.main.dataset.e225d916-8639-4b63-80d3-03ec344e3fa2")
)
SELECT count(distinct condition_concept_name) as dx_ct
FROM pre_post_dx_count_clean pp JOIN long_covid_clinic_dates lc on pp.person_id = lc.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.55ac751a-15dc-4aa2-a382-c1efc9676967"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f")
)
SELECT *
FROM manifest

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b99242e1-289e-4759-ab65-c5008fbda654"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
SELECT csm.*, con.domain_id
FROM concept_set_members csm JOIN concept con ON csm.concept_id = con.concept_id
WHERE concept_set_name = 'Invasive Mechanical Ventilation 2OCT20' and is_most_recent_version = true

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.db362b00-03cb-4789-ada3-e6dbf3a95387"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
SELECT distinct visit_start_date, visit_end_date, visit_concept_name, macrovisit_start_date, macrovisit_end_date
FROM microvisit_to_macrovisit_lds
where person_id = 2989086611756982779 and visit_start_date between '2021-04-12' and '2021-10-01' and macrovisit_id is not null
order by visit_start_date asc

