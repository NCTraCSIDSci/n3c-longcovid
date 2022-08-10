"""
We did not have permission in AOU Workspace to create intermediate tables. So, I have created temporary tables to pull intermediate data.
Also, these temporary tables are only valid for a single transaction.
"""

import pandas as pd


inpatient_microvisits_sql = f"""
SELECT * FROM visit_occurrence WHERE
(visit_start_date IS NOT NULL AND visit_end_date IS NOT NULL)
AND visit_start_date <= visit_end_date AND (visit_concept_id in (9201, 8717, 262, 32037, 581379)
or (visit_concept_id = 9202 AND DATE_DIFF(visit_end_date,visit_start_date, DAY) = 1)
or (visit_concept_id = 9203 AND DATE_DIFF(visit_end_date,visit_start_date, DAY) >= 1))
"""

merging_intervals_sql = f"""
with inpatient_microvisits as
({inpatient_microvisits_sql}),
merging_intervals_a as
(
SELECT  person_id, visit_start_date, visit_end_date,
MAX(visit_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) AS max_end_date,
RANK() OVER (PARTITION BY person_id ORDER by visit_start_date) AS rank_value
FROM inpatient_microvisits
),
merging_intervals_b as
(
 SELECT  *, CASE WHEN visit_start_date <= LAG(max_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) THEN 0 ELSE 1
END AS gap FROM merging_intervals_a
),
merging_intervals_c as
(
SELECT *, SUM(gap) OVER (PARTITION BY person_id ORDER by visit_start_date) AS group_number FROM merging_intervals_b
)
SELECT  person_id, group_number, MIN(visit_start_date) AS macrovisit_start_date, MAX(visit_end_date) AS macrovisit_end_date,
CONCAT(person_id, '_', group_number, '_', ABS(MOD(FARM_FINGERPRINT(STRING(min(visit_start_date))), 10))) AS macrovisit_id
FROM merging_intervals_c
GROUP BY person_id, group_number
ORDER BY person_id, group_number
"""

initial_macrovisits_with_microvisits_sql = f"""
with merging_intervals as
({merging_intervals_sql})
SELECT  v.person_id, v.provider_id, v.visit_occurrence_id, v.visit_concept_id, m.macrovisit_id, m.macrovisit_start_date, m.macrovisit_end_date
FROM visit_occurrence v LEFT JOIN merging_intervals m
ON v.person_id = m.person_id
AND v.visit_start_date >= m.macrovisit_start_date
AND v.visit_start_date <= m.macrovisit_end_date
"""

final_microvisits_to_macrovisits_sql = f"""
BEGIN TRANSACTION;
create TEMP TABLE microvisits_to_macrovisits as
with initial_macrovisits_with_microvisits as
({initial_macrovisits_with_microvisits_sql}),
ip_and_er_microvisits AS
(
    SELECT DISTINCT macrovisit_id
    FROM initial_macrovisits_with_microvisits
    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 9203)
),
macrovisits_containing_at_least_one_ip_visit AS
(
    SELECT m.*
    FROM initial_macrovisits_with_microvisits m
    INNER JOIN ip_and_er_microvisits i
    ON m.macrovisit_id = i.macrovisit_id
)
SELECT v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM visit_occurrence v
LEFT JOIN macrovisits_containing_at_least_one_ip_visit m
ON v.visit_occurrence_id = m.visit_occurrence_id;
COMMIT TRANSACTION;
select * from microvisits_to_macrovisits;
"""

microvisits_to_macrovisits = pd.read_gbq(final_microvisits_to_macrovisits_sql, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")

microvisits_to_macrovisits.head()

cohort_and_features_sql = f"""
BEGIN TRANSACTION;
create TEMP TABLE microvisits_to_macrovisits as
with initial_macrovisits_with_microvisits as
({initial_macrovisits_with_microvisits_sql}),
ip_and_er_microvisits AS
(
    SELECT DISTINCT macrovisit_id
    FROM initial_macrovisits_with_microvisits
    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 9203)
),
macrovisits_containing_at_least_one_ip_visit AS
(
    SELECT m.*
    FROM initial_macrovisits_with_microvisits m
    INNER JOIN ip_and_er_microvisits i
    ON m.macrovisit_id = i.macrovisit_id
)
SELECT v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM visit_occurrence v
LEFT JOIN macrovisits_containing_at_least_one_ip_visit m
ON v.visit_occurrence_id = m.visit_occurrence_id;
COMMIT TRANSACTION;
with Positive_Results as (
select concept_id from concept
where concept_id IN (9191,4126681,36032716,36715206,45878745,45881802,45877985,45884084)
),
COVID_Tests as (
select concept_id from concept
where concept_id IN (586520,586523,586525,586526,586529,706157,706159,715261,715272,723470,723472,757678,36032061,36032174,36032258,36661371,586518,586524,706154,706175,723464,723467,723478,36031453,586516,706158,706160,706163,706171,706172,715260,723469,36031213,36661377,586528,706161,706165,706167,723463,723468,723471,757677,36031238,36031944,586519,706166,706169,706173,723465,723476,757685,36031506,706155,706156,706170,723466,36031652,36661370,706168,706174,715262,723477,36032419,36661378,37310257)
),
u099 as (
SELECT co.person_id, min(co.condition_start_date) as u099_idx
FROM condition_occurrence co
WHERE co.condition_concept_id IN (710706,705076) or co.condition_source_value LIKE '%U09.9%'
GROUP BY co.person_id
),
u07_any as (
SELECT co.person_id, min(co.condition_start_date) as u07_any_idx
FROM condition_occurrence co
WHERE co.condition_concept_id = 37311061
GROUP BY co.person_id
),
COVID_Lab_Index as (
SELECT m.person_id, min(m.measurement_date) as pos_test_idx
FROM measurement m JOIN COVID_Tests p on m.measurement_concept_id = p.concept_id
JOIN Positive_Results pr on m.value_as_concept_id = pr.concept_id
GROUP BY m.person_id
),
person_union as (
SELECT person_id FROM u07_any
UNION distinct
SELECT person_id FROM u099
UNION distinct
SELECT person_id FROM COVID_Lab_Index
),
All_Index_Dates as (
SELECT pu.person_id, lp.pos_test_idx, ua.u07_any_idx, u9.u099_idx
FROM person_union pu LEFT JOIN u099 u9 ON pu.person_id = u9.person_id
LEFT JOIN u07_any ua ON pu.person_id = ua.person_id
LEFT JOIN COVID_Lab_Index lp ON pu.person_id = lp.person_id
),
Collect_the_Cohort as (
SELECT distinct
    pr.person_id,
    (2021 - pr.year_of_birth) as apprx_age,
    (select concept_name from concept where concept_id=gender_concept_id) as sex,
    (select concept_name from concept where concept_id=race_concept_id) as race,
    (select concept_name from concept where concept_id=ethnicity_concept_id) as ethn,
    pr.care_site_id as site_id,
    LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE())) as min_covid_dt
FROM All_Index_Dates vc
    JOIN person pr ON vc.person_id = pr.person_id
    LEFT JOIN death d ON vc.person_id = d.person_id
WHERE
    date_diff('2022-06-22',LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE()), IFNULL(d.death_date, CURRENT_DATE())), DAY) >= 100
),
Hospitalized_Cases as (
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between DATE_SUB(b.min_covid_dt, INTERVAL 14 DAY) and DATE_ADD(b.min_covid_dt, INTERVAL 14 DAY)
UNION ALL
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061
),
hosp_and_non as (
SELECT ctc.*,
case when hc.person_id is not null
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
    LEFT JOIN Hospitalized_Cases hc ON ctc.person_id = hc.person_id
)
SELECT * FROM
(SELECT ottbl.*, IFNULL(count(distinct visit_start_date),0) as post_visits_count,
date_diff(ottbl.post_window_end_dt,ottbl.post_window_start_dt, DAY) as tot_post_days,
IFNULL(count(distinct visit_start_date),0)/300 as post_visits_per_pt_day FROM
(SELECT distinct
hc.*,
date_diff(max(mm.visit_start_date),min(mm.visit_start_date), DAY) as tot_long_data_days,
DATE_SUB(hc.min_covid_dt, INTERVAL 365 DAY) as pre_window_start_dt,
DATE_SUB(hc.min_covid_dt, INTERVAL 45 DAY) as pre_window_end_dt,
DATE_ADD(hc.min_covid_dt, INTERVAL 45 DAY) as post_window_start_dt,
case when DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY) < '2022-06-22'
then DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY)
else '2022-06-22' end as post_window_end_dt
FROM hosp_and_non hc JOIN microvisits_to_macrovisits mm ON hc.person_id = mm.person_id
LEFT JOIN All_Index_Dates lc ON hc.person_id = lc.person_id
GROUP BY
hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, lc.u099_idx
) ottbl
LEFT JOIN microvisits_to_macrovisits mmpost ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and
ottbl.post_window_end_dt and mmpost.macrovisit_id is null)
WHERE tot_long_data_days > 1
GROUP BY ottbl.person_id, ottbl.patient_group, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt,
ottbl.post_window_start_dt, ottbl.post_window_end_dt) pts
WHERE post_visits_count >=1;
"""

Feature_Table_Builder_df = pd.read_gbq(cohort_and_features_sql, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")

med_feature_sql = f"""
BEGIN TRANSACTION;
create TEMP TABLE microvisits_to_macrovisits as
with initial_macrovisits_with_microvisits as
({initial_macrovisits_with_microvisits_sql}),
ip_and_er_microvisits AS
(
    SELECT DISTINCT macrovisit_id
    FROM initial_macrovisits_with_microvisits
    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 9203)
),
macrovisits_containing_at_least_one_ip_visit AS
(
    SELECT m.*
    FROM initial_macrovisits_with_microvisits m
    INNER JOIN ip_and_er_microvisits i
    ON m.macrovisit_id = i.macrovisit_id
)
SELECT v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM visit_occurrence v
LEFT JOIN macrovisits_containing_at_least_one_ip_visit m
ON v.visit_occurrence_id = m.visit_occurrence_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE Feature_Table_Builder as
with Positive_Results as (
select concept_id from concept
where concept_id IN (9191,4126681,36032716,36715206,45878745,45881802,45877985,45884084)
),
COVID_Tests as (
select concept_id from concept
where concept_id IN (586520,586523,586525,586526,586529,706157,706159,715261,715272,723470,723472,757678,36032061,36032174,36032258,36661371,586518,586524,706154,706175,723464,723467,723478,36031453,586516,706158,706160,706163,706171,706172,715260,723469,36031213,36661377,586528,706161,706165,706167,723463,723468,723471,757677,36031238,36031944,586519,706166,706169,706173,723465,723476,757685,36031506,706155,706156,706170,723466,36031652,36661370,706168,706174,715262,723477,36032419,36661378,37310257)
),
u099 as (
SELECT co.person_id, min(co.condition_start_date) as u099_idx
FROM condition_occurrence co
WHERE co.condition_concept_id IN (710706,705076) or co.condition_source_value LIKE '%U09.9%'
GROUP BY co.person_id
),
u07_any as (
SELECT co.person_id, min(co.condition_start_date) as u07_any_idx
FROM condition_occurrence co
WHERE co.condition_concept_id = 37311061
GROUP BY co.person_id
),
COVID_Lab_Index as (
SELECT m.person_id, min(m.measurement_date) as pos_test_idx
FROM measurement m JOIN COVID_Tests p on m.measurement_concept_id = p.concept_id
JOIN Positive_Results pr on m.value_as_concept_id = pr.concept_id
GROUP BY m.person_id
),
person_union as (
SELECT person_id FROM u07_any
UNION distinct
SELECT person_id FROM u099
UNION distinct
SELECT person_id FROM COVID_Lab_Index
),
All_Index_Dates as (
SELECT pu.person_id, lp.pos_test_idx, ua.u07_any_idx, u9.u099_idx
FROM person_union pu LEFT JOIN u099 u9 ON pu.person_id = u9.person_id
LEFT JOIN u07_any ua ON pu.person_id = ua.person_id
LEFT JOIN COVID_Lab_Index lp ON pu.person_id = lp.person_id
),
Collect_the_Cohort as (
SELECT distinct
    pr.person_id,
    (2021 - pr.year_of_birth) as apprx_age,
    (select concept_name from concept where concept_id=gender_concept_id) as sex,
    (select concept_name from concept where concept_id=race_concept_id) as race,
    (select concept_name from concept where concept_id=ethnicity_concept_id) as ethn,
    pr.care_site_id as site_id,
      LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE())) as min_covid_dt
FROM All_Index_Dates vc
    JOIN person pr ON vc.person_id = pr.person_id
    LEFT JOIN death d ON vc.person_id = d.person_id
WHERE
    date_diff('2022-06-22',LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE()),
    IFNULL(d.death_date, CURRENT_DATE())), DAY) >= 100
),
Hospitalized_Cases as (
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between DATE_SUB(b.min_covid_dt, INTERVAL 14 DAY) and
    DATE_ADD(b.min_covid_dt, INTERVAL 14 DAY)
UNION ALL
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061
),
hosp_and_non as (
SELECT ctc.*,
case when hc.person_id is not null
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
    LEFT JOIN Hospitalized_Cases hc ON ctc.person_id = hc.person_id
)
SELECT * FROM
(SELECT ottbl.*, IFNULL(count(distinct visit_start_date),0) as post_visits_count,
date_diff(ottbl.post_window_end_dt,ottbl.post_window_start_dt, DAY) as tot_post_days,
IFNULL(count(distinct visit_start_date),0)/300 as post_visits_per_pt_day FROM
(SELECT distinct
hc.*,
date_diff(max(mm.visit_start_date),min(mm.visit_start_date), DAY) as tot_long_data_days,
DATE_SUB(hc.min_covid_dt, INTERVAL 365 DAY) as pre_window_start_dt,
DATE_SUB(hc.min_covid_dt, INTERVAL 45 DAY) as pre_window_end_dt,
DATE_ADD(hc.min_covid_dt, INTERVAL 45 DAY) as post_window_start_dt,
case when DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY) < '2022-06-22'
then DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY)
else '2022-06-22' end as post_window_end_dt
FROM hosp_and_non hc JOIN microvisits_to_macrovisits mm ON hc.person_id = mm.person_id
LEFT JOIN All_Index_Dates lc ON hc.person_id = lc.person_id
GROUP BY
hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, lc.u099_idx
) ottbl
LEFT JOIN microvisits_to_macrovisits mmpost ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and
ottbl.post_window_end_dt and mmpost.macrovisit_id is null)
WHERE tot_long_data_days > 1
GROUP BY ottbl.person_id, ottbl.patient_group, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt,
ottbl.post_window_start_dt, ottbl.post_window_end_dt) pts
WHERE post_visits_count >=1;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE DrugConcepts as
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient'
and standard_concept = 'S';
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE tot_ip_days_calc as
SELECT person_id, post_window_start_dt, post_window_end_dt, sum(LOS) as tot_ip_days FROM
(
SELECT distinct feat.person_id, feat.post_window_start_dt, feat.post_window_end_dt, mm.macrovisit_start_date,
mm.macrovisit_end_date, (DATE_DIFF(mm.macrovisit_end_date,mm.macrovisit_start_date, DAY) + 1) as LOS
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mm ON feat.person_id = mm.person_id and mm.macrovisit_start_date
between feat.post_window_start_dt and feat.post_window_end_dt
) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE Drugs_for_These_Patients as
SELECT d.*
FROM drug_exposure d JOIN Feature_Table_Builder f ON d.person_id = f.person_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE drugRollUp as
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id,
ds.drug_concept_id as original_drug_concept_id, (select concept_name from concept where concept_id=ds.drug_concept_id)as original_drug_concept_name,
dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id
JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_drugs as
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date,
co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id
and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE post_drugs as
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and
co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_post_med_count as
SELECT
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.ancestor_drug_concept_name as pre_ancestor_drug_concept_name,
pretbl.ancestor_drug_concept_id as pre_ancestor_drug_concept_id, pretbl.count_type as pre_count_type, pretbl.med_count as pre_med_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group,
posttbl.ancestor_drug_concept_name as post_ancestor_drug_concept_name, posttbl.ancestor_drug_concept_id as post_ancestor_drug_concept_id,
posttbl.count_type as post_count_type, posttbl.med_count as post_med_count
FROM
(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id,
'pre count' as count_type,
count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as med_count
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
) pretbl
FULL JOIN
(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type,
count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as med_count
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
) posttbl
ON pretbl.person_id = posttbl.person_id AND pretbl.ancestor_drug_concept_name = posttbl.ancestor_drug_concept_name;
COMMIT TRANSACTION;
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days,
feat.post_visits_per_pt_day as op_post_visit_ratio, (IFNULL(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT
case when pre_person_id is not null then pre_person_id else post_person_id end as person_id,
case when pre_patient_group is not null then pre_patient_group else post_patient_group end as patient_group,
case when pre_ancestor_drug_concept_name is not null then pre_ancestor_drug_concept_name else post_ancestor_drug_concept_name end as ancestor_drug_concept_name,
case when pre_ancestor_drug_concept_id is not null then pre_ancestor_drug_concept_id else post_ancestor_drug_concept_id end as ancestor_drug_concept_id,
IFNULL(pre_med_count, 0) as pre_med_count,
IFNULL(post_med_count, 0) as post_med_count
FROM pre_post_med_count) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id;
"""

pre_post_med_count_clean = pd.read_gbq(med_feature_sql, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")

dx_feature_table_sql = f"""
BEGIN TRANSACTION;
create TEMP TABLE microvisits_to_macrovisits as
with initial_macrovisits_with_microvisits as
({initial_macrovisits_with_microvisits_sql}),
ip_and_er_microvisits AS
(
    SELECT DISTINCT macrovisit_id
    FROM initial_macrovisits_with_microvisits
    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 9203)
),
macrovisits_containing_at_least_one_ip_visit AS
(
    SELECT m.*
    FROM initial_macrovisits_with_microvisits m
    INNER JOIN ip_and_er_microvisits i
    ON m.macrovisit_id = i.macrovisit_id
)
SELECT v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM visit_occurrence v
LEFT JOIN macrovisits_containing_at_least_one_ip_visit m
ON v.visit_occurrence_id = m.visit_occurrence_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE Feature_Table_Builder as
with Positive_Results as (
select concept_id from concept
where concept_id IN (9191,4126681,36032716,36715206,45878745,45881802,45877985,45884084)
),
COVID_Tests as (
select concept_id from concept
where concept_id IN (586520,586523,586525,586526,586529,706157,706159,715261,715272,723470,723472,757678,36032061,36032174,36032258,36661371,586518,586524,706154,706175,723464,723467,723478,36031453,586516,706158,706160,706163,706171,706172,715260,723469,36031213,36661377,586528,706161,706165,706167,723463,723468,723471,757677,36031238,36031944,586519,706166,706169,706173,723465,723476,757685,36031506,706155,706156,706170,723466,36031652,36661370,706168,706174,715262,723477,36032419,36661378,37310257)
),
u099 as (
SELECT co.person_id, min(co.condition_start_date) as u099_idx
FROM condition_occurrence co
WHERE co.condition_concept_id IN (710706,705076) or co.condition_source_value LIKE '%U09.9%'
GROUP BY co.person_id
),
u07_any as (
SELECT co.person_id, min(co.condition_start_date) as u07_any_idx
FROM condition_occurrence co
WHERE co.condition_concept_id = 37311061
GROUP BY co.person_id
),
COVID_Lab_Index as (
SELECT m.person_id, min(m.measurement_date) as pos_test_idx
FROM measurement m JOIN COVID_Tests p on m.measurement_concept_id = p.concept_id
JOIN Positive_Results pr on m.value_as_concept_id = pr.concept_id
GROUP BY m.person_id
),
person_union as (
SELECT person_id FROM u07_any
UNION distinct
SELECT person_id FROM u099
UNION distinct
SELECT person_id FROM COVID_Lab_Index
),
All_Index_Dates as (
SELECT pu.person_id, lp.pos_test_idx, ua.u07_any_idx, u9.u099_idx
FROM person_union pu LEFT JOIN u099 u9 ON pu.person_id = u9.person_id
LEFT JOIN u07_any ua ON pu.person_id = ua.person_id
LEFT JOIN COVID_Lab_Index lp ON pu.person_id = lp.person_id
),
Collect_the_Cohort as (
SELECT distinct
    pr.person_id,
    (2021 - pr.year_of_birth) as apprx_age,
    (select concept_name from concept where concept_id=gender_concept_id) as sex,
    (select concept_name from concept where concept_id=race_concept_id) as race,
    (select concept_name from concept where concept_id=ethnicity_concept_id) as ethn,
    pr.care_site_id as site_id,
    LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE())) as min_covid_dt
FROM All_Index_Dates vc
    JOIN person pr ON vc.person_id = pr.person_id
    LEFT JOIN death d ON vc.person_id = d.person_id
WHERE
    date_diff('2022-06-22',LEAST(IFNULL(vc.pos_test_idx, CURRENT_DATE()), IFNULL(vc.u07_any_idx, CURRENT_DATE()), IFNULL(d.death_date, CURRENT_DATE())), DAY) >= 100
),
Hospitalized_Cases as (
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between DATE_SUB(b.min_covid_dt, INTERVAL 14 DAY) and DATE_ADD(b.min_covid_dt, INTERVAL 14 DAY)
UNION ALL
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisits_to_macrovisits mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061
),
hosp_and_non as (
SELECT ctc.*,
case when hc.person_id is not null
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
    LEFT JOIN Hospitalized_Cases hc ON ctc.person_id = hc.person_id
)
SELECT * FROM
(SELECT ottbl.*, IFNULL(count(distinct visit_start_date),0) as post_visits_count,
date_diff(ottbl.post_window_end_dt,ottbl.post_window_start_dt, DAY) as tot_post_days,
IFNULL(count(distinct visit_start_date),0)/300 as post_visits_per_pt_day FROM
(SELECT distinct
hc.*,
date_diff(max(mm.visit_start_date),min(mm.visit_start_date), DAY) as tot_long_data_days,
DATE_SUB(hc.min_covid_dt, INTERVAL 365 DAY) as pre_window_start_dt,
DATE_SUB(hc.min_covid_dt, INTERVAL 45 DAY) as pre_window_end_dt,
DATE_ADD(hc.min_covid_dt, INTERVAL 45 DAY) as post_window_start_dt,
case when DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY) < '2022-06-22'
then DATE_ADD(hc.min_covid_dt, INTERVAL 300 DAY)
else '2022-06-22' end as post_window_end_dt
FROM hosp_and_non hc JOIN microvisits_to_macrovisits mm ON hc.person_id = mm.person_id
LEFT JOIN All_Index_Dates lc ON hc.person_id = lc.person_id
GROUP BY
hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, lc.u099_idx
) ottbl
LEFT JOIN microvisits_to_macrovisits mmpost ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and
ottbl.post_window_end_dt and mmpost.macrovisit_id is null)
WHERE tot_long_data_days > 1
GROUP BY ottbl.person_id, ottbl.patient_group, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt,
ottbl.post_window_start_dt, ottbl.post_window_end_dt) pts
WHERE post_visits_count >=1;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE DrugConcepts as
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient'
and standard_concept = 'S';
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE tot_ip_days_calc as
SELECT person_id, post_window_start_dt, post_window_end_dt, sum(LOS) as tot_ip_days FROM
(
SELECT distinct feat.person_id, feat.post_window_start_dt, feat.post_window_end_dt, mm.macrovisit_start_date,
mm.macrovisit_end_date, (DATE_DIFF(mm.macrovisit_end_date,mm.macrovisit_start_date, DAY) + 1) as LOS
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mm ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt
) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE Drugs_for_These_Patients as
SELECT d.*
FROM drug_exposure d JOIN Feature_Table_Builder f ON d.person_id = f.person_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE drugRollUp as
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id,
ds.drug_concept_id as original_drug_concept_id, (select concept_name from concept where concept_id=ds.drug_concept_id)as original_drug_concept_name,
dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id
JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_drugs as
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date,
co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id
and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE post_drugs as
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and
co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_post_med_count as
SELECT
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.ancestor_drug_concept_name as pre_ancestor_drug_concept_name,
pretbl.ancestor_drug_concept_id as pre_ancestor_drug_concept_id, pretbl.count_type as pre_count_type, pretbl.med_count as pre_med_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group,
posttbl.ancestor_drug_concept_name as post_ancestor_drug_concept_name, posttbl.ancestor_drug_concept_id as post_ancestor_drug_concept_id,
posttbl.count_type as post_count_type, posttbl.med_count as post_med_count
FROM
(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id,
'pre count' as count_type,
count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as med_count
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
) pretbl
FULL JOIN
(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type,
count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as med_count
FROM Feature_Table_Builder feat
JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
GROUP BY
feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
) posttbl
ON pretbl.person_id = posttbl.person_id AND pretbl.ancestor_drug_concept_name = posttbl.ancestor_drug_concept_name;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_post_med_count_clean as
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days,
feat.post_visits_per_pt_day as op_post_visit_ratio, (IFNULL(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT
case when pre_person_id is not null then pre_person_id else post_person_id end as person_id,
case when pre_patient_group is not null then pre_patient_group else post_patient_group end as patient_group,
case when pre_ancestor_drug_concept_name is not null then pre_ancestor_drug_concept_name else post_ancestor_drug_concept_name end as ancestor_drug_concept_name,
case when pre_ancestor_drug_concept_id is not null then pre_ancestor_drug_concept_id else post_ancestor_drug_concept_id end as ancestor_drug_concept_id,
IFNULL(pre_med_count, 0) as pre_med_count,
IFNULL(post_med_count, 0) as post_med_count
FROM pre_post_med_count) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE tot_ip_days_calc_dx as
SELECT
    person_id, post_window_start_dt, post_window_end_dt, sum(LOS) as tot_ip_days
FROM (
SELECT distinct feat.person_id, feat.post_window_start_dt,
feat.post_window_end_dt, mm.macrovisit_start_date, mm.macrovisit_end_date,
(date_diff(mm.macrovisit_end_date,mm.macrovisit_start_date, DAY) + 1) as LOS
FROM Feature_Table_Builder feat JOIN microvisits_to_macrovisits mm ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt
) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_condition as
SELECT feat.*, (select concept_name from concept where concept_id=co.condition_concept_id) as condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN condition_occurrence co ON feat.person_id = co.person_id and
co.condition_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE post_condition as
SELECT feat.*, (select concept_name from concept where concept_id=co.condition_concept_id) as condition_concept_name, co.condition_concept_id, co.condition_start_date, co.condition_source_value, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN condition_occurrence co ON feat.person_id = co.person_id
and co.condition_start_date between feat.post_window_start_dt and feat.post_window_end_dt;
COMMIT TRANSACTION;
BEGIN TRANSACTION;
create TEMP TABLE pre_post_dx_counts as
SELECT
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.condition_concept_name as pre_condition_concept_name, pretbl.condition_concept_id as pre_condition_concept_id, pretbl.count_type as pre_count_type, pretbl.dx_count as pre_dx_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.condition_concept_name as post_condition_concept_name, posttbl.condition_concept_id as post_condition_concept_id, posttbl.count_type as post_count_type, posttbl.dx_count as post_dx_count
FROM
(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'pre count' as
count_type, count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as dx_count
FROM Feature_Table_Builder feat
    JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    JOIN pre_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
    GROUP BY
feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id
) pretbl
FULL JOIN
(SELECT feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id, 'post count' as
count_type, count(distinct case when mml.macrovisit_id is not null then mml.macrovisit_id else cast(mml.visit_occurrence_id as string) end) as dx_count
FROM Feature_Table_Builder feat
    JOIN microvisits_to_macrovisits mml ON feat.person_id = mml.person_id
    JOIN post_condition prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
    GROUP BY
feat.person_id, feat.patient_group, prc.condition_concept_name, prc.condition_concept_id) posttbl ON
pretbl.person_id = posttbl.person_id AND pretbl.condition_concept_name = posttbl.condition_concept_name;
COMMIT TRANSACTION;
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days, feat.post_visits_per_pt_day as op_post_visit_ratio,
(IFNULL(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT
case when pre_person_id is not null then pre_person_id else  post_person_id end as person_id,
case when pre_patient_group is not null then pre_patient_group else post_patient_group end as patient_group,
case when pre_condition_concept_name is not null then pre_condition_concept_name else post_condition_concept_name end as condition_concept_name,
case when pre_condition_concept_id is not null then pre_condition_concept_id else post_condition_concept_id end as condition_concept_id,
case when pre_dx_count is not null then pre_dx_count else 0 end as pre_dx_count,
case when post_dx_count is not null then post_dx_count else 0 end as post_dx_count
FROM pre_post_dx_counts
) tbl
ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id
"""

pre_post_dx_count_clean = pd.read_gbq(dx_feature_table_sql, dialect="standard", use_bqstorage_api=("BIGQUERY_STORAGE_API_ENABLED" in os.environ),  progress_bar_type="tqdm_notebook")