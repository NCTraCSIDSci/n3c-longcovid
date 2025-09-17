/* 
Shareable N3C PASC Phenotype code
V1.0, last updated May 2022 by Emily Pfaff

PLEASE READ THE COMMENTS ABOVE EACH CTE BEFORE EXECUTING
*/

with
-- this is here for performance reasons--it's just a copy of the CONCEPT table subset to standard RxNorm-coded drugs at the ingredient level.
DrugConcepts as (
SELECT *
FROM concept
where domain_id = 'Drug' and lower(vocabulary_id) = 'rxnorm' and concept_class_id = 'Ingredient' and standard_concept = 'S'
),

-- find the total number of inpatient days in the post period for use later
tot_ip_days_calc as (
SELECT 
    person_id, post_window_start_dt, post_window_end_dt, sum(LOS) as tot_ip_days
FROM 	(
	SELECT distinct feat.person_id, 
	feat.post_window_start_dt,
	feat.post_window_end_dt,
	mm.macrovisit_start_date, 
	mm.macrovisit_end_date, 
	(datediff(mm.macrovisit_end_date,mm.macrovisit_start_date) + 1) as LOS
	FROM Feature_Table_Builder feat JOIN microvisit_to_macrovisit_lds mm ON feat.person_id = mm.person_id and mm.macrovisit_start_date between feat.post_window_start_dt and feat.post_window_end_dt
	) tbl
GROUP BY person_id, post_window_start_dt, post_window_end_dt
),

-- another performance assist; this subsets the giant drug_exposure table just to those drugs that are associated with a patient in our cohort
Drugs_for_These_Patients as (
SELECT d.*
FROM drug_exposure d JOIN Feature_Table_Builder f ON d.person_id = f.person_id
),

-- roll up all drugs to RxNorm ingredient level for consistency
drugRollUp as (
SELECT distinct ds.person_id, ds.drug_exposure_start_date, ds.visit_occurrence_id, ds.drug_concept_id as original_drug_concept_id, ds.drug_concept_name as original_drug_concept_name, dc.concept_id as ancestor_drug_concept_id, dc.concept_name as ancestor_drug_concept_name
-- sing only the portion of concept_ancestor where the ancestors are rxnorm ingredients and are standard concepts.
FROM DrugConcepts dc JOIN concept_ancestor ca ON dc.concept_id = ca.ancestor_concept_id -- the ingredients are the ancestors
-- if a med for one of our patients is a descendent of one of those ingredients
    JOIN Drugs_for_These_Patients ds ON ds.drug_concept_id = ca.descendant_concept_id -- the original meds are the descendents 
),

-- pull all the drugs associated with the patient in their pre window
pre_drugs as (
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.pre_window_start_dt and feat.pre_window_end_dt
),

-- pull all the drugs associated with the patient in their post window
post_drugs as (
SELECT feat.*, co.ancestor_drug_concept_name, co.ancestor_drug_concept_id, co.drug_exposure_start_date, co.visit_occurrence_id
FROM Feature_Table_Builder feat JOIN drugRollUp co ON feat.person_id = co.person_id and co.drug_exposure_start_date between feat.post_window_start_dt and feat.post_window_end_dt
),

-- do a full outer join between pre and post drugs so as to compare
pre_post_med_count (
SELECT 
pretbl.person_id as pre_person_id, pretbl.patient_group as pre_patient_group, pretbl.ancestor_drug_concept_name as pre_ancestor_drug_concept_name, pretbl.ancestor_drug_concept_id as pre_ancestor_drug_concept_id, pretbl.count_type as pre_count_type, pretbl.med_count as pre_med_count,
posttbl.person_id as post_person_id, posttbl.patient_group as post_patient_group, posttbl.ancestor_drug_concept_name as post_ancestor_drug_concept_name, posttbl.ancestor_drug_concept_id as post_ancestor_drug_concept_id, posttbl.count_type as post_count_type, posttbl.med_count as post_med_count
FROM 
	(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'pre count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_Table_Builder feat 
    		JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    		JOIN pre_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
	) pretbl

	FULL JOIN

	(SELECT feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id, 'post count' as count_type, count(distinct nvl(mml.macrovisit_id, mml.visit_occurrence_id)) as med_count
	FROM Feature_Table_Builder feat 
    		JOIN microvisit_to_macrovisit_lds mml ON feat.person_id = mml.person_id
    		JOIN post_drugs prc ON mml.visit_occurrence_id = prc.visit_occurrence_id
	GROUP BY 
	feat.person_id, feat.patient_group, prc.ancestor_drug_concept_name, prc.ancestor_drug_concept_id
	) posttbl 
	ON pretbl.person_id = posttbl.person_id AND pretbl.ancestor_drug_concept_name = posttbl.ancestor_drug_concept_name
)

-- clean up the full outer join for meds. this table will be handed off as is to the next part of the pipeline
-- STOP: The results of the query below need to be stored in a table called "pre_post_med_count_clean". Once you have this table created, move to the next script in the sequence.
select distinct tbl.*, feat.apprx_age, feat.sex, feat.race, feat.ethn, feat.tot_long_data_days, feat.post_visits_per_pt_day as op_post_visit_ratio, (nvl(tot.tot_ip_days,0)/feat.tot_post_days) as ip_post_visit_ratio
FROM
Feature_Table_Builder feat JOIN
(SELECT 
nvl(pre_person_id, post_person_id) as person_id, nvl(pre_patient_group, post_patient_group) as patient_group, nvl(pre_ancestor_drug_concept_name, post_ancestor_drug_concept_name) as ancestor_drug_concept_name, nvl(pre_ancestor_drug_concept_id, post_ancestor_drug_concept_id) as ancestor_drug_concept_id, nvl(pre_med_count, 0) as pre_med_count, nvl(post_med_count, 0) as post_med_count
FROM pre_post_med_count) tbl ON feat.person_id = tbl.person_id
LEFT JOIN tot_ip_days_calc tot ON feat.person_id = tot.person_id





























