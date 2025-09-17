/* 
Shareable N3C PASC Phenotype code
V1.0, last updated May 2022 by Emily Pfaff

PLEASE READ THE NOTES BELOW AND THE COMMENTS ABOVE EACH CTE BEFORE EXECUTING

Assumptions:
-- You have built out a "macrovisit" table. (Code to create this table is included in this GitHub repository.) In this script, the macrovisit table is called "microvisit_to_macrovisit_lds". If you use a different name, you will need to CTRL+F for this string and replace with your table name.
-- You are receiving data from multiple sites, and (1) have some mechanism to identify which rows of data come from which site, and (2) have a table that stores the most recent date that each site's data was updated. We call this table our "manifest" table; if you use a different table name, or column names, you may need to make some edits. I have marked all instances where this our site ID variable or the manifest table appears with "-- SITE_INFO". You can find all instances in the code if you CTRL+F for that string.

Notes:
-- This is written in Spark SQL, which is mostly similar to other SQL dialects, but certainly differs in syntax for particular functions. One that I already know will be an issue is the date_add function, and most likely the datediff function. You will need to CTRL+F for each problematic function and replace each one with the equivalent from your RDBNMS.
-- For efficiency, we have pre-joined the OMOP CONCEPT table to all of the core OMOP tables using all possible foreign keys. Thus, you will see that we, e.g., select "race_concept_name" directly from the PERSON table, when that would usually require a join to CONCEPT on race_concept_id to get the name. If you do not wish to pre-join in your own repository, you will need to add in one or more JOINs to one or more aliases of the CONCEPT table in all areas where this occurs. 
-- I have written this script as a series of CTEs. This is likely not performant--it will almost certainly be better to stop at certain points in this flow and create materialized tables so you don't have to hold everything in memory. I have not done this ahead of time because every RDBMS handles this differently. 
*/

with
-- concept set for "positive" test result concepts. If your data uses other variations, add those concepts here.
Positive_Results as (
select concept_id	
from concept
where concept_id IN (9191,4126681,36032716,36715206,45878745,45881802,45877985,45884084)
),

-- concept set for PCR and antigen tests for COVID. Does not include antibody or ambiguous tests.
COVID_Tests as (
select concept_id	
from concept
where concept_id IN (586520,586523,586525,586526,586529,706157,706159,715261,715272,723470,723472,757678,36032061,36032174,36032258,36661371,586518,586524,706154,706175,723464,723467,723478,36031453,586516,706158,706160,706163,706171,706172,715260,723469,36031213,36661377,586528,706161,706165,706167,723463,723468,723471,757677,36031238,36031944,586519,706166,706169,706173,723465,723476,757685,36031506,706155,706156,706170,723466,36031652,36661370,706168,706174,715262,723477,36032419,36661378,37310257)
),

-- building block 1 for base population table; finds all patients with a U09.9 ICD-10 code, or OMOP concept_id equivalent, as well as the earliest date that code occurs
u099 as (
SELECT co.person_id, min(co.condition_start_date) as u099_idx
FROM condition_occurrence co 
WHERE co.condition_concept_id IN (710706,705076) or co.condition_source_value LIKE '%U09.9%'
GROUP BY co.person_id
),

-- building block 2 for base population table; finds all patients with a U07.1 ICD-10 code, or OMOP concept_id equivalent, as well as the earliest date that code occurs
u07_any as (
SELECT co.person_id, min(co.condition_start_date) as u07_any_idx
FROM condition_occurrence co 
WHERE co.condition_concept_id = 37311061 
GROUP BY co.person_id
),

-- building block 3 for base population table; finds all patients with a positive PCR or antigen test, as well as the earliest date that code occurs
COVID_Lab_Index as (
SELECT m.person_id, min(m.measurement_date) as pos_test_idx
FROM measurement m JOIN COVID_Tests p on m.measurement_concept_id = p.concept_id
    JOIN Positive_Results pr on m.value_as_concept_id = pr.concept_id
GROUP BY m.person_id
),

-- union set of all building blocks
person_union as (
SELECT person_id
FROM u07_any

UNION

SELECT person_id
FROM u099

UNION

SELECT person_id
FROM COVID_Lab_Index
),

-- brings back in index date(s) for each of the building block criteria, one row per patient
-- note that we don't use the U09.9 index column in the remainder of this script, but it does become a handy way to identify your confirmed long-COVID patients for re-training or validation
-- HINT: This would be a good place to stop and materialize a table
All_Index_Dates as (
SELECT pu.person_id, lp.pos_test_idx, ua.u07_any_idx, u9.u099_idx, mc.m3581_idx
FROM person_union pu LEFT JOIN u099 u9 ON pu.person_id = u9.person_id
    LEFT JOIN u07_any ua ON pu.person_id = ua.person_id
    LEFT JOIN COVID_Lab_Index lp ON pu.person_id = lp.person_id
),

-- determine earliest index date, filter base population on amount of post-covid data available, add demographic variables
-- this is a query you will need to adjust, join-wise, if you have not pre-joined the concept table to your person table.
Collect_the_Cohort as (
SELECT distinct 
    pr.person_id, 
    (year(msh.run_date) - pr.year_of_birth) as apprx_age,	-- SITE_INFO: Uses most recent site data update date as anchor for age calculation
    pr.gender_concept_name as sex, 
    pr.race_concept_name as race, 
    pr.ethnicity_concept_name as ethn, 
    pr.data_partner_id as site_id,                              -- SITE_INFO: Pulls in site ID for each patient row; useful for troubleshooting later
    array_min(array(vc.pos_test_idx, vc.u07_any_idx)) as min_covid_dt
FROM All_Index_Dates vc 
    JOIN person pr ON vc.person_id = pr.person_id
    JOIN manifest msh ON pr.data_partner_id = msh.data_partner_id 	-- SITE_INFO: Joining to the manifest table here
    LEFT JOIN death d ON vc.person_id = d.person_id
WHERE
    datediff(msh.run_date,array_min(array(vc.pos_test_idx, vc.u07_any_idx, d.death_date))) >= 100	-- SITE_INFO: Using the most recent site data update date to determine number of longitudinal patient days
),

-- flag patients if they were hospitalized with an acute covid infection. note that this may include an acute covid infection other than their index infection. 
Hospitalized_Cases as (
-- hospitalization close to index date
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisit_to_macrovisit_lds mac ON b.person_id = mac.person_id
WHERE
    mac.macrovisit_start_date between date_add(b.min_covid_dt, -14) and date_add(b.min_covid_dt, 14)

UNION

-- hospitalization with a U07.1
SELECT b.person_id, b.apprx_age, b.sex, b.race, b.ethn, site_id, min_covid_dt
FROM Collect_the_Cohort b JOIN microvisit_to_macrovisit_lds mac ON b.person_id = mac.person_id
    JOIN condition_occurrence cond ON mac.visit_occurrence_id = cond.visit_occurrence_id
WHERE
    mac.macrovisit_id is not null
    and condition_concept_id = 37311061 
),

-- add flag to show whether patients were in the hospitalized group or the non-hospitalized group; this becomes a model feature
hosp_and_non as (
SELECT ctc.*,
case when hc.person_id is not null 
    then 'CASE_HOSP' else 'CASE_NONHOSP' end as patient_group
FROM Collect_the_Cohort ctc
    LEFT JOIN Hospitalized_Cases hc ON ctc.person_id = hc.person_id
)

-- calculate each patient's observation windows and outpatient visit ratio; filter to ensure each patient has at least 1 visit in their post window
-- STOP: The results of the query below need to be stored in a table called "Feature_Table_Builder". Once you have this table created, move to the next script in the sequence.

SELECT * FROM 
	(
	SELECT ottbl.*, nvl(count(distinct visit_start_date),0) as post_visits_count, datediff(ottbl.post_window_end_dt,ottbl.post_window_start_dt) as tot_post_days, nvl(count(distinct visit_start_date),0)/300 as 		
	post_visits_per_pt_day
	FROM 
		(
		SELECT distinct 
		hc.*, 
		datediff(max(mm.visit_start_date),min(mm.visit_start_date)) as tot_long_data_days, 
		date_add(hc.min_covid_dt,-365) as pre_window_start_dt, 
		date_add(hc.min_covid_dt, -45) as pre_window_end_dt, 
		date_add(hc.min_covid_dt, 45) as post_window_start_dt, 
		case when date_add(hc.min_covid_dt, 300) < msh.run_date 
			then date_add(hc.min_covid_dt, 300) 
			else msh.run_date end as post_window_end_dt 			-- SITE_INFO: Using the most recent site data update date to determine number of longitudinal patient days
		FROM hosp_and_non hc JOIN manifest msh ON hc.site_id = msh.data_partner_id            -- SITE_INFO: Pulls in site ID for each patient row; useful for troubleshooting later
    			JOIN microvisit_to_macrovisit_lds mm ON hc.person_id = mm.person_id
    			LEFT JOIN All_Index_Dates lc ON hc.person_id = lc.person_id
		GROUP BY
		hc.person_id, hc.patient_group, hc.apprx_age, hc.sex, hc.race, hc.ethn, hc.site_id, hc.min_covid_dt, msh.run_date, lc.u099_idx
		) ottbl
	LEFT JOIN microvisit_to_macrovisit_lds mmpost ON (ottbl.person_id = mmpost.person_id and mmpost.visit_start_date between ottbl.post_window_start_dt and 
        	ottbl.post_window_end_dt and mmpost.macrovisit_id is null)
	WHERE tot_long_data_days > 1 
	GROUP BY ottbl.person_id, ottbl.patient_group, ottbl.apprx_age, ottbl.sex, ottbl.race, ottbl.ethn, ottbl.site_id, ottbl.min_covid_dt, ottbl.tot_long_data_days, ottbl.pre_window_start_dt, ottbl.pre_window_end_dt, 	
	ottbl.post_window_start_dt, ottbl.post_window_end_dt
	) pts
WHERE post_visits_count >=1



























