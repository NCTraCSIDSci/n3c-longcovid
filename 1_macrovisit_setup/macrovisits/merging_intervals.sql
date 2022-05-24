CREATE TABLE `/UNITE/LDS/macrovisits/merging_intervals` AS
    

WITH a AS
(
    SELECT  person_id,
            visit_start_date,
            -- end date of current visit
            visit_end_date,
            -- max end date we've seen so far for this person, ordered by start date
            MAX(visit_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) AS max_end_date, 
            RANK() OVER (PARTITION BY person_id ORDER by visit_start_date) AS rank_value


    FROM `/UNITE/LDS/macrovisits/inpatient_microvisits`


),


-- Put a gap anytime the current visit starts AFTER the max end date of the last group of visits
b AS
(
    SELECT  *,
            CASE 
                WHEN visit_start_date <= LAG(max_end_date) OVER (PARTITION BY person_id ORDER by visit_start_date) THEN 0
                ELSE 1
            END AS gap
    FROM a
),

-- the SUM(gaps) defines a unique number for each  macrovisit 
c AS
(
    SELECT *,
        SUM(gap) OVER (PARTITION BY person_id ORDER by visit_start_date) AS group_number
        FROM b
)

SELECT  person_id,
        group_number,
        MIN(visit_start_date) AS macrovisit_start_date,
        MAX(visit_end_date)   AS macrovisit_end_date,
        CONCAT(person_id, '_', group_number, '_', ABS(HASH(MIN(visit_start_date)))) AS macrovisit_id

FROM c
GROUP BY person_id, group_number
ORDER BY person_id, group_number

    

