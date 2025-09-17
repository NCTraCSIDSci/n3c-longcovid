CREATE TABLE microvisits_to_macrovisits
    
    -- This final cleanup step removes macrocvisits that consist ONLY of NON-IP visits
    -- We firstbuild a list of all macrovisits that have at least one IP or ER visit
    -- Then keep all data on those macrovisits, and discard the rest


-- Find the distinct macrovisit_ids that contain at least one IP (micro)visit
WITH ip_and_er_microvisits AS
(
    SELECT DISTINCT macrovisit_id
    FROM initial_macrovisits_with_microvisits
    WHERE visit_concept_id IN (9201, 8717, 262, 32037, 581379, 9203)
),

-- Filter the macrovisit table to only those macrovisits that contain at least one IP visit
macrovisits_containing_at_least_one_ip_visit AS
(
    SELECT m.* 
    FROM initial_macrovisits_with_microvisits m
    INNER JOIN ip_and_er_microvisits i
    ON m.macrovisit_id = i.macrovisit_id
)

-- Build a copy of the visit table with macrovisit_id added
-- leaving macrovisit_id null if the initial macrovisit contained no IP visits of any type
SELECT v.*,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM visit_occurrence v
LEFT JOIN macrovisits_containing_at_least_one_ip_visit m
ON v.visit_occurrence_id = m.visit_occurrence_id





    
