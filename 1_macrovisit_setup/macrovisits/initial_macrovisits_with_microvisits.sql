CREATE TABLE `/UNITE/LDS/macrovisits/initial_macrovisits_with_microvisits` AS
    
SELECT  v.person_id,
        v.data_partner_id,
        v.visit_occurrence_id,
        v.visit_concept_id,
        m.macrovisit_id,
        m.macrovisit_start_date,
        m.macrovisit_end_date
FROM `/UNITE/LDS/harmonized/visit_occurrence` v
LEFT JOIN `/UNITE/LDS/macrovisits/merging_intervals` m
ON v.person_id = m.person_id
AND v.visit_start_date >= m.macrovisit_start_date
AND v.visit_start_date <= m.macrovisit_end_date 
