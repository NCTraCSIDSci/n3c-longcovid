CREATE TABLE `/UNITE/LDS/macrovisits/conditions_to_macrovisits/join_conditions_to_macrovisits_on_visit_occurrene` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_MEDIUM, NUM_EXECUTORS_8') AS
    
  
    -- join all conditions that directly match a microvisit that is part of a macrovisit
    SELECT      c.condition_occurrence_id,
                c.person_id,
                c.data_partner_id,
                c.condition_start_date,
                c.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  `/UNITE/LDS/harmonized/condition_occurrence` c
    INNER JOIN `/UNITE/LDS/harmonized/microvisits_to_macrovisits` v
    ON c.visit_occurrence_id = v.visit_occurrence_id
