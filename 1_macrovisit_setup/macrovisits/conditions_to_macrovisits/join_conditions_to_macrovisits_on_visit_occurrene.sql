CREATE TABLE join_conditions_to_macrovisits_on_visit_occurrene AS
    
  
    -- join all conditions that directly match a microvisit that is part of a macrovisit
    SELECT      c.condition_occurrence_id,
                c.person_id,
                c.data_partner_id,
                c.condition_start_date,
                c.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  condition_occurrence c
    INNER JOIN microvisits_to_macrovisits v
    ON c.visit_occurrence_id = v.visit_occurrence_id
