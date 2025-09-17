CREATE TABLE `/UNITE/LDS/macrovisits/procedures_to_macrovisits/join_procedures_to_macrovisits_on_visit_occurrence` AS


    -- join all procedures that directly match a microvisit that is part of a macrovisit
    SELECT      p.procedure_occurrence_id,
                p.person_id,
                p.data_partner_id,
                p.procedure_date,
                p.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  `/UNITE/LDS/harmonized/procedure_occurrence` p
    INNER JOIN `/UNITE/LDS/harmonized/microvisits_to_macrovisits` v
    ON p.visit_occurrence_id = v.visit_occurrence_id


