CREATE TABLE `/UNITE/LDS/macrovisits/measurements_to_macrovisits/join_measurements_to_macrovisits_on_visit_occurrence` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_SMALL, NUM_EXECUTORS_16') AS
    

    -- join all conditions that directly match a microvisit that is part of a macrovisit
    SELECT      me.measurement_id,
                me.person_id,
                me.data_partner_id,
                me.measurement_date,
                me.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  `/UNITE/LDS/harmonized/measurement` me
    INNER JOIN `/UNITE/LDS/harmonized/microvisits_to_macrovisits` v
    ON me.visit_occurrence_id = v.visit_occurrence_id


