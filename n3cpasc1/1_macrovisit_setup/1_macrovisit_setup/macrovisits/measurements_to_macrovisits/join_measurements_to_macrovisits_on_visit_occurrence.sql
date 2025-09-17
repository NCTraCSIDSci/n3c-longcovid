CREATE TABLE join_measurements_to_macrovisits_on_visit_occurrence  AS
    

    -- join all conditions that directly match a microvisit that is part of a macrovisit
    SELECT      me.measurement_id,
                me.person_id,
                me.data_partner_id,
                me.measurement_date,
                me.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM  measurement me
    INNER JOIN microvisits_to_macrovisits v
    ON me.visit_occurrence_id = v.visit_occurrence_id


