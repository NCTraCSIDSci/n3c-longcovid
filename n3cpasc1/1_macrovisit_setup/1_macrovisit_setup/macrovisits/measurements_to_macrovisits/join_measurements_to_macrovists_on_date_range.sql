CREATE TABLE join_measurements_to_macrovists_on_date_range AS

    
    -- starting with a map of microvisit to macrovisit, generate a table of
    -- distinct macrovisits 
    WITH macrovisits_only AS
    (
        SELECT DISTINCT person_id, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM microvisits_to_macrovisits
        WHERE macrovisit_id IS NOT NULL

    ),

    -- find measurements that lack a visit_occurrence_id but DO have a date
    me_with_date_but_not_visit_occurrence_id AS 
    (
        SELECT *
        FROM measurement  
        WHERE visit_occurrence_id IS NULL
        AND measurement_date IS NOT NULL
    )


    -- Join on date range when we can't join on microvisit_id

    SELECT      me.measurement_id,
                me.person_id,
                me.data_partner_id,
                me.measurement_date,
                me.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM me_with_date_but_not_visit_occurrence_id me
    INNER JOIN
    macrovisits_only v
    ON me.person_id = v.person_id
    AND me.measurement_date BETWEEN v.macrovisit_start_date AND v.macrovisit_end_date

