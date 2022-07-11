CREATE TABLE join_procedures_to_macrovisits_on_date_range  AS
    
    
    -- starting with a map of microvisit to macrovisit, generate a table of
    -- distinct macrovisits 
    WITH macrovisits_only AS
    (
        SELECT DISTINCT person_id, macrovisit_id, macrovisit_start_date, macrovisit_end_date
        FROM microvisits_to_macrovisits
        WHERE macrovisit_id IS NOT NULL

    ),

    -- find procedures that lack a visit_occurrence_id but DO have a date
    p_with_date_but_not_visit_occurrence_id AS 
    (
        SELECT *
        FROM procedure_occurrence 
        WHERE visit_occurrence_id IS NULL
        AND procedure_date IS NOT NULL
    )


    -- Join on date range when we can't join on microvisit_id

    SELECT      p.procedure_occurrence_id,
                p.person_id,
                p.data_partner_id,
                p.procedure_date,
                p.visit_occurrence_id,
                v.macrovisit_id,
                v.macrovisit_start_date,
                v.macrovisit_end_date

    FROM p_with_date_but_not_visit_occurrence_id p
    INNER JOIN
    macrovisits_only v
    ON p.person_id = v.person_id
    AND p.procedure_date BETWEEN v.macrovisit_start_date AND v.macrovisit_end_date

