CREATE TABLE procedures_to_macrovisits AS
    
    SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM join_procedures_to_macrovisits_on_date_range
        UNION
        SELECT * FROM join_procedures_to_macrovisits_on_visit_occurrence
    )
   