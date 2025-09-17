CREATE TABLE measurements_to_macrovisits AS
    
    SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM join_measurements_to_macrovists_on_date_range
        UNION
        SELECT * FROM join_measurements_to_macrovisits_on_visit_occurrence
    )
    