CREATE TABLE conditions_to_macrovisit AS
  
   SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM join_conditions_to_macrovisits_on_date_range
        UNION
        SELECT * FROM join_conditions_to_macrovisits_on_visit_occurrene
    )
    
    
     