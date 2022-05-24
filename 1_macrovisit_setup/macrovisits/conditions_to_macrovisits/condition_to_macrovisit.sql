CREATE TABLE `/UNITE/LDS/harmonized/conditions_to_macrovisit` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_4, EXECUTOR_MEMORY_MEDIUM') AS
  
   SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM `/UNITE/LDS/macrovisits/conditions_to_macrovisits/join_conditions_to_macrovisits_on_date_range`
        UNION
        SELECT * FROM `/UNITE/LDS/macrovisits/conditions_to_macrovisits/join_conditions_to_macrovisits_on_visit_occurrene`
    )
    
    
     