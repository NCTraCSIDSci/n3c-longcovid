CREATE TABLE `/UNITE/LDS/harmonized/measurements_to_macrovisits` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_SMALL, NUM_EXECUTORS_8') AS
    
    SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM `/UNITE/LDS/macrovisits/measurements_to_macrovisits/join_measurements_to_macrovists_on_date_range`
        UNION
        SELECT * FROM `/UNITE/LDS/macrovisits/measurements_to_macrovisits/join_measurements_to_macrovisits_on_visit_occurrence`
    )
    