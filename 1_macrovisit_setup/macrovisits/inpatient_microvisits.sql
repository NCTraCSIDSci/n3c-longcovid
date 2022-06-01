-- Selects any inpatient (IP) visit by the codes below, 
-- Any OP visit that is exactly 1 day
-- Any ER visit that is >= 1 day 
--  These are assumed to be IP visits.
-- These visits will be assembled into the scaffold of the macrovisit.

    -- 262	Emergency Room and Inpatient Visit	
    -- 8717	Inpatient Hospital
    -- 9201	Inpatient Visit		
    -- 32037	Intensive Care	
    -- 581379	Inpatient Critical Care Facility	

    -- inpatient visit, inpatient hospital, emergency room and intensive care

CREATE TABLE inpatient_microvisits AS
    

    SELECT *     

    FROM visit_occurrence

    WHERE  
    
    -- we only use visits with defined start and end dates in the scaffold
    -- visits with a start date but lacking an end date can still be joined in with the OTHER visits
    -- at the end

    (visit_start_date IS NOT NULL AND visit_end_date IS NOT NULL)

    -- remove visits that end before they start
    AND visit_start_date <= visit_end_date
    
    AND

    -- Visit Types that are allowed to be part of the macrovisit scaffold
    (
        -- Any inpatient visit
        visit_concept_id in (9201, 8717, 262, 32037, 581379)
        
        OR

        -- A one day Outpatient visit 
        -- It was decided that a 1-day OP visit could be an Overnight procedure
        -- But the very long OP visits are likely an administrative concept and don't reflect actual hospital stays
        (visit_concept_id = 9202 AND DATEDIFF(visit_end_date,visit_start_date) = 1)

        OR

        -- ER visit >= 1 day
        (visit_concept_id = 9203 AND DATEDIFF(visit_end_date,visit_start_date) >= 1)


    )


