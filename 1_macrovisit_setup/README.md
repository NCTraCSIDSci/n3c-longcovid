## Purpose of this code
OMOP and other common data models do not enforce a definition for a "visit." In order to make sure our code is reproducible, we need to ensure that you define a visit in the same way that we do. This code will not change anything in your existing VISIT_OCCURRENCE table, but rather will create a new table to contain "macrovisits," or visits defined using the N3C definition. 

Note that there is a manuscript in progress describing the macrovisit method and design. Once this is available, we'll add a link here for more detailed information.

## How to run this code

1. Run inpatient_macrovists.sql.
	This dataset has one input:
		the OMOP visit occurrence domain table.

2. Run merging_intervals.sql.
	This dataset has one input:
		the output of inpatient_macrovisits.sql.

3. Run initial_macrovisits_with_microvisits.sql.
	This dataset has two inputs:
		the output of merging_intervals.sql
		the OMOP visit occurrence domain table.

4. Run microvisits_to_macrovisits.sql.
	This dataset has two inputs:
		the output of initial_macrovisits_with_microvisits.sql
		the OMOP visit occurrence domain table.
