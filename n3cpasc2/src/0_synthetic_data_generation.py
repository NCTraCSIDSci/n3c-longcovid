import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F

def generate_window_spans(start, end, window_length = 100):
    """
    Generate a dataframe of time windows spanning from January 2020 to the current month.
    This is the only synthetic data generation function used in the model within the
    enclave, other generating functions are included for convenience and testing.

    Each window starts on the first day of a month and spans 100 days.
    The function creates a list of window start and end dates formatted as strings.

    Returns:
        pd.DataFrame: A DataFrame with columns:
            - 'window_name': Integer index of each window.
            - 'window_start': Start date of the window (YYYY-MM-DD).
            - 'window_end': End date of the window (YYYY-MM-DD).
    """
    
    num_windows = (end[0] - start[0])*12 + end[1] - start[1]
    window_length = timedelta(days=window_length)

    format = "%Y-%m-%d"
    
    window_start = []
    window_end = []
    for i_window in range(num_windows):
        start_month = (start[1] + i_window - 1) % 12 + 1
        start_year_add = (start[1] + i_window - 1) // 12
        
        start_datetime = datetime(start[0] + start_year_add, start_month, 1)
        end_datetime = start_datetime + window_length

        start_datetime = start_datetime.strftime(format)
        end_datetime = end_datetime.strftime(format)

        window_start.append(start_datetime)
        window_end.append(end_datetime)

    window_name = list(range(num_windows))
    data = {
        "window_name": window_name,
        "window_start": window_start,
        "window_end": window_end,
    }
    df = pd.DataFrame(data=data)

    return df

def generate_person_table(
    n_person=10000,
    yob_start=1930,
    yob_end=2000,
    gender_population=[8507, 8532],
    gender_weights=[0.49, 0.51],
    race_population=[8527, 8516, 8657, 8515, 0],
    race_weights=[0.68, 0.12, 0.03, 0.07, 0.10],
    ethnicity_population=[38003563, 38003564],
    ethnicity_weights=[0.1, 0.9]
):
    """
    Generates a synthetic OMOP PERSON table.

    Parameters:
        n_person (int): Number of synthetic persons to generate.
        yob_start (int): Earliest year of birth.
        yob_end (int): Latest year of birth.
        gender_population (list): Concept IDs for gender.
        gender_weights (list): Sampling weights for gender.
        race_population (list): Concept IDs for race.
        race_weights (list): Sampling weights for race.
        ethnicity_population (list): Concept IDs for ethnicity.
        ethnicity_weights (list): Sampling weights for ethnicity.

    Returns:
        pd.DataFrame: DataFrame containing synthetic PERSON records.
    """
    person_data = []
    for person_id in range(1, n_person + 1):
        # Generate birthday:
        birth_year = random.randint(yob_start, yob_end)
        birth_month = random.randint(1, 12)
        if birth_month == 2:
            month_length = 28 + 1 * (birth_year % 4 == 0)
        elif birth_month in {1, 3, 5, 7, 8, 10, 12}:
            month_length = 31
        else:
            month_length = 30
        birth_day = random.randint(1, month_length)
        birth_datetime = datetime(birth_year, birth_month, birth_day)
        # Generate demographic variables:
        gender_concept_id = random.choices(
            population=gender_population,
            weights=gender_weights,
            k=1)[0]
        race_concept_id = random.choices(
            population=race_population,
            weights=race_weights,
            k=1)[0]
        ethnicity_concept_id = random.choices(
            population=ethnicity_population,
            weights=ethnicity_weights,
            k=1)[0]
        # Append to data:
        person_data.append({
            "person_id": person_id,
            "gender_concept_id": gender_concept_id,
            "year_of_birth": birth_year,
            "month_of_birth": birth_month,
            "day_of_birth": birth_day,
            "birth_datetime": birth_datetime,
            "race_concept_id": race_concept_id,
            "ethnicity_concept_id": ethnicity_concept_id,
            "location_id": random.randint(1, 1000),
            "provider_id": random.randint(1, 100),
            "care_site_id": random.randint(1, 10),
            "person_source_value": f"PSV{person_id}",
            "gender_source_value": 'M' if gender_concept_id == 8507 else 'F',
            "race_source_value": f"Race{race_concept_id}",
            "ethnicity_source_value": f"Ethnicity{ethnicity_concept_id}"})
    return pd.DataFrame(person_data)

def generate_visit_table(
    person_df,
    max_visits_per_person=20,
    visit_start_year=2018,
    visit_end_year=2022):
    """
    Generate a synthetic OMOP VISIT_OCCURRENCE table based on a PERSON table.

    Parameters:
        person_df (pd.DataFrame): DataFrame of PERSON records.
        max_visits_per_person (int): Maximum number of visits per person.
        visit_start_year (int): Earliest year for visit start dates.
        visit_end_year (int): Latest year for visit start dates.

    Returns:
        pd.DataFrame: DataFrame containing synthetic VISIT_OCCURRENCE records.
    """
    visit_data = []
    visit_id = 1
    for _, person in person_df.iterrows():
        num_visits = random.randint(1, max_visits_per_person)
        for _ in range(num_visits):
            year = random.randint(visit_start_year, visit_end_year)
            month = random.randint(1, 12)
            if month == 2:
                month_length = 28 + 1 * (year % 4 == 0)
            elif month in {1, 3, 5, 7, 8, 10, 12}:
                month_length = 31
            else:
                month_length = 30
            day = random.randint(1, month_length)
            visit_datetime = datetime(year, month, day)

            visit_data.append({
                "visit_occurrence_id": visit_id,
                "person_id": person["person_id"],
                "visit_concept_id": random.choice([9201, 9202, 9203]),
                "visit_start_date": visit_datetime.date(),
                "visit_end_date": visit_datetime.date(), # Not dealing with multi-day visits
                "visit_type_concept_id": random.choice([44818518, 44818517]),
                "provider_id": person["provider_id"],
                "care_site_id": person["care_site_id"],
                "visit_source_value": f"Visit{visit_id}"
            })
            visit_id += 1
    return pd.DataFrame(visit_data)

def generate_observation_table(
    visit_df,
    concept_df,
    target_n_observations=3
):
    """
    Generate a synthetic OMOP OBSERVATION table using seeded concept logic.

    Parameters:
        visit_df (pd.DataFrame): DataFrame of VISIT_OCCURRENCE records.
        concept_df (pd.DataFrame): DataFrame with 'observation_concept_id', 'concept_seed_1', 'concept_seed_2'.
        target_n_observations (int): Number of observations per visit.

    Returns:
        pd.DataFrame: Synthetic OBSERVATION records.
    """
    observation_data = []
    observation_id = 1

    for _, visit in visit_df.iterrows():
        person_id = visit["person_id"]

        # Filter eligible concepts based on seeded condition
        eligible_concepts = concept_df[
            (person_id % concept_df["concept_seed_1"]) == concept_df["concept_seed_2"]
        ]

        # Sample without replacement
        sampled_concepts = eligible_concepts.sample(
            n=min(target_n_observations, len(eligible_concepts)),
            replace=False
        )

        for _, concept in sampled_concepts.iterrows():
            observation_data.append({
                "observation_id": observation_id,
                "person_id": person_id,
                "observation_concept_id": concept["observation_concept_id"],
                "observation_date": visit["visit_start_date"],
                "observation_type_concept_id": random.choice([44786627, 44786629]),
                "visit_occurrence_id": visit["visit_occurrence_id"],
                "observation_source_value": f"Obs{observation_id}"
            })
            observation_id += 1

    return pd.DataFrame(observation_data)

def generate_condition_table(
    visit_df,
    concept_df,
    target_n_conditions=5
):
    """
    Generate a synthetic OMOP CONDITION_OCCURRENCE table using seeded concept logic.

    Parameters:
        visit_df (pd.DataFrame): DataFrame of VISIT_OCCURRENCE records.
        concept_df (pd.DataFrame): DataFrame with 'condition_concept_id', 'concept_seed_1', 'concept_seed_2'.
        target_n_conditions (int): Number of conditions per visit.

    Returns:
        pd.DataFrame: Synthetic CONDITION_OCCURRENCE records.
    """
    condition_data = []
    condition_id = 1

    for _, visit in visit_df.iterrows():
        person_id = visit["person_id"]

        # Filter eligible concepts based on seeded condition
        eligible_concepts = concept_df[
            (person_id % concept_df["concept_seed_1"]) == concept_df["concept_seed_2"]
        ]

        # Sample without replacement
        sampled_concepts = eligible_concepts.sample(
            n=min(target_n_conditions, len(eligible_concepts)),
            replace=False
        )

        for _, concept in sampled_concepts.iterrows():
            condition_data.append({
                "condition_occurrence_id": condition_id,
                "person_id": person_id,
                "condition_concept_id": concept["condition_concept_id"],
                "condition_start_date": visit["visit_start_date"],
                "condition_type_concept_id": random.choice([32020, 32021]),
                "visit_occurrence_id": visit["visit_occurrence_id"],
                "condition_source_value": f"Cond{condition_id}"
            })
            condition_id += 1

    return pd.DataFrame(condition_data)

    """
    Generate a synthetic OMOP CONDITION_OCCURRENCE table based on a VISIT_OCCURRENCE table.

    Parameters:
        visit_df (pd.DataFrame): DataFrame of VISIT_OCCURRENCE records.
        max_conditions_per_visit (int): Maximum number of conditions per visit.

    Returns:
        pd.DataFrame: DataFrame containing synthetic CONDITION_OCCURRENCE records.
    """
    condition_data = []
    condition_id = 1
    for _, visit in visit_df.iterrows():
        num_conditions = random.randint(1, max_conditions_per_visit)
        for _ in range(num_conditions):
            condition_data.append({
                "condition_occurrence_id": condition_id,
                "person_id": visit["person_id"],
                "condition_concept_id": random.choice([319835, 432867, 440383]),
                "condition_start_date": visit["visit_start_date"],
                "condition_type_concept_id": random.choice([32020, 32021]),
                "visit_occurrence_id": visit["visit_occurrence_id"],
                "condition_source_value": f"Cond{condition_id}"
            })
            condition_id += 1
    return pd.DataFrame(condition_data)

def generate_fact_table(
    visit_df,
    covid_earliest_date="2020-03-01",
    long_covid_earliest_date="2020-06-01",
    seed=404,
    probabilities={
        "PCR_AG_Pos": 0.05,
        "LL_COVID_diagnosis": 0.025,
        "PAX1_NIRMATRELVIR": 0.015,
        "PAX2_RITONAVIR": 0.015,
        "PAXLOVID": 0.02,
        "REMDISIVIR": 0.01,
        "LL_Long_COVID_diagnosis": 0.03,
        "B94_8": 0.02,
        "LL_MISC": 0.005}):
    """
    Generates a synthetic COVID-related fact table from a visit-level DataFrame.

    This function adds binary indicator columns for various COVID-related diagnoses and treatments
    based on visit dates and randomized probabilities. It is useful for testing cohort logic or
    simulating patient data in the absence of real clinical records.

    Parameters:
    ----------
    visit_df : pyspark.sql.DataFrame
        Input DataFrame containing at least 'person_id' and 'visit_date' columns.

    covid_earliest_date : str, optional
        The earliest date to begin assigning COVID-related flags (default is "2020-03-01").

    long_covid_earliest_date : str, optional
        The earliest date to begin assigning long COVID-related flags (default is "2020-06-01").

    seed : int, optional
        Random seed for reproducibility (default is 404).

    probabilities : dict, optional
        Dictionary mapping each synthetic column name to its probability of being assigned a value of 1.

    Returns:
    -------
    pyspark.sql.DataFrame
        A DataFrame with the original visit data plus synthetic binary flags for COVID-related events.
        Note that the synthetic binary flags are generated independently, therefore unrealistically.
    """
    visit_fat = visit_df.select("person_id", "visit_date")

    for i, (col_name, prob) in enumerate(probabilities.items()):
        threshold_date = long_covid_earliest_date if "Long_COVID" in col_name or col_name == "B94_8" else covid_earliest_date
        visit_fat = visit_fat.withColumn(
            col_name,
            F.when(
                F.col("visit_date") >= threshold_date,
                F.expr(f"rand({seed + i}) < {prob}")
            ).cast("int")
        )

    return visit_fat

