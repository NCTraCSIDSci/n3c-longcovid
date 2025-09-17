import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def process_data_full(basic_cohort, 
                      post_visit_counts_in_windows, 
                      feature_downselect, 
                      condition_features_alt):
    downselected_features = condition_features_alt.join(feature_downselect[['concept_id']], on='concept_id', how = 'right')
    df = downselected_features
    df = df.groupBy(['person_id','window']).pivot('concept_id').count()

    df = basic_cohort.join(df, on=['person_id','window'], how='left')
    df = df.na.fill(value=0)

    ppv = post_visit_counts_in_windows[['person_id','window','post_nonhosp_visit_count','post_hosp_count']]
    df = df.join(ppv, on=['person_id','window'])

    df = df.withColumn("is_female", F.lower(df.sex).contains('female').cast("long"))
    df = df.drop('sex')

    df = df.withColumn("age_bin", F.floor(df['window_age'] / 10))
    df = df.drop('window_age')

    df = df.withColumn("op_visit_bin", (df["post_nonhosp_visit_count"] >= 2).cast("long") +
                                        (df["post_nonhosp_visit_count"] >= 10).cast("long"))

    df = df.withColumn("ip_visit_bin", (df["post_hosp_count"] >= 2).cast("long") +
                                        (df["post_hosp_count"] >= 10).cast("long"))

    df = df.drop('post_nonhosp_visit_count')
    df = df.drop('post_hosp_count')

    return df

def model_scores(input_data, pasc_trained_model):
    df = input_data
    model = pasc_trained_model
    # model = pasc_trained_model.stages[0].model
    def run_model(row):
        arr = np.array([x for x in row])[None,:]
        y_pred =  model.predict_proba(arr)
        value = float(y_pred.squeeze()[1]) # prob of class=1
        return value
    model_udf = F.udf(run_model, FloatType())
    X = df
    X = X.withColumn("model_score",
                    model_udf(
                        F.struct([df[x] for x in X.columns if x not in ['person_id','window','label']])
                        )
                    )
    new_df = X[['person_id','window','model_score']]
    return new_df

