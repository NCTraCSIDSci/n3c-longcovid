from pyspark.sql import functions as F
from xgboost.sklearn import XGBClassifier

def training_set(basic_cohort, 
                 labels, 
                 window_spans):
    """
    Process the basic cohort and labels to create a training set with balanced classes.
    For each person in the basic cohort, assigns a label (True/False) for each window.
    """
    # Create windowed labels:
    outer_labels = labels \
        .select('person_id','label','label_date') \
        .join(window_spans,how='outer') \
        .filter(F.col('label_date').between(F.col('window_start'), F.col('window_end')))
    windowed_labels = outer_labels \
        .groupby('person_id') \
        .agg(F.max('window_name') \
        .alias('window_name')) \
        .join(outer_labels, on=['person_id','window_name'])
    # Add labels by window to basic cohort:
    df0 = basic_cohort
    df0 = df0.join(windowed_labels, on='person_id',how='left_anti')
    
    neg_count = df0.count()
    pos_count = windowed_labels.count()

    df0 = df0.orderBy(df0.person_id)\
        .sample(False, pos_count / neg_count, seed=42)
    df0 = df0.withColumn("label", F.lit(False))

    df1 = basic_cohort
    df1 = df1.withColumnRenamed('person_id','person_id_copy')
    df1 = df1.join(windowed_labels, (df1['person_id_copy']==windowed_labels['person_id']) & (df1['window'] == windowed_labels['window_name']))
    df1 = df1[['person_id','sex','window','window_age']]
    df1 = df1.withColumn("label", F.lit(True))

    df = df0.union(df1)

    return df

def feature_downselect(training_set, 
                       selected_features, 
                       condition_features_alt,
                       threshold = 0.0025):
    """
    Downselect features based on their covariance with the label in the training set.
    The threshold parameter controls the sensitivity of the downselection, and has a big impact on the number of features selected.
    Returns a DataFrame of selected features with their covariance values.
    """
    # Select the training features by window and person
    ts = training_set[['person_id','window','label']]
    training_features = condition_features_alt.join(ts, on=['person_id','window'])

    # Add counts and calculate covariances
    num_obs = training_set.count()
    h1 = training_set.filter(training_set.label)
    num_h1 = h1.count()

    # df = selected_features.groupBy('ancestor_concept_id').count()
    df = training_features #.filter(training_features['greater_in_post'])
    dfm = df.groupBy('concept_id').count()
    dfm = dfm.withColumn('mean',dfm['count'] / num_obs)
    dfm = dfm.drop('count')

    df = df.join(dfm, on='concept_id')
    df = df.withColumn("label", df.label.cast("float"))

    label_ev = num_h1/num_obs
    df = df.withColumn('value', (1 - df['mean']) * (df['label'] - label_ev) / num_obs)
    df_cov = df.groupBy('concept_id').agg(F.sum('value').alias('label_covariance'))

    # put back concept names
    sf = selected_features.groupBy(['ancestor_concept_id','anc_concept_name']).count()
    df_cov = df_cov.join(sf, df_cov['concept_id']==sf['ancestor_concept_id'])
    df_cov = df_cov.withColumnRenamed('anc_concept_name','concept_name')
    df_cov = df_cov[['concept_id','label_covariance','concept_name']]

    # df_cov at this point is a potentially useful diagnostic table. Print to file or inspect 

    #for condition in drop_conditions:
    #    df = df.filter(df['concept_id'] != condition)

    df = df_cov.filter(df_cov['label_covariance'] > threshold)

    return df

def train_data_full(training_set, 
                    post_visit_counts_in_windows, 
                    feature_downselect, 
                    condition_features_alt,
                    rand_seed = 1234,
                    validation_share = 0.05):
    """
    Applies the downselection to the training features, and combines with demographic and visit count data.
    Splits the data into training and validation sets based on the validation_share parameter.
    Returns a DataFrame ready for model training.
    """
    # Select the training features by window and person
    ts = training_set[['person_id','window','label']]
    training_features = condition_features_alt.join(ts, on=['person_id','window'])

    # Downselect to only include salient features
    training_data_downselect = training_features.join(feature_downselect[['concept_id']], on='concept_id')

    df = training_data_downselect.groupBy(['person_id','window','label']).pivot('concept_id').count()

    df = training_set.join(df, on=['person_id','window','label'], how='left')
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
    
    df = df.orderBy(df.person_id)\
        .withColumn('hold_for_val',F.rand(seed=rand_seed) < validation_share)
    return df
    
def pasc_train_model(model_train_data,
                     colsample_bytree=0.1,
                     gamma=0.4,
                     learning_rate=0.09, 
                     max_depth=12, 
                     min_child_weight=0,
                     n_estimators=400, 
                     subsample=0.9, 
                     random_state=42):
    """
    Train an XGBoost model on the provided training data.
    This is not much more than a shell around the XGBoost training function.
    """
    df = model_train_data.toPandas()
    y = (df["label"].to_numpy())
    X = df.drop(columns = ["label", "person_id", "window", "hold_for_val"])
    X = X.to_numpy()
    model =  XGBClassifier(colsample_bytree=colsample_bytree,
                           gamma=gamma, 
                           learning_rate=learning_rate,
                           max_depth=max_depth,
                           min_child_weight=min_child_weight,
                           n_estimators=n_estimators, 
                           subsample=subsample,
                           random_state=random_state)
    model.fit(X, y)
    return model
