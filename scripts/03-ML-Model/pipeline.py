from foundry_ml import Model, Stage
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.18e0073c-afb6-488b-9cec-e84d4d11d4c0"),
    add_label=Input(rid="ri.foundry.main.dataset.3ac52a88-c634-4472-9f45-a0e65f783978"),
    encode_dummy_variables=Input(rid="ri.foundry.main.dataset.41a4f27e-7595-496e-a2f2-467cf7de41e6"),
    pivot_drugs=Input(rid="ri.foundry.main.dataset.4c45e96e-c27a-42ad-abf6-c03545083c1d")
)
def add_demographic_dummies(add_label, encode_dummy_variables, pivot_drugs):
    a = add_label
    d = encode_dummy_variables
    p = pivot_drugs

    df = a.join(d, on="person_id",  how="left")
    df = df.join(p, on='person_id', how='left')

    # Some patients in the condition data aren't in the drug dataset
    # Presumably they don't have any drugs in the relevant period?
    df = df.fillna(0)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.013af07a-4b21-4df6-9272-01b39ab728b8"),
    union_data=Input(rid="ri.foundry.main.dataset.98b5ce44-c2ef-412d-8ffe-8fe8cd55d81f")
)
def drop_features(union_data):
    
    df = union_data
    #df = df.drop("case_apprx_age")
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41a4f27e-7595-496e-a2f2-467cf7de41e6"),
    demographic_and_history_features=Input(rid="ri.foundry.main.dataset.ccf935e2-8e36-48ea-ba14-06315a30bbbb")
)
def encode_dummy_variables(demographic_and_history_features):
    
    dhf = demographic_and_history_features.toPandas()

    

    # encode these caterogical variables as binaries
    df = pd.get_dummies(dhf, columns=["case_sex","case_race","case_ethn","case_diab_ind","case_kid_ind", "case_CHF_ind", "case_chronicPulm_ind"])

    #df["approx_age"] = dhf["case_apprx_age"] 

    #r = dum_df.rename(str.lower, axis='columns')

    df = df.rename(columns = lambda c: str.lower(c.replace(" ", "_")))

    return spark.createDataFrame(df)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9bb492f8-18e9-4b40-a2f6-3adc128ca603"),
    random_forest_trained=Input(rid="ri.foundry.main.dataset.4acae42e-111a-4024-8b12-d7e560f61183"),
    training_data=Input(rid="ri.foundry.main.dataset.fb1fd621-c97c-4e66-a2f5-c3f0793dfabc")
)
def feature_importance(random_forest_trained, training_data):
    
    clf = random_forest_trained.stages[0].model
    clf = random_forest_trained.stages[0].model
    df = training_data.toPandas()
    
    X = df.drop(columns = ["long_covid", "person_id",  "random"])
    imps = pd.Series(clf.feature_importances_)
    imps.index = X.columns
    imps = imps.sort_values(ascending=False).reset_index()
    imps.columns = ["feature", "importance"]

    return spark.createDataFrame(imps)

'''
    clf = random_forest_trained.stages[0].model
    df = training_data.toPandas()
    X = df.drop("ventilator", axis=1)
    imps = pd.Series(clf.feature_importances_)
    imps.index = X.columns
    imps = imps.sort_values(ascending=False).reset_index()
    imps.columns = ["feature", "importance"]

'''

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.12cf84b7-5768-4a24-800a-d8d29f183f27"),
    feature_importance=Input(rid="ri.foundry.main.dataset.9bb492f8-18e9-4b40-a2f6-3adc128ca603")
)
def feature_importance_plot(feature_importance):
    
    df = feature_importance.toPandas()
    df = df.sort_values("importance", ascending=False).head(50)
    df.index = df["feature"]
    plt.figure(figsize=(7, 14))
    sns.barplot(x=df["importance"], y=df["feature"], palette=sns.color_palette("RdYlBu", df.shape[0]))
    plt.tight_layout()
    plt.show()
    return None

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4c45e96e-c27a-42ad-abf6-c03545083c1d"),
    drug_features=Input(rid="ri.foundry.main.dataset.4b035e8a-a890-43d8-aa7c-e917363ed7be")
)
def pivot_drugs(drug_features):
    df = drug_features

    df = df.withColumn("ingredient", F.regexp_replace(df["ingredient"], r'[^A-Za-z0-9]', '_') )
    df = df.groupby("person_id").pivot("ingredient").agg(F.max("post_only_med").alias("post_only_med"))
    df = df.fillna(0)
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ebeb3830-4ac7-4c5d-a3c9-428876c7506b"),
    random_forest_trained=Input(rid="ri.foundry.main.dataset.4acae42e-111a-4024-8b12-d7e560f61183"),
    test_data=Input(rid="ri.foundry.main.dataset.8e3588f7-bc6b-4dcb-ac82-9f9ab3982fdd")
)
def plot_roc_curve(random_forest_trained, test_data):
    
    df = test_data.toPandas()

    y = df["long_covid"]
    X = df.drop(columns = ["long_covid", "person_id",  "random"])

    clf = random_forest_trained.stages[0].model

    y_pred = clf.predict_proba(X)
    y_pred = [y[1] for y in y_pred]

    fpr, tpr, thresholds = roc_curve(y, y_pred)

    roc_auc = auc(fpr, tpr)

    
    
    lw = 2
    plt.figure(figsize=(7, 7))
    plt.plot(fpr, tpr, color='darkorange',
            lw=lw, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()

    roc_df = pd.DataFrame({"FPR": fpr, "TPR": tpr, "threshold": thresholds})

    return spark.createDataFrame(roc_df).orderBy("threshold")

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4acae42e-111a-4024-8b12-d7e560f61183"),
    training_data=Input(rid="ri.foundry.main.dataset.fb1fd621-c97c-4e66-a2f5-c3f0793dfabc")
)
def random_forest_trained(training_data):

    df = training_data.toPandas()
    y = df["long_covid"]
    X = df.drop(columns = ["long_covid", "person_id",  "random"])

    # Tried increasing n_estimators and changing criterion, with no eeffect
    clf = RandomForestClassifier(n_estimators=1000, verbose=1, criterion='entropy')
    
    clf.fit(X, y)

    return Model(Stage(clf))
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40bdf50a-e909-4335-ae1c-fbb8dbda1dc4"),
    hosp_not_long_covid=Input(rid="ri.foundry.main.dataset.7b2b5050-aa66-42f4-b71f-7bd0eb2c5178")
)
def sample_hosp_not_long_covid(hosp_not_long_covid):

    df = hosp_not_long_covid

    df = df.sample(0.05, seed=42)
   

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e1067fc6-fc66-44f7-94bd-2a0b44e8a883"),
    not_hosp_not_long_covid=Input(rid="ri.foundry.main.dataset.18df8bcd-94e4-48fb-aaf7-82b9bbc54af6")
)
def sample_not_hosp_not_long_covid(not_hosp_not_long_covid):
    df = not_hosp_not_long_covid

    df = df.sample(0.0037, seed=42)
   
    

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.98b5ce44-c2ef-412d-8ffe-8fe8cd55d81f"),
    long_covids=Input(rid="ri.foundry.main.dataset.733f3868-da85-437c-acd8-16a98f97d9cb"),
    sample_hosp_not_long_covid=Input(rid="ri.foundry.main.dataset.40bdf50a-e909-4335-ae1c-fbb8dbda1dc4"),
    sample_not_hosp_not_long_covid=Input(rid="ri.foundry.main.dataset.e1067fc6-fc66-44f7-94bd-2a0b44e8a883")
)
def union_data(long_covids, sample_not_hosp_not_long_covid, sample_hosp_not_long_covid):
    sample_negatives = sample_not_hosp_not_long_covid
    

    # Want to get roughly the same number of hospitalized and non-hospita

    
    lc = long_covids
    snh = sample_not_hosp_not_long_covid
    sh  = sample_hosp_not_long_covid

    result = lc.union(snh)
    result = result.union(sh)

    result = result.withColumn("random", F.rand(seed=42) )

    # This is now one hot encoded in the field 'long_covid'
    return result.drop("patient_group")

