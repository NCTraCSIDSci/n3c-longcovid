pip install xgboost==1.4.2

## Make sure to install this version of xgboost otherwise you may experience serialization errors

import xgboost

xgb_model = xgboost.XGBClassifier()

## Have long_covid_model.ubj somewhere in the project folder and give right path to it
xgb_model.load_model('long_covid_model.ubj')

person_id = test_data['person_id']

test_data.drop(['person_id', 'long_covid'], axis=1, inplace=True)
X = test_data

y_pred = xgb_model.predict_proba(X)

y_pred = [y[1] for y in y_pred]

result = {"y_pred" : y_pred}

result = pd.DataFrame.from_dict(result)

result["person_id"] = person_id



