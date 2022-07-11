## Purpose of this code
This folder contains the pre-trained model from N3C. In order to use this model as is, you will either need to have all of the same features available in your data, or be willing to fill in missing features with 0s. (E.g., if no one in your data has a diagnosis of "malaise," because malaise is a feature in our model, you will need to add a placeholder for that missing feature.)

## Running this code
To run the model, load the .ubj file using xgboost as follows:

\# xgboost 1.4.2  
import xgboost  
xgb_model = xgboost.XGBClassifier()  
xgb_model.load_model(PATH_TO_MODEL.ubj)
