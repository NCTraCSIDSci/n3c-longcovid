## Purpose of this code
The code in this folder creates and cleans up the feature table that the model ultimately runs against. All feature engineering steps are included in this folder.

## Running this code
Run the SQL steps in numbered order, ensuring that you carefully read the comments within. You will likely need to make changes to the code to account for local features of your data that may not be present in N3C. The comments will guide you to the places in the code that will most likely need to be changed.

Once the two output tables from the SQL steps are available, you can run the Python steps, starting with utils. The output will be a feature table that can be passed to the model in the next code step.
