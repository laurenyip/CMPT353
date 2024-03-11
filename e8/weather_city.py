# python3 weather_city.py monthly-data-labelled.csv monthly-data-unlabelled.csv labels.csv

import sys
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.neighbors import KNeighborsClassifier

labelled_data = pd.read_csv(sys.argv[1])
unlabelled_data = pd.read_csv(sys.argv[2])

# X: data for the columns tmax-01 through snwd-12
X = labelled_data.loc[:,'tmax-01':'snwd-12']
# Y: the city column
Y = labelled_data['city']

# make unlabelled dataset
X_unlabelled = unlabelled_data.loc[:,'tmax-01':'snwd-12']

# split up training and testing data
X_train, X_test, Y_train, Y_test = train_test_split(X, Y)

# make a model - use a pipeline to first scale the data, then fit a KNN classifier
model = make_pipeline(StandardScaler(), KNeighborsClassifier(n_neighbors=10))

# train the model
model.fit(X_train, Y_train)

# make predictions on the unlabelled data
predictions = model.predict(X_unlabelled)

# print the score
print("Score :", round(model.score(X_test, Y_test), 3))


df = pd.DataFrame({'truth': Y_test, 'prediction': model.predict(X_test)})
#print(df[df['truth'] != df['prediction']])

# output format: one prediction per line

pd.Series(predictions).to_csv(sys.argv[3], index=False, header=False)