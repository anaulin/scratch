import numpy as np
import operator
import pandas
import re
from sklearn.feature_selection import chi2, SelectKBest
from sklearn import cross_validation
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier, ExtraTreesClassifier, AdaBoostClassifier, VotingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn import svm

def add_title(data):
  def get_title(name):
    title_search = re.search(' ([A-Za-z]+)\.', name)
    return title_search.group(1) if title_search else ""
  titles = data["Name"].apply(get_title)
  # Create mapping from title to number code.
  mapping = {}
  idx = 0
  for title in titles.unique():
    mapping[title] = idx
    idx += 1
  for k, v in mapping.items():
    titles[titles == k] = v
  data["Title"] = titles

def add_family_id(data):
  family_id_mapping = {}
  # A function to get the id given a row
  def get_family_id(row):
    # Find the last name by splitting on a comma
    last_name = row["Name"].split(",")[0]
    # Create the family id
    family_id = "{0}{1}".format(last_name, row["FamilySize"])
    # Look up the id in the mapping
    if family_id not in family_id_mapping:
        if len(family_id_mapping) == 0:
            current_id = 1
        else:
            # Get the maximum id from the mapping and add one to it if we don't have an id
            # TODO: What is this line even doing?
            current_id = (max(family_id_mapping.items(), key=operator.itemgetter(1))[1] + 1)
        family_id_mapping[family_id] = current_id
    return family_id_mapping[family_id]
  family_ids = data.apply(get_family_id, axis=1)
  data["FamilyId"] = family_ids

def cleanup_data(data):
    # Add missing values -- set to median of existing values.
    data["Age"] = data["Age"].fillna(data["Age"].median())
    data["Fare"] = data["Fare"].fillna(data["Fare"].median())
    # Convert features to numeric
    data.loc[data["Sex"] == "male", "Sex"] = 0
    data.loc[data["Sex"] == "female", "Sex"] = 1
    data["Embarked"] = data["Embarked"].fillna("S")
    data.loc[data["Embarked"] == "S", "Embarked"] = 0
    data.loc[data["Embarked"] == "C", "Embarked"] = 1
    data.loc[data["Embarked"] == "Q", "Embarked"] = 2
    # Add features
    data["FamilySize"] = data["SibSp"] + data["Parch"]
    data["NameLength"] = data["Name"].apply(lambda x: len(x))
    add_title(data)
    add_family_id(data)

def select_features(data):
  selector = SelectKBest(chi2, k=10)
  # Possible redictors are all of our columns, without any of the non-numerical ones (and not "Survived").
  predictors = [p for p in list(data.columns.values) if p not in ["Cabin", "Name", "Ticket", "Survived"]]
  selector.fit(data[predictors], data["Survived"])
  # Chose the predictors with a p-value below 5%.
  chosen_predictors = [p for p in zip(predictors, selector.pvalues_) if p[1] < 0.05]
  return [pair[0] for pair in chosen_predictors]

def get_predictions(data, algo, features):
    kf = cross_validation.KFold(data.shape[0], n_folds=3, random_state=1)
    predictions = []
    for train, test in kf:
        train_target = data["Survived"].iloc[train]
        # Fit the algorithm on the training data.
        algo.fit(data[features].iloc[train,:], train_target)
        # Select and predict on the test fold.
        # The .astype(float) is necessary to convert the dataframe to all floats and avoid an sklearn error.
        test_predictions = algo.predict_proba(data[features].iloc[test,:].astype(float))[:,1]
        predictions.append(test_predictions)
    # Put all the predictions together into one array.
    predictions = np.concatenate(predictions, axis=0)
    return pandas.Series(predictions)

def get_accuracy(predictions, data):
  reduce_to_decision(predictions)
  return sum(predictions == data["Survived"]) * 1.0 / len(predictions)

def test_algo(data, algo, features):
  predictions = get_predictions(data, algo, features)
  accuracy = get_accuracy(predictions, data)
  print "Accuracy:", accuracy, "Algo:", type(algo)
  return accuracy

def ensemble(data, algos, features):
  prediction_sum = get_predictions(data, algos[0], features)
  for algo in algos[1:]:
    prediction_sum = prediction_sum + get_predictions(data, algo, features)
  return prediction_sum / len(algos)

def reduce_to_decision(predictions):
  predictions[predictions <= 0.5] = 0
  predictions[predictions > 0.5] = 1

def train_full_and_predict(training_data, test_data, features, algo):
  algo.fit(training_data[features], training_data["Survived"])
  predictions = algo.predict_proba(test_data[features].astype(float))[:,1]
  reduce_to_decision(predictions)
  return pandas.Series(predictions.astype(int))

if __name__ == '__main__':
  titanic = pandas.read_csv("train.csv")
  cleanup_data(titanic)
  features = select_features(titanic)
  print "Selected features:", features
  # A selection of classifiers to try out. Parameters a little tuned, except for SVM.
  algos = [
    GradientBoostingClassifier(random_state=1, n_estimators=9, max_depth=3),
    LogisticRegression(random_state=1),
    RandomForestClassifier(n_estimators=13, max_depth=11, min_samples_split=2, random_state=1),
    ExtraTreesClassifier(n_estimators=10, max_depth=13, min_samples_split=16, random_state=1),
    AdaBoostClassifier(n_estimators=100),
    GaussianNB(),
    svm.SVC(probability=True, random_state=1)
  ]

  for algo in algos:
    test_algo(titanic, algo, features)
  ensemble_predictions = ensemble(titanic, algos, features)
  print "Ensemble accuracy:", get_accuracy(ensemble_predictions, titanic)

  # Looks like our RandomForestClassifier works best, standalone. Use that for final submission.
  titanic_test = pandas.read_csv("titanic_test.csv")
  cleanup_data(titanic_test)
  predictions = train_full_and_predict(titanic, titanic_test, features,
    RandomForestClassifier(n_estimators=13, max_depth=11, min_samples_split=2, random_state=1))
  submission = pandas.DataFrame({"PassengerId": titanic_test["PassengerId"], "Survived": predictions})
  submission.to_csv("kaggle.csv", index=False)
