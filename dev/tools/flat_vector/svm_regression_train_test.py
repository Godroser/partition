import pandas as pd
from sklearn.svm import SVR
from sklearn.metrics import r2_score

# 1. Load training and test data
train_csv = 'sql_features_with_time_train.csv'
test_csv = 'sql_features_with_time.csv'
df_train = pd.read_csv(train_csv)
df_test = pd.read_csv(test_csv)

# 2. Prepare features and label
y_train = df_train['label'].values
X_train = df_train.drop(columns=['label']).values
y_test = df_test['label'].values
X_test = df_test.drop(columns=['label']).values

# 3. Train SVR model on training set
svr = SVR(kernel='rbf')
svr.fit(X_train, y_train)

# 4. Predict on test set
y_pred = svr.predict(X_test)

# 5. Evaluate R^2 score
score = r2_score(y_test, y_pred)
print(f'SVR R^2 score (accuracy) on test set: {score:.4f}')

# 6. Compute pairwise ranking loss

def pairwise_ranking_loss(y_true, y_pred):
    n = len(y_true)
    total = 0
    incorrect = 0
    for i in range(n):
        for j in range(i+1, n):
            if y_true[i] == y_true[j]:
                continue
            total += 1
            # If the order of prediction does not match the order of true label
            if (y_true[i] < y_true[j] and y_pred[i] >= y_pred[j]) or (y_true[i] > y_true[j] and y_pred[i] <= y_pred[j]):
                incorrect += 1
    return incorrect / total if total > 0 else 0.0

ranking_loss = pairwise_ranking_loss(y_test, y_pred)
print(f'Pairwise ranking loss on test set: {ranking_loss:.4f}') 