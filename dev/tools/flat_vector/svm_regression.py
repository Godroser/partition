import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.svm import SVR
from sklearn.metrics import r2_score
import time

# 1. Load data
csv_file = 'sql_features_with_time.csv'
df = pd.read_csv(csv_file)

# 2. Prepare features and label
y = df['label'].values
X = df.drop(columns=['label']).values

# 3. Split train/test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# 4. Train SVR model
svr = SVR(kernel='rbf')
svr.fit(X_train, y_train)

# 5. Predict and evaluate
y_pred = svr.predict(X_test)
score = r2_score(y_test, y_pred)
print(f'SVR R^2 score (accuracy) on test set: {score:.4f}')

# 6. Predict on all data and compute ranking loss
start_time = time.time()
all_pred = svr.predict(X)
predict_time = time.time() - start_time
print(f'Prediction on all data took {predict_time:.6f} seconds')

# Compute pairwise ranking loss manually
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

ranking_loss = pairwise_ranking_loss(y, all_pred)
print(f'Pairwise ranking loss on all data: {ranking_loss:.4f}') 