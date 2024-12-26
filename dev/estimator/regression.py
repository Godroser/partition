import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score


# 从 CSV 文件读取数据
data = pd.read_csv('data/TableRangeScan.csv')

# 提取特征 X 和目标 y
X = data[['cardinality','size']].values
y = data['latency'].values

# 将数据集划分为训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 创建多元线性回归模型
model = LinearRegression()

# 训练模型
model.fit(X_train, y_train)

# 进行预测
y_pred = model.predict(X_test)

# 评估模型
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"均方误差 (MSE): {mse}")
print(f"决定系数 (R²): {r2}")

# 输出模型的系数和截距
print(f"系数: {model.coef_}")
print(f"截距: {model.intercept_}")