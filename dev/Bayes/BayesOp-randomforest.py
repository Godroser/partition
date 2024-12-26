import numpy as np
from skopt import BayesSearchCV
from skopt.space import Integer
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.datasets import make_regression
from sklearn.metrics import mean_squared_error

import matplotlib.pyplot as plt
from sklearn.tree import plot_tree


# 1. 创建多元回归数据集
# 假设我们有5个特征，每个特征取值范围是 0, 1, 2, 3, 4
n_samples = 100
n_features = 5
X = np.random.randint(0, 5, size=(n_samples, n_features))  # 生成输入X，取值在0到4之间
y = np.random.randn(n_samples)  # 生成目标值y，这里使用随机值来模拟回归问题

# 2. 切分数据集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 3. 定义搜索空间：每个特征的取值范围是 [0, 1, 2, 3, 4]
# 同时，搜索超参数是随机森林回归模型的常见超参数
search_space = {
    'n_estimators': Integer(10, 100),  # 随机森林的树木数量 (树的数量范围从10到100)
    'max_depth': Integer(1, 10),        # 树的最大深度 (从1到10)
    'min_samples_split': Integer(2, 10),  # 每次分裂的最小样本数 (从2到10)
    'min_samples_leaf': Integer(1, 5),   # 叶节点的最小样本数 (从1到5)
}

# 4. 使用贝叶斯优化寻找最佳超参数
opt = BayesSearchCV(
    estimator=RandomForestRegressor(random_state=42),
    search_spaces=search_space,
    n_iter=20,          # 进行50次优化迭代
    cv=3,               # 3折交叉验证
    random_state=42,
    verbose=1
)

# 5. 拟合贝叶斯优化模型
opt.fit(X_train, y_train)

# 6. 输出最优超参数
print(f"Best parameters found: {opt.best_params_}")

# 7. 在测试集上进行预测并评估
y_pred = opt.predict(X_test)
print(y_pred)
print(y_test)
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error on test set: {mse:.4f}")


for idx, tree in enumerate(opt.best_estimator_.estimators_):
    plt.figure(figsize=(15, 10))
    plot_tree(tree, filled=True, feature_names=[f'Feature {i}' for i in range(X_train.shape[1])], class_names=['Target'], rounded=True)
    plt.title(f"Tree {idx + 1}")
    # 保存图形到文件
    plt.savefig('decision_treeP{}.png'.format(idx), format='png')  # 保存为PNG文件


