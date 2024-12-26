import sys
import time
import numpy as np

from skopt import gp_minimize
from skopt.space import Integer

# 定义目标函数
def objective_function(X):
    # 将 X 转换为 NumPy 数组
    X = np.array(X)
    
    # 假设 y[0] 和 y[1] 是根据 X 计算出的
    y1 = np.sum(X)  # 第一维
    y2 = np.sum((X - 2) ** 2)  # 第二维，这里是一个简单的平方和

    return (y1, y2)

# 自定义约束条件：确保第一维小于阈值
def constrained_objective(X, threshold):
    y = objective_function(X)
    if y[0] < threshold:
        return y[1]  # 返回第二维的值以进行优化
    else:
        return 10000  # 如果第一维不满足条件，返回无穷大

# 定义参数空间，假设每个维度的取值为 0 到 10 的离散整数
param_space = [
    Integer(0, 10, name='x1'),
    Integer(0, 10, name='x2'),
    Integer(0, 10, name='x3'),
    Integer(0, 10, name='x4'),
    Integer(0, 10, name='x5'),
    Integer(0, 5, name='x6')
]

# 设置阈值
threshold_value = 15

start_time = time.time()

# 执行贝叶斯优化
res = gp_minimize(lambda x: constrained_objective(x, threshold_value), 
                   dimensions=param_space,
                   n_calls=50,  # 评估次数
                   random_state=42)
end_time = time.time()

# 输出结果
print("最佳参数:", res.x)
print("对应的目标值:", objective_function(res.x))
print("搜索时间：", end_time - start_time, "秒")