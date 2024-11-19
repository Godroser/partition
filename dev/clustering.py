import pandas as pd
from sklearn.cluster import KMeans

# 读取CSV文件
data = pd.read_csv('Output/node_embeddings.csv')

# 获取向量数据
X = data.values

# 选择聚类算法（这里使用KMeans，你可以根据需要替换）
kmeans = KMeans(n_clusters=4, random_state=0)  # 这里假设分成3个簇，你可以调整

# 进行聚类
kmeans.fit(X)

# 获取聚类标签
labels = kmeans.labels_

# 将聚类标签添加到原始数据中
data['cluster'] = labels

# 输出结果（可以保存为CSV文件或直接打印）
print(data)
data.to_csv('Output/clustering.csv', index=False)