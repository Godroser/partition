import random
import string
import sys
import os
import time

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

import pandas as pd
import torch
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.data import Data
from sklearn.metrics import roc_auc_score
from torch_geometric.transforms import RandomLinkSplit
from torch_geometric.utils import negative_sampling

# 测试函数
def test(model, data):
    model.eval()
    with torch.no_grad():
        mu, logstd = model.encode(data.x, data.edge_index)
        z = model


def get_connection(autocommit: bool = True) -> MySQLConnection:
  config = Config()
  db_conf = {
      "host": config.TIDB_HOST,
      "port": config.TIDB_PORT,
      "user": config.TIDB_USER,
      "password": config.TIDB_PASSWORD,
      "database": config.TIDB_DB_NAME,
      "autocommit": autocommit,
      # mysql-connector-python will use C extension by default,
      # to make this example work on all platforms more easily,
      # we choose to use pure python implementation.
      "use_pure": True
  }

  if config.ca_path:
      db_conf["ssl_verify_cert"] = True
      db_conf["ssl_verify_identity"] = True
      db_conf["ssl_ca"] = config.ca_path
  return mysql.connector.connect(**db_conf)




"""
order_line 
节点v的表示: [column的数据类型, distinct value占的比例 * 100,在predicate中出现的频率, 在aggregate操作中出现的频率, 在DML语句中出现的频率]
边e的表示: [两个节点的表示, join操作的selectivity, join操作占的比例]
数据类型, int:0, decimal:1, datetiem:2, char:3, 
"""

def get_type_code(data_type):
    if "int" in data_type:
        return 0
    elif "decimal" in data_type or "float" in data_type or "double" in data_type:
        return 1
    elif "datetime" in data_type or "timestamp" in data_type or "date" in data_type:
        return 2
    elif "char" in data_type or "text" in data_type:
        return 3
    else:
        return 4

def get_basic_embedding(): ##get basic embedding like column的数据类型, distinct value占的比例 * 100
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            # 获取所有表名
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            
            result = {}
            for table in tables:
                table_name = table[0]
                result[table_name] = {}

                # 获取表的列信息
                cur.execute(f"SHOW COLUMNS FROM `{table_name}`")
                columns = cur.fetchall()
                
                for column in columns:
                    column_name = column[0]
                    data_type = column[1].lower()  # 列的数据类型

                    # 获取 count(distinct column) 和 count(column)
                    distinct_query = f"SELECT COUNT(DISTINCT `{column_name}`), COUNT(`{column_name}`) FROM `{table_name}`"
                    cur.execute(distinct_query)
                    distinct_count, total_count = cur.fetchone()

                    # 计算 distinct 比例
                    ratio = (distinct_count / total_count) * 100 if total_count > 0 else 0

                    # 获取类型码
                    type_code = get_type_code(data_type)

                    # 保存结果
                    result[table_name][column_name] = [ratio, type_code]

            return result           





def txt_to_tensor(file_path):
    """
    读取txt文件, 将每行的[]内的数据提取并转换为torch tensor
    :param file_path: 文本文件路径
    :return: n*m 的 torch tensor
    """
    data = []
    
    # 读取文件
    with open(file_path, 'r') as file:
        for line in file:
            # 提取[]内的数据
            if '[' in line and ']' in line:
                start = line.find('[')
                end = line.find(']')
                array_str = line[start + 1:end]  # 提取括号内的内容
                
                # 转换为列表
                array = list(map(float, array_str.split(',')))
                data.append(array)
    
    # 转换为 torch tensor
    tensor = torch.tensor(data)
    return tensor





class VGAE(torch.nn.Module):
    def __init__(self, in_channels, hidden_channels, out_channels):
        super(VGAE, self).__init__()
        self.conv1 = GCNConv(in_channels, hidden_channels)
        self.conv_mu = GCNConv(hidden_channels, out_channels)
        self.conv_logstd = GCNConv(hidden_channels, out_channels)

    def encode(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        mu = self.conv_mu(x, edge_index)
        logstd = self.conv_logstd(x, edge_index)
        return mu, logstd

    def reparameterize(self, mu, logstd):
        if self.training:
            std = torch.exp(logstd)
            eps = torch.randn_like(std)
            return mu + eps * std
        else:
            return mu

    def decode(self, z, edge_index):
        return (z[edge_index[0]] * z[edge_index[1]]).sum(dim=-1)

    def forward(self, x, edge_index):
        mu, logstd = self.encode(x, edge_index)
        z = self.reparameterize(mu, logstd)
        adj_pred = self.decode(z, edge_index)
        return adj_pred, mu, logstd
    
    
# 训练函数
def train(model, optimizer, train_data):
    model.train()
    optimizer.zero_grad()
    mu, logstd = model.encode(train_data.x, train_data.edge_index)
    z = model.reparameterize(mu, logstd)
    recon_loss = F.binary_cross_entropy_with_logits(
        model.decode(z, train_data.edge_index), 
        torch.ones(train_data.edge_index.size(1), device=z.device)
    )
    kl_loss = -0.5 * torch.mean(1 + 2 * logstd - mu.pow(2) - torch.exp(2 * logstd))
    loss = recon_loss + kl_loss
    loss.backward()
    optimizer.step()
    return loss.item()

# 测试函数
def test(model, data):
    model.eval()
    with torch.no_grad():
        mu, logstd = model.encode(data.x, data.edge_index)
        z = model.reparameterize(mu, logstd)

        # 生成负样本
        neg_edge_index = negative_sampling(
            edge_index=data.edge_index,
            num_nodes=data.num_nodes,
            num_neg_samples=data.edge_index.size(1),
            method="sparse"
        )
                
        pos_pred = model.decode(z, data.edge_index).sigmoid()
        neg_pred = model.decode(z, neg_edge_index).sigmoid()

        pos_y = torch.ones(pos_pred.size(0))
        neg_y = torch.zeros(neg_pred.size(0))

        y_pred = torch.cat([pos_pred, neg_pred])
        y_true = torch.cat([pos_y, neg_y])

        # 计算 AUC
        auc = roc_auc_score(y_true.cpu(), y_pred.cpu())
    return auc 


# 保存模型嵌入和预测结果
def save_final_output(model, data, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # 获取节点嵌入
    model.eval()
    with torch.no_grad():
        mu, logstd = model.encode(data.x, data.edge_index)
        z = model.reparameterize(mu, logstd)

    # 保存节点嵌入
    embeddings_path = os.path.join(output_dir, "node_embeddings.csv")
    pd.DataFrame(z.cpu().numpy()).to_csv(embeddings_path, index=False, encoding='utf-8')
    print(f"Node embeddings saved to {embeddings_path}")

    # 生成边的预测
    edge_index = data.edge_index
    with torch.no_grad():
        pred = model.decode(z, edge_index).sigmoid()  # 计算边存在的概率

    # 保存边的预测
    edges_path = os.path.join(output_dir, "edge_predictions.csv")
    pd.DataFrame(pred.cpu().numpy(), columns=["Prediction"]).to_csv(edges_path, index=False, encoding='utf-8')
    print(f"Edge predictions saved to {edges_path}")

    # 打印嵌入和边的预测示例
    print("Example node embeddings:", z[:5])  # 打印前5个节点的嵌入
    print("Example edge predictions:", pred[:5])  # 打印前5条边的预测概率

    

if __name__ == "__main__":
    # # 示例数据
    # x = torch.tensor([[1, 0], [0, 1]], dtype=torch.float)
    # edge_index = torch.tensor([[0, 1],[1, 0]], dtype=torch.long)
    # data = Data(x=x, edge_index=edge_index)

    # import graph data
    file_path = 'workload/basic_embedding.txt'  # 替换为你的txt文件路径
    x = txt_to_tensor(file_path)
    edge_index = torch.tensor([[40,95,47,75,95,47,0,2,1,51,50,49,54,53,52,62,64,63,57,56,45,45,95,40,76,0,47,58,46,56,54,52],[75,45,70,92,45,70,65,64,63,64,63,62,64,63,62,52,54,53,76,75,95,9,45,75,57,65,70,66,9,75,76,62]])
    
    
    data = Data(x=x, edge_index=edge_index)

    transform = RandomLinkSplit(is_undirected=True)
    train_data, val_data, test_data = transform(data)    

    # 初始化模型
    model = VGAE(in_channels=4, hidden_channels=16, out_channels=4)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    
    # # 单独测试部分
    # # 前向传播
    # adj_pred, mu, logstd = model(data.x, data.edge_index)
    # # 打印结果
    # print("预测的邻接矩阵:", adj_pred)
    # print("均值:", mu)
    # print("对数标准差:", logstd)


    # 训练过程
    for epoch in range(100):
        loss = train(model, optimizer, train_data)
        if epoch % 10 == 0:
            val_auc = test(model, val_data)
            test_auc = test(model, test_data)
            print(f'Epoch {epoch:03d}, Loss: {loss:.4f}, Val AUC: {val_auc:.4f}, Test AUC: {test_auc:.4f}')

    model.eval()
    edge_index = data.edge_index
    with torch.no_grad():
        mu, logstd = model.encode(data.x, data.edge_index)
        z = model.reparameterize(mu, logstd)
        pred = model.decode(z, edge_index).sigmoid()
    # 打印嵌入和边的预测示例
    print("Example node embeddings:", z)  # 打印前5个节点的嵌入
    print("Example edge predictions:", pred[:5])  # 打印前5条边的预测概率
    
    output_dir = "Output/test/"
    save_final_output(model, data, output_dir)

    # result = get_basic_embedding()
    # file_path = 'basic_embedding.txt'
    # with open(file_path, 'w', encoding='utf-8') as file:
    #     for table, columns in result.items():
    #         file.write(f"Table: {table}\n")
    #         for column, data in columns.items():
    #             ratio, type_code = data
    #             file.write(f"  {column} = [{type_code}, {ratio:.6f}]\n")
    #         file.write("\n")  # Add a blank line between tables
    # print(f"Results written to {file_path}")
