import random
import string
import sys
import os
import time

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

import torch
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.data import Data



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

ol_o_id = [0, 0.253]
ol_w_id = [0, 0.003107]
ol_number = [0, 0.01165, , 2]    
ol_i_id = [0, 77.68]
ol_supply_w_id = [0, 0.003107]
ol_delivery_d = [2, 1000, 1]   
ol_quantity = [0, 0.007768]
ol_amount = [1, 26.02]
ol_dist_info = [3, 997.05]


c_id = [0, 2.500000]
c_d_id = [0, 0.008333]
c_w_id = [0, 0.003333]
c_first = [3, 100.000000]
c_middle = [3, 0.000833]
c_last = [3, 0.833333]
c_street_1 = [3, 100.000000]
c_street_2 = [3, 100.000000]
c_city = [3, 100.000000]
c_state = [3, 0.563333]
c_zip = [3, 8.333333]
c_phone = [3, 100.000000]
c_since = [2, 0.000833]
c_credit = [3, 0.001667]
c_credit_lim = [1, 0.000833]
c_discount = [1, 4.167500]
c_balance = [1, 12.519167]
c_ytd_payment = [1, 6.261667]
c_payment_cnt = [0, 0.004167]
c_delivery_cnt = [0, 0.001667]
c_data = [3, 100.000000]


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

if __name__ == "__main__":
    # # 示例数据
    # x = torch.tensor([[1, 0], [0, 1]], dtype=torch.float)
    # edge_index = torch.tensor([[0, 1], [1, 0]], dtype=torch.long)
    # data = Data(x=x, edge_index=edge_index)

    # # 初始化模型
    # model = VGAE(in_channels=2, hidden_channels=16, out_channels=8)

    # # 前向传播
    # adj_pred, mu, logstd = model(data.x, data.edge_index)

    # # 打印结果
    # print("预测的邻接矩阵:", adj_pred)
    # print("均值:", mu)
    # print("对数标准差:", logstd)

    result = get_basic_embedding()
    file_path = 'basic_embedding.txt'
    with open(file_path, 'w', encoding='utf-8') as file:
        for table, columns in result.items():
            file.write(f"Table: {table}\n")
            for column, data in columns.items():
                ratio, type_code = data
                file.write(f"  {column} = [{type_code}, {ratio:.6f}]\n")
            file.write("\n")  # Add a blank line between tables
    print(f"Results written to {file_path}")
