import torch
import torch.nn.functional as F
from torch_geometric.nn import GCNConv
from torch_geometric.data import Data

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

# 示例数据
x = torch.tensor([[1, 0], [0, 1]], dtype=torch.float)
edge_index = torch.tensor([[0, 1], [1, 0]], dtype=torch.long)
data = Data(x=x, edge_index=edge_index)

# 初始化模型
model = VGAE(in_channels=2, hidden_channels=16, out_channels=8)

# 前向传播
adj_pred, mu, logstd = model(data.x, data.edge_index)

# 打印结果
print("预测的邻接矩阵:", adj_pred)
print("均值:", mu)
print("对数标准差:", logstd)