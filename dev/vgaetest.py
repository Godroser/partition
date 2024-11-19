import torch
import torch.nn.functional as F
from torch_geometric.nn import VGAE, GCNConv
from torch_geometric.transforms import RandomLinkSplit
from torch_geometric.data import Data

# 定义 GCN 编码器
class GCNEncoder(torch.nn.Module):
    def __init__(self, in_channels, out_channels):
        super(GCNEncoder, self).__init__()
        self.conv1 = GCNConv(in_channels, 2 * out_channels)
        self.conv2 = GCNConv(2 * out_channels, out_channels)
    
    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        return self.conv2(x, edge_index)

# 生成随机图数据
def generate_random_graph(num_nodes, num_features, num_edges):
    x = torch.rand((num_nodes, num_features))  # 节点特征矩阵
    edge_index = torch.randint(0, num_nodes, (2, num_edges))  # 边索引矩阵
    return x, edge_index

# 数据准备
num_nodes = 100
num_features = 16
num_edges = 500
x, edge_index = generate_random_graph(num_nodes, num_features, num_edges)

# 转换为 PyTorch Geometric 数据对象
data = Data(x=x, edge_index=edge_index)

# 使用 RandomLinkSplit 分割数据集
transform = RandomLinkSplit(is_undirected=True)
train_data, val_data, test_data = transform(data)

# 定义 VGAE 模型
embedding_dim = 32
encoder = GCNEncoder(in_channels=num_features, out_channels=embedding_dim)
model = VGAE(encoder)

# 优化器
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# 训练函数
def train():
    model.train()
    optimizer.zero_grad()
    print(model.encode(train_data.x, train_data.edge_index))  # 编码潜在表示
    z=0
    print(type(z), z)
    
    loss = model.recon_loss(z, train_data.edge_index)  # 重构损失
    loss += (1 / train_data.num_nodes) * model.kl_loss()  # KL 损失
    loss.backward()
    optimizer.step()
    return loss.item()

# 测试函数
def test(data):
    model.eval()
    with torch.no_grad():
        z = model.encode(data.x, data.edge_index)  # 编码潜在表示
        auc, ap = model.test(z, data.edge_index, data.neg_edge_index)  # AUC 和 AP
    return auc, ap

# 训练过程
for epoch in range(200):
    loss = train()
    if epoch % 10 == 0:
        val_auc, val_ap = test(val_data)
        test_auc, test_ap = test(test_data)
        print(f'Epoch {epoch:03d}, Loss: {loss:.4f}, Val AUC: {val_auc:.4f}, Val AP: {val_ap:.4f}, Test AUC: {test_auc:.4f}, Test AP: {test_ap:.4f}')
