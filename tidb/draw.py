import matplotlib.pyplot as plt
import numpy as np

# 数据准备
workloads = ['OLTP Heavy', 'Balanced', 'OLAP Heavy']
categories = ['Category A', 'Category B', 'Category C', 'Category D']  # 假设有4个类别（对应图中多色柱子）

# 示例数据（单位：秒），根据实际需求修改
data = {
    'Category A': [6500, 3000, 5800],  # 每个类别对应3种工作负载的完成时间
    'Category B': [4200, 1800, 3900],
    'Category C': [3800, 1500, 3600],
    'Category D': [2800, 1200, 2500]
}

# 颜色设置（接近原图的橙色、紫色、红色、绿色）
colors = ['#FF7F0E', '#9467BD', '#D62728', '#2CA02C']

# 绘图
fig, ax = plt.subplots(figsize=(10, 6))

# 设置柱子宽度和位置
bar_width = 0.2
x = np.arange(len(workloads))  # 工作负载的横坐标位置

# 绘制每组柱子
for i, (category, values) in enumerate(data.items()):
    ax.bar(x + i * bar_width, values, width=bar_width, 
           label=category, color=colors[i])

# 设置图表标题和坐标轴标签
ax.set_title('Workload Completion Time Comparison', fontsize=14)
ax.set_xlabel('Workload Type', fontsize=12)
ax.set_ylabel('Completion Time (sec)', fontsize=12)
ax.set_xticks(x + bar_width * (len(categories) - 1) / 2)  # 居中显示横坐标标签
ax.set_xticklabels(workloads)

# 设置纵坐标范围（0~7200秒）和网格线
ax.set_ylim(0, 7200)
ax.yaxis.grid(True, linestyle='--', alpha=0.6)

# 添加图例
ax.legend(title='Categories', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()  # 自动调整布局
plt.savefig('draw.png', dpi = 400)