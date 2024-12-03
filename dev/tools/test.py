import numpy as np

def calculate_average_ratios_from_file(file_path):
    """
    从文件中读取 22*4 的数据，计算每行第 2、3、4 列相对于第 1 列的比值（百分数），并返回平均比值。
    
    :param file_path: 输入数据文件的路径
    :return: 第 2、3、4 列比值的平均值
    """
    try:
        # 从文件读取数据，假设以制表符分隔
        data = np.loadtxt(file_path, delimiter='\t')
        
        # 检查数据形状是否正确
        if data.shape[1] != 4:
            raise ValueError("数据文件的每行必须有 4 列！")
        
        # 计算比值（以百分数计算）
        ratios = (data[:, 1:] / data[:, [0]]) * 100

        # 计算每列的平均比值
        avg_ratios = np.mean(ratios, axis=0)
        
        return avg_ratios
    
    except Exception as e:
        print(f"发生错误: {e}")
        return None

# 示例用法
file_path = "test.txt"  # 替换为你的数据文件路径

# 计算平均比值
average_ratios = calculate_average_ratios_from_file(file_path)

# 输出结果
if average_ratios is not None:
    print("\n第 2、3、4 列相对于第 1 列的平均比值（百分数）:")
    print(f"第 2 列平均比值: {average_ratios[0]:.2f}%")
    print(f"第 3 列平均比值: {average_ratios[1]:.2f}%")
    print(f"第 4 列平均比值: {average_ratios[2]:.2f}%")
