import random

def generate_similar_numbers(base_num, threshold=0.1, num_results=5):
    """
    生成与基准数字相近的随机数
    
    参数:
        base_num: 基准数字
        threshold: 允许的最大差异比例(默认0.1即±10%)
        num_results: 要生成的数字数量(默认4个)
    
    返回:
        包含相似数字的列表
    """
    if threshold <= 0:
        raise ValueError("阈值必须为正数")
    
    # 计算允许的最小值和最大值
    min_val = base_num * (1 - threshold)
    max_val = base_num * (1 + threshold)
    # min_val = base_num - threshold
    # max_val = base_num + threshold
    
    # 生成指定数量的随机数
    similar_numbers = [random.uniform(min_val, max_val) for _ in range(num_results)]
    
    return similar_numbers

# 示例使用
if __name__ == "__main__":
    try:
        base_number = float(input("请输入基准数字: "))
        threshold = 0.1
        
        results = generate_similar_numbers(base_number, threshold)
        
        for result in results:
            print(result)
    
    except ValueError as e:
        print(f"输入错误: {e}")