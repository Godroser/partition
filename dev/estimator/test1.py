import re

def calculate_weighted_sum(time_str):
    # 匹配带有单位的数字
    pattern = re.compile(r'(\d+(?:\.\d+)?)(ms|m|s)')
    total_sum = 0

    # 遍历每个匹配到的数字和单位
    for match in pattern.findall(time_str):
        value, unit = match
        value = float(value)
        
        # 根据单位进行加权计算
        if unit == 'm':
            total_sum += value * 60000
        elif unit == 's':
            total_sum += value * 1000
        elif unit == 'ms':
            total_sum += value

    return total_sum

# 测试示例
inputs = ["5m23.34ms", "1m3.2s10ms", "1m6.3s"]
for input_str in inputs:
    result = calculate_weighted_sum(input_str)
    print(f"输入: {input_str} -> 加权总和: {result}")
