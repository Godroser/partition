def extract_latency(file_path):
    latency_values = []  # 用于存储提取的延迟值
    with open(file_path, 'r') as file:
        for line in file:
            # 提取匹配格式的值
            if "latency (avg):" in line:
                try:
                    # 找到 ":" 后面的数值
                    latency = float(line.split("latency (avg):")[-1].strip().rstrip('s'))
                    latency_values.append(latency)
                except ValueError:
                    print(f"无法解析行：{line.strip()}")
    return latency_values

# 示例用法
file_path = "summary_latency.txt"  # 替换为你的文件路径
latencies = extract_latency(file_path)
for i in latencies:
    print(i)

