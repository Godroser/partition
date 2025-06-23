def multiply_list_by_200(input_list):
    # 使用列表推导式将每个元素乘以200
    return [x * 0.92 for x in input_list]

def get_list_from_input():
    numbers = []
    print("请输入数字（每行一个），输入非数字结束：")
    
    while True:
        try:
            # 获取用户输入
            user_input = input()
            
            # 尝试将输入转换为数字
            number = float(user_input)
            numbers.append(number)
            
        except ValueError:
            # 如果输入不是数字，结束输入
            if numbers:  # 确保至少输入了一个数字
                break
            else:
                print("请至少输入一个数字！")
                continue
    
    return numbers

# 主程序
if __name__ == "__main__":
    # 获取用户输入的列表
    input_list = get_list_from_input()
    
    # 调用函数并打印结果
    result = multiply_list_by_200(input_list)
    print("原始列表:", input_list)
    print("乘以200后的列表:")
    for res in result:
        print(res)