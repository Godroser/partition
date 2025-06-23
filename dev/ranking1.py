import numpy as np
from itertools import combinations

def ranking_loss(y_true, y_pred1, y_pred2, y_pred3):
    """
    计算两个预测模型的Ranking Loss
    
    参数:
    y_true -- 真实值的列表/数组
    y_pred1 -- 模型1预测值的列表/数组
    y_pred2 -- 模型2预测值的列表/数组
    
    返回:
    loss1 -- 模型1的Ranking Loss
    loss2 -- 模型2的Ranking Loss
    """
    # 转换为numpy数组
    y_true = np.array(y_true)
    y_pred1 = np.array(y_pred1)
    y_pred2 = np.array(y_pred2)
    y_pred3 = np.array(y_pred3)
    
    # 检查输入长度是否一致
    assert len(y_true) == len(y_pred1) == len(y_pred2) == len(y_pred3), "输入列表长度必须相等"
    
    # 生成所有可能的样本对
    pairs = list(combinations(range(len(y_true)), 2))

    print("lenght of pairs is: ", len(pairs))
    
    def calculate_loss(y_true, y_pred):
        loss = 0
        valid_pairs = 0
        
        for i, j in pairs:
            # 只计算有明确排序关系的对
            if y_true[i] == y_true[j]:
                continue
                
            valid_pairs += 1
            
            # 确定真实排序关系
            if y_true[i] > y_true[j]:
                true_order = 1
            else:
                true_order = -1
                
            # 确定预测排序关系
            if y_pred[i] > y_pred[j]:
                pred_order = 1
            else:
                pred_order = -1
                
            # 计算损失（0-1损失）
            loss += 0 if true_order == pred_order else 1
        
        return loss / valid_pairs if valid_pairs > 0 else 0
    
    # 计算两个模型的Ranking Loss
    loss1 = calculate_loss(y_true, y_pred1)
    loss2 = calculate_loss(y_true, y_pred2)
    loss3 = calculate_loss(y_true, y_pred3)

    return loss1, loss2, loss3

# 示例使用
if __name__ == "__main__":
    # 示例数据
    y_true = [0,1,2,3,4,5,6,7,8,9]
    
    y_model1 = [1,2,3,4,5,6,7,8,9,0]  # 模型1预测分数
    y_model2 = [0,9,8,7,6,5,4,3,2,1]  # 模型2预测分数
    
    y_model3 =[0,1,2,4,3,6,5,7,8,9]
    
    loss1, loss2, loss3 = ranking_loss(y_true, y_model1, y_model2, y_model3)
    
    print(f"模型1的Ranking Loss: {loss1:.4f}")
    print(f"模型2的Ranking Loss: {loss2:.4f}")
    print(f"模型3的Ranking Loss: {loss3:.4f}")
    
    if loss1 < loss2:
        print("模型1的排序性能更好")
    else:
        print("模型2的排序性能更好")