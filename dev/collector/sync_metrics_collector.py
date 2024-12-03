from prometheus_api_client import PrometheusConnect
import time

# Prometheus 服务器的地址
PROMETHEUS_URL = "http://10.77.110.144:9090"

# 初始化 Prometheus 连接
prom = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)

# 查询 Prometheus 获取指标数据
def get_tiflash_syncing_data_freshness_count():
    try:
        # 使用 PromQL 查询 `tiflash_syncing_data_freshness_count`
        query = "tiflash_syncing_data_freshness_count"
        
        # 获取指标数据（返回的是时间序列数据）
        result = prom.custom_query(query)
        
        # 输出查询结果
        if result:
            # print("Result:", result)
            for metric in result:
                # 转换时间戳为可读的时间格式
                readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(metric['value'][0])))
                # print(f"Time: {readable_time}, Value: {metric['value'][1]}")

                # only one metric in result, so return it
                return readable_time, metric['value'][1]
        else:
            print("No data available for the query.")
    
    except Exception as e:
        print(f"Error fetching data: {e}")


# 查询 Prometheus 获取指标数据
def direct_get_tiflash_syncing_data_freshness_count():
    try:
        # 使用 PromQL 查询 `tiflash_syncing_data_freshness_count`
        query = "tiflash_syncing_data_freshness_count"
        
        # 获取指标数据（返回的是时间序列数据）
        result = prom.custom_query(query)
        
        # 输出查询结果
        if result:
            # print("Result:", result)
            for metric in result:
                # 转换时间戳为可读的时间格式
                # readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(metric['value'][0])))
                # print(f"Time: {readable_time}, Value: {metric['value'][1]}")

                # only one metric in result, so return it
                return metric['value'][0], metric['value'][1]
        else:
            print("No data available for the query.")
    
    except Exception as e:
        print(f"Error fetching data: {e}")

# 循环查询，设定一个时间间隔
def fetch_data_periodically(interval):
    while True:
        # print(f"Fetching data from Prometheus...")
        readable_time, metric = get_tiflash_syncing_data_freshness_count()
        print(f"Time: {readable_time}, Value: {metric}")  
        # print(f"Waiting for {interval} seconds before next fetch...")
        time.sleep(interval)



# 程序入口
if __name__ == "__main__":
    fetch_data_periodically(interval=1)  # 每 60 秒查询一次
