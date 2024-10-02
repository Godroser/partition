# 使用 MySQL Connector/Python 连接 TiDB

[English](/README.md) | 中文

这是 PingCAP 为 PyMySQL 编写的用于连接 TIDB 的示例项目
> TiDB 是一个兼容 MySQL 的数据库。[MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/) 是 MySQL 官方推出的 Python 版本驱动。

## 前置要求

- 推荐 [Python 3.10 及以上版本](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)
- TiDB 集群。如果你还没有 TiDB 集群，可以按照以下方式创建：
  - （推荐方式）参考[创建 TiDB Serverless 集群](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud)，创建你自己的 TiDB Cloud 集群。
  - 参考[部署本地测试 TiDB 集群](https://docs.pingcap.com/zh/tidb/stable/quick-start-with-tidb#部署本地测试集群)或[部署正式 TiDB 集群](https://docs.pingcap.com/zh/tidb/stable/production-deployment-using-tiup)，创建本地集群。

## 开始实践

### 1. 克隆示例代码仓库到本地

```shell
git clone https://github.com/tidb-samples/tidb-python-mysqlconnector-quickstart.git
cd tidb-python-mysqlconnector-quickstart
```

### 2. 安装依赖 (包括 MySQL Connector/Python)

```shell
pip install -r requirements.txt
```

### 3. 配置连接信息

<details open>
<summary><b>(选项 1) TiDB Serverless</b></summary>

1. 在 TiDB Cloud 控制台中，打开 [Clusters](https://tidbcloud.com/console/clusters) 页面，选择你的 TiDB Serverless 集群，进入 **Overview** 页面，点击右上角的 **Connect** 按钮。
2. 确认窗口中的配置和你的运行环境一致。
    - **Endpoint Type** 为 **Public**
    - **Connect With** 为 **General**
    - Operating System 为你的运行环境
    > 如果你在 Windows Subsystem for Linux (WSL) 中运行，请切换为对应的 Linux 发行版。
3. 点击 **Generate password** 生成密码。
    > 如果你之前已经生成过密码，可以直接使用原密码，或点击 **Reset Password** 重新生成密码。
4. 运行以下命令，将 `.env.example` 复制并重命名为 `.env`：

    ```shell
    cp .env.example .env
    ```

5. 复制并粘贴对应连接字符串至 `.env` 中。示例结果如下：

   ```python
    TIDB_HOST='{gateway-region}.aws.tidbcloud.com'
    TIDB_PORT='4000'
    TIDB_USER='{prefix}.root'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    CA_PATH='{ca_path}'
    ```

    注意替换 `{}` 中的占位符为 **Connect** 窗口中获得的值。

    TiDB Serverless 要求使用安全连接，你可以参考 [TiDB Cloud 文档](https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-clusters#root-certificate-default-path)获取不同操作系统下证书的路径。

6. 保存文件。

</details>

<details>

<summary><b>(选项 2) TiDB Dedicated</b></summary>

1. 在 TiDB Cloud Web Console 中，选择你的 TiDB Dedicated 集群，进入 **Overview** 页面，点击右上角的 **Connect** 按钮。点击 **Allow Access from Anywhere** 并点击 **Download TiDB cluster CA** 下载证书。
    > 更多配置细节，可参考 [TiDB Dedicated 标准连接教程](https://docs.pingcap.com/tidbcloud/connect-via-standard-connection).

2. 运行以下命令，将 `.env.example` 复制并重命名为 `.env`：

    ```shell
    cp .env.example .env
    ```

3. 复制并粘贴对应的连接字符串至 `.env` 中。示例结果如下：

   ```python
    TIDB_HOST='{host}.clusters.tidb-cloud.com'
    TIDB_PORT='4000'
    TIDB_USER='{username}'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    CA_PATH='{your-downloaded-ca-path}'
    ```

    注意替换 `{}` 中的占位符为 **Connect** 窗口中获得的值，并配置前面步骤中下载好的证书路径。

4. 保存文件。

</details>

<details>
<summary><b>(选项 3) 自建 TiDB</b></summary>

1. 运行以下命令，将 `.env.example` 复制并重命名为 `.env`：

    ```shell
    cp .env.example .env
    ```

2. 复制并粘贴对应 TiDB 的连接字符串至 `.env` 中。示例结果如下：

    ```python
    TIDB_HOST='{tidb_server_host}'
    TIDB_PORT='4000'
    TIDB_USER='root'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    ```

    注意替换 `{}` 中的占位符为你的 TiDB 对应的值，并删除 `CA_PATH` 这行。如果你在本机运行 TiDB，默认 Host 地址为 `127.0.0.1`，密码为空。

3. 保存文件。

</details>

### 4. 运行示例代码

```shell
python mysql_connector_example.py
```

### 5. 期望输出

[期望的输出](/Expected-Output.txt)

## 注意事项

- Driver 封装程度较低，因此在程序内会见到大量的 SQL 语句。与 ORM 不同，因为没有数据对象的存在，`MySQL Connector/Python` 的查询对象以元组 (tuple)/字典(dict) 进行表示。虽然 Python 的 Driver 相较其他语言的使用更方便，但因其不可屏蔽底层实现，需手动管控事务的特性，如果没有大量必须使用 SQL 的场景，仍然推荐使用 ORM 进行程序编写。这可以降低程序的耦合性。
- 关于 MySQL Connector/Python 的更多使用方法，可以参考其[官方文档](https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html)。

## 下一步

- 你可以继续阅读开发者文档，以获取更多关于 TiDB 的开发者知识。例如：[插入数据](https://docs.pingcap.com/zh/tidb/stable/dev-guide-insert-data)，[更新数据](https://docs.pingcap.com/zh/tidb/stable/dev-guide-update-data)，[删除数据](https://docs.pingcap.com/zh/tidb/stable/dev-guide-delete-data)，[单表读取](https://docs.pingcap.com/zh/tidb/stable/dev-guide-get-data-from-single-table)，[事务](https://docs.pingcap.com/zh/tidb/stable/dev-guide-transaction-overview)，[SQL 性能优化](https://docs.pingcap.com/zh/tidb/stable/dev-guide-optimize-sql-overview)等。
- 如果你更倾向于参与课程进行学习，我们也提供专业的 [TiDB 开发者课程](https://cn.pingcap.com/courses-catalog/back-end-developer/)支持，并在考试后提供相应的[资格认证](https://learn.pingcap.com/learner/certification-center)。
