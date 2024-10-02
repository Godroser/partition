# Connecting to TiDB cluster with MySQL Connector/Python

English | [中文](/README-zh.md)

This a sample project written by PingCAP for MySQL Connector/Python to connect to TiDB.
> TiDB is a MySQL-compatible database. And [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/) is an official MySQL driver written in Python.

## Prerequisites

- [Python 3.10 or higher](https://www.python.org/downloads/)
- [Git](https://git-scm.com/downloads)
- A TiDB cluster. If you don't have a TiDB cluster, you can create one as follows:
  - (Recommended) Follow [Creating a TiDB Serverless Cluster](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud) to create your own TiDB Cloud cluster.
  - Follow [Deploy a Local Test TiDB Cluster](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb#deploy-a-local-test-cluster) or [Deploy a Production TiDB Cluster](https://docs.pingcap.com/tidb/stable/production-deployment-using-tiup) to create a local cluster

## Getting started

### 1. Clone the repository

```shell
git clone https://github.com/tidb-samples/tidb-python-mysqlconnector-quickstart.git
cd tidb-python-mysqlconnector-quickstart
```

### 2. Install dependencies (including MySQL Connector/Python)

```shell
pip install -r requirements.txt
```

### 3. Configure connection information

<details open>
<summary><b>(Option 1) TiDB Serverless</b></summary>

1. In the TiDB Cloud, navigate to the [Clusters](https://tidbcloud.com/console/clusters) page, select your TiDB Serverless cluster. Go to the **Overview** page, and click the **Connect** button in the upper right corner.
2. Ensure the configurations in the confirmation window match your operating environment.
    - **Endpoint Type** is set to **Public**
    - **Connect With** is set to **General**
    - Operating System matches your environment
    > If you are running in Windows Subsystem for Linux (WSL), switch to the corresponding Linux distribution.
3. Click **Create password** to create a password.
    > If you have created a password before, you can either use the original password or click **Reset Password** to generate a new one.
4. Run the following command to copy `.env.example` and rename it to `.env`:

    ```shell
    cp .env.example .env
    ```

5. Copy and paste the corresponding connection string into the `.env` file. Example result is as follows:

   ```python
    TIDB_HOST='{gateway-region}.aws.tidbcloud.com'
    TIDB_PORT='4000'
    TIDB_USER='{prefix}.root'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    CA_PATH='{ca_path}'
    ```

    Be sure to replace the placeholders `{}` with the values obtained from the connection dialog.

    TiDB Serverless requires a secure connection, you can refer to the [TLS Connections to TiDB Serverless](https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-clusters) to get the certificate paths for different operating systems.

6. Save the `.env` file.

</details>

<details>

<summary><b>(Option 2) TiDB Dedicated</b></summary>

1. In the TiDB Cloud, select your TiDB Dedicated cluster. Go to the **Overview** page, and click the **Connect** button in the upper right corner. Click **Allow Access from Anywhere** and then click **Download TiDB cluster CA** to download the certificate.
    > For more configuration details, refer to [TiDB Dedicated Standard Connection](https://docs.pingcap.com/tidbcloud/connect-via-standard-connection).
2. Run the following command to copy `.env.example` and rename it to `.env`:

    ```shell
    cp .env.example .env
    ```

3. Copy and paste the corresponding connection string into the `.env` file. Example result is as follows:

   ```python
    TIDB_HOST='{host}.clusters.tidb-cloud.com'
    TIDB_PORT='4000'
    TIDB_USER='{username}'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    CA_PATH='{your-downloaded-ca-path}'
    ```

    Be sure to replace the placeholders `{}` with the values obtained from the **Connect** window, and configure `CA_PATH` with the certificate path downloaded in the previous step.

4. Save the `.env` file.

</details>

<details>
<summary><b>(Option 3) Self-Hosted TiDB</b></summary>

1. Run the following command to copy `.env.example` and rename it to `.env`:

    ```shell
    cp .env.example .env
    ```

2. Copy and paste the corresponding connection string into the `.env` file. Example result is as follows:

    ```python
    TIDB_HOST='{tidb_server_host}'
    TIDB_PORT='4000'
    TIDB_USER='root'
    TIDB_PASSWORD='{password}'
    TIDB_DB_NAME='test'
    ```

    Be sure to replace the placeholders `{}` with the values, and remove the `CA_PATH` line. If you are running TiDB locally, the default host address is `127.0.0.1`, and the password is empty.

3. Save the `.env` file.

</details>

### 4. Run

```shell
python mysql_connector_example.py
```

### 5. Expected output

[Expected output](/Expected-Output.txt)

## Useful Notes

- This Python driver is relatively low-level, so you will see a lot of SQL statements in the sample app. Unlike ORMs, there is no data object, and `MySQL Connector/Python` represents query objects with tuple/dict. Although Python's driver is more convenient to use than those in other languages, due to its exposure to underlying implementations and the manual transaction management required, it is still recommended to use ORMs for programming unless there is a significant need for SQL. This can reduce the coupling of your app.
- For more on how to use `MySQL Connector/Python`, refer to its [official documentation](https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html).

## Next Steps

- You can continue reading the developer documentation to get more knowledge about TiDB development, such as: [Insert Data](https://docs.pingcap.com/tidb/stable/dev-guide-insert-data), [Update Data](https://docs.pingcap.com/tidb/stable/dev-guide-update-data), [Delete Data](https://docs.pingcap.com/tidb/stable/dev-guide-delete-data), [Single Table Reading](https://docs.pingcap.com/tidb/stable/dev-guide-get-data-from-single-table), [Transactions](https://docs.pingcap.com/tidb/stable/dev-guide-transaction-overview), [SQL Performance Optimization](https://docs.pingcap.com/tidb/stable/dev-guide-optimize-sql-overview), etc.
- If you prefer to learn through courses, we also offer professional [TiDB Developer Courses](https://www.pingcap.com/education/), and provide [TiDB certifications](https://www.pingcap.com/education/certification/) after the exam.
