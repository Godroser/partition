<div align= "center">
    <h1>Jasper</h1>
</div>

<p align="center">
  <a href="#1-setup">Setup</a> •
  <a href="#2-configuration">Configurations</a> •
  <a href="#3-run">Run</a> •
  <a href="#4-citation">Citation</a> •
</p>

Jasper is a joint adaptive storage framework for HTAP systems. Jasper jointly optimizes *horizontal and vertical partitioning* along with *selective column store* configuration. We propose *MCTS-HTAP*, a workload-aware search algorithm that integrates with a lightweight, data synchronization aware *evaluation model* to estimate both query execution time and synchronization overhead. Moreover, Jasper supports incremental configuration updates, allowing the system to adapt to workload changes without significant performance disruption.


## 1. Setup

### DB Cluster
We implement Jasper on TiDB, an open-source commercial HTAP database system. We recommend to install and deploy TiDB with:

[Deploy a local test cluster (for macOS and Linux)](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb/#deploy-a-local-test-cluster)

[Simulate production deployment on a single machine (for Linux only)](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb/#simulate-production-deployment-on-a-single-machine)

### Requirements

TiUP: ​​TiUP is the component manager for the TiDB ecosystem, providing one-click deployment, upgrade, and management of TiDB clusters and related tools.

mysql.connector: ​​mysql.connector is the official Python driver for MySQL, enabling database interactions through a pure Python implementation with support for transactions, connection pooling, and compatibility with MySQL protocols.​

## 2.Configuration

Jasper configurations can be changed in the config.py file. Here we list several most important ones. You can adjust the relevant parameters based on the deployment situation.

```bash
self.TIDB_HOST=''
self.TIDB_PORT=''
self.TIDB_USER=''
self.TIDB_PASSWORD=''
self.TIDB_DB_NAME=''
```

## 3. Run

### Test Jasper

Step 1: Change the settings within ./dev/config.py.

Step 2: Run the test script to get the performance.

```bash
python test_advisor.py
```

### Test MCST-HTAP

Step 1: Change the settings within ./dev/config.py.

Step 2: Run the search script with evaluation model.

```bash
python advisor.py
```

## 4. Citation

If you use Grep in your research, please cite:

```bibtex
@article{DBLP:journals/pacmmod/ZhouLFLG23,
  author       = {Xuanhe Zhou and
                  Guoliang Li and
                  Jianhua Feng and
                  Luyang Liu and
                  Wei Guo},
  title        = {Grep: {A} Graph Learning Based Database Partitioning System},
  journal      = {Proc. {ACM} Manag. Data},
  volume       = {1},
  number       = {1},
  pages        = {94:1--94:24},
  year         = {2023},
  url          = {https://doi.org/10.1145/3588948},
  doi          = {10.1145/3588948},
  timestamp    = {Mon, 19 Jun 2023 16:36:09 +0200},
  biburl       = {https://dblp.org/rec/journals/pacmmod/ZhouLFLG23.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```
