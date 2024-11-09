# TIDB_HOST='127.0.0.1'
# TIDB_PORT='4000'
# TIDB_USER='root'
# TIDB_PASSWORD=''
# TIDB_DB_NAME='tpch'
# ca_path = ''


class Config:
    def __init__(self) -> None:
      self.TIDB_HOST='127.0.0.1'
      self.TIDB_PORT='4000'
      self.TIDB_USER='root'
      self.TIDB_PASSWORD=''
      self.TIDB_DB_NAME='tpcc'
      self.ca_path = ''