import re
from collections import defaultdict

def parse_workload_joins(file_path="/data3/dzh/project/grep/dev/workload/workloadd.sql"):
    """
    解析SQL文件中的join条件, 提取涉及的表名和列名。

    :param file_path: SQL文件路径
    :return: 包含每条SQL的join条件信息的列表
    """
    join_conditions = []

    # 正则表达式匹配join条件
    join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON\s+([\w\.]+)\s*=\s*([\w\.]+)", re.IGNORECASE)

    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if not line:  # 跳过空行
                continue

            matches = join_pattern.findall(line)
            if matches:
                for match in matches:
                    table_name = match[0]
                    column1 = match[1]
                    column2 = match[2]
                    join_conditions.append({
                        "table": table_name,
                        "columns": [column1, column2]
                    })

    return join_conditions

def construct_graph_from_conditions(join_conditions):
    """
    根据Join_Conditions构造无向图。

    :param join_conditions: Join_Conditions列表
    :return: 图的结构，字典形式 {table: [(connected_table, edge_info)]}
    """
    graph = defaultdict(list)

    for sql_conditions in join_conditions:
        for condition in sql_conditions:
            left_table = condition["tables"]["left"]
            right_table = condition["tables"]["right"]
            edge_info = {
                "tables": condition["tables"],
                "conditions": condition["conditions"]
            }
            # 添加无向边
            graph[left_table].append((right_table, edge_info))
            graph[right_table].append((left_table, edge_info))

    return graph

# [[{}]] 列表代表每个sql, 列表代表每个join条件, {}代表join条件的具体信息
Join_Conditions = [
    # 1
    [],
    # 2
    [
    {
        "tables": {
        "left": "item",
        "right": "stock"
        },
        "conditions": [
        {
            "left_column": "i_id",
            "right_column": "s_i_id"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "supplier"
        },
        "conditions": [
        {
            "left_column": "s_w_id",
            "right_column": "s_suppkey"
        },
        {
            "left_column": "s_i_id",
            "right_column": "s_suppkey"
        }
        ]
    },
    {
        "tables": {
        "left": "supplier",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    },
    {
        "tables": {
        "left": "nation",
        "right": "region"
        },
        "conditions": [
        {
            "left_column": "n_regionkey",
            "right_column": "r_regionkey"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "supplier"
        },
        "conditions": [
        {
            "left_column": "s_w_id",
            "right_column": "s_suppkey"
        },
        {
            "left_column": "s_i_id",
            "right_column": "s_suppkey"
        }
        ]
    },
    {
        "tables": {
        "left": "supplier",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    },
    {
        "tables": {
        "left": "nation",
        "right": "region"
        },
        "conditions": [
        {
            "left_column": "n_regionkey",
            "right_column": "r_regionkey"
        }
        ]
    }
    ],
    # 3
    [
    {
        "tables": {
        "left": "customer",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "c_id",
            "right_column": "o_c_id"
        },
        {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "new_order"
        },
        "conditions": [
        {
            "left_column": "o_w_id",
            "right_column": "no_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "no_d_id"
        },
        {
            "left_column": "o_id",
            "right_column": "no_o_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "order_line"
        },
        "conditions": [
        {
            "left_column": "o_w_id",
            "right_column": "ol_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "ol_d_id"
        },
        {
            "left_column": "o_id",
            "right_column": "ol_o_id"
        }
        ]
    }
    ],
    # 4
    [
    {
        "tables": {
        "left": "orders",
        "right": "order_line"
        },
        "conditions": [
        {
            "left_column": "o_id",
            "right_column": "ol_o_id"
        },
        {
            "left_column": "o_w_id",
            "right_column": "ol_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "ol_d_id"
        },
        {
            "left_column": "ol_delivery_d",
            "right_column": "o_entry_d"
        }
        ]
    }
    ],
    # 5
    [
    {
        "tables": {
        "left": "customer",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "c_id",
            "right_column": "o_c_id"
        },
        {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "order_line"
        },
        "conditions": [
        {
            "left_column": "o_id",
            "right_column": "ol_o_id"
        },
        {
            "left_column": "o_w_id",
            "right_column": "ol_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "ol_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "stock"
        },
        "conditions": [
        {
            "left_column": "ol_w_id",
            "right_column": "s_w_id"
        },
        {
            "left_column": "ol_i_id",
            "right_column": "s_i_id"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    },
    {
        "tables": {
        "left": "nation",
        "right": "region"
        },
        "conditions": [
        {
            "left_column": "n_regionkey",
            "right_column": "r_regionkey"
        }
        ]
    }
    ],
    # 6
    [],
    # 7
    [
    {
        "tables": {
        "left": "order_line",
        "right": "stock"
        },
        "conditions": [
        {
            "left_column": "ol_supply_w_id",
            "right_column": "s_w_id"
        },
        {
            "left_column": "ol_i_id",
            "right_column": "s_i_id"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "ol_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "ol_d_id",
            "right_column": "o_d_id"
        },
        {
            "left_column": "ol_o_id",
            "right_column": "o_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "customer"
        },
        "conditions": [
        {
            "left_column": "o_c_id",
            "right_column": "c_id"
        },
        {
            "left_column": "o_w_id",
            "right_column": "c_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "c_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "supplier",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    }
    ],
    # 8
    [
    {
        "tables": {
        "left": "item",
        "right": "stock"
        },
        "conditions": [
        {
            "left_column": "i_id",
            "right_column": "s_i_id"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "order_line"
        },
        "conditions": [
        {
            "left_column": "s_i_id",
            "right_column": "ol_i_id"
        },
        {
            "left_column": "s_w_id",
            "right_column": "ol_supply_w_id"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "supplier"
        },
        "conditions": [
        {
            "left_column": "MOD((s_w_id * s_i_id), 10000)",
            "right_column": "s_suppkey"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "ol_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "ol_d_id",
            "right_column": "o_d_id"
        },
        {
            "left_column": "ol_o_id",
            "right_column": "o_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "customer"
        },
        "conditions": [
        {
            "left_column": "o_c_id",
            "right_column": "c_id"
        },
        {
            "left_column": "o_w_id",
            "right_column": "c_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "c_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "nation",
        "right": "region"
        },
        "conditions": [
        {
            "left_column": "n_regionkey",
            "right_column": "r_regionkey"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    },
    {
        "tables": {
        "left": "nation",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "n_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    }
    ],
    # 9
    [
    {
        "tables": {
        "left": "order_line",
        "right": "stock"
        },
        "conditions": [
        {
            "left_column": "ol_i_id",
            "right_column": "s_i_id"
        },
        {
            "left_column": "ol_supply_w_id",
            "right_column": "s_w_id"
        }
        ]
    },
    {
        "tables": {
        "left": "stock",
        "right": "nation"
        },
        "conditions": [
        {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "ol_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "ol_d_id",
            "right_column": "o_d_id"
        },
        {
            "left_column": "ol_o_id",
            "right_column": "o_id"
        }
        ]
    },
    {
        "tables": {
        "left": "order_line",
        "right": "item"
        },
        "conditions": [
        {
            "left_column": "ol_i_id",
            "right_column": "i_id"
        }
        ]
    }
    ],
    # 10
    [
    {
        "tables": {
        "left": "customer",
        "right": "orders"
        },
        "conditions": [
        {
            "left_column": "c_id",
            "right_column": "o_c_id"
        },
        {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
        },
        {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
        }
        ]
    },
    {
        "tables": {
        "left": "orders",
        "right": "order_line"
        },
        "conditions": [
        {
            "left_column": "o_w_id",
            "right_column": "ol_w_id"
        },
        {
            "left_column": "o_d_id",
            "right_column": "ol_d_id"
        },
        {
            "left_column": "o_id",
            "right_column": "ol_o_id"
        }
        ]
    }
    ],
    # 11
    [
        {
        "tables": {
            "left": "supplier",
            "right": "nation"
        },
        "conditions": [
            {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
            }
        ]
        }
    ],
    # 12
    [
        {
        "tables": {
            "left": "orders",
            "right": "order_line"
        },
        "conditions": [
            {
            "left_column": "ol_w_id",
            "right_column": "o_w_id"
            },
            {
            "left_column": "ol_d_id",
            "right_column": "o_d_id"
            },
            {
            "left_column": "ol_o_id",
            "right_column": "o_id"
            }
        ]
        }
    ],
    # 13
    [
        {
        "tables": {
            "left": "customer",
            "right": "orders"
        },
        "conditions": [
            {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
            },
            {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
            },
            {
            "left_column": "c_id",
            "right_column": "o_c_id"
            }
        ]
        }
    ],
    # 14
    [
        {
        "tables": {
            "left": "order_line",
            "right": "item"
        },
        "conditions": [
            {
            "left_column": "ol_i_id",
            "right_column": "i_i_id"
            }
        ]
        }
    ],
    # 15
    [],
    # 16
    [
        {
        "tables": {
            "left": "stock",
            "right": "item"
        },
        "conditions": [
            {
            "left_column": "i_i_id",
            "right_column": "st_s_i_id"
            }
        ]
        }
    ],
    # 17
    [],
    # 18
    [
        {
        "tables": {
            "left": "customer",
            "right": "orders"
        },
        "conditions": [
            {
            "left_column": "c_id",
            "right_column": "o_c_id"
            },
            {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
            },
            {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
            }
        ]
        },
        {
        "tables": {
            "left": "orders",
            "right": "order_line"
        },
        "conditions": [
            {
            "left_column": "o_w_id",
            "right_column": "ol_w_id"
            },
            {
            "left_column": "o_d_id",
            "right_column": "ol_d_id"
            },
            {
            "left_column": "o_id",
            "right_column": "ol_o_id"
            }
        ]
        }
    ],
    # 19
    [
        {
        "tables": {
            "left": "order_line",
            "right": "item"
        },
        "conditions": [
            {
            "left_column": "ol_i_id",
            "right_column": "i_id"
            }
        ]
        }
    ],
    # 20
    [
        {
        "tables": {
            "left": "supplier",
            "right": "nation"
        },
        "conditions": [
            {
            "left_column": "s_nationkey",
            "right_column": "n_nationkey"
            }
        ]
        }
    ],
    # 21
    [
        {
        "tables": {
            "left": "customer",
            "right": "orders"
        },
        "conditions": [
            {
            "left_column": "c_id",
            "right_column": "o_c_id"
            },
            {
            "left_column": "c_w_id",
            "right_column": "o_w_id"
            },
            {
            "left_column": "c_d_id",
            "right_column": "o_d_id"
            }
        ]
        },
        {
        "tables": {
            "left": "order_line",
            "right": "item"
        },
        "conditions": [
            {
            "left_column": "ol_i_id",
            "right_column": "i_id"
            }
        ]
        }
    ],
    # 22
    []
]


# 示例调用
if __name__ == "__main__":
    result = parse_workload_joins()
    for item in result:
        print(item)
    
    graph = construct_graph_from_conditions(Join_Conditions)
    for table, connections in graph.items():
        print(f"Table: {table}")
        for connected_table, edge_info in connections:
            print(f"  Connected to: {connected_table}, Edge Info: {edge_info}")
