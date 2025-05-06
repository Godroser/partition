import re
from collections import defaultdict
import sys
import os
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from estimator.ch_columns_ranges_meta import Customer_columns, Warehouse_columns, Supplier_columns, Stock_columns, Region_columns, Orders_columns, Order_line_columns, New_order_columns, Nation_columns, Item_columns, History_columns, District_columns

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

def construct_graph(join_conditions):
    """
    根据Join_Conditions构造无向图, 计算边的重复次数和节点的度数。

    :param join_conditions: Join_Conditions列表
    :return: 图结构(edges, degrees)
    """
    edges = defaultdict(int)  # 存储边及其重复次数
    degrees = defaultdict(int)  # 存储节点的度数

    for sql_joins in join_conditions:
        for join in sql_joins:
            left_table = join["tables"]["left"]
            right_table = join["tables"]["right"]

            for condition in join["conditions"]:
                left_column = condition["left_column"]
                right_column = condition["right_column"]

                # 构造边，确保边的顺序不影响结果
                edge = tuple(sorted([(left_table, left_column), (right_table, right_column)]))
                edges[edge] += 1

                # 更新节点的度数
                degrees[left_table] += 1
                degrees[right_table] += 1

    return edges, degrees

def calculate_edge_weights(edges):
    """
    计算每条边的权重，权重=cost*次数, cost由边包含的两个表的列的size相加得到。

    :param edges: 边及其重复次数的字典
    :return: 边及其权重的字典
    """
    # 表名到列大小的映射
    table_columns_map = {
        "customer": Customer_columns(),
        "district": District_columns(),
        "item": Item_columns(),
        "new_order": New_order_columns(),
        "orders": Orders_columns(),
        "order_line": Order_line_columns(),
        "stock": Stock_columns(),
        "warehouse": Warehouse_columns(),
        "history": History_columns(),
        "nation": Nation_columns(),
        "supplier": Supplier_columns(),
        "region": Region_columns(),
    }

    edge_weights = {}

    for edge, count in edges.items():
        # 获取边的两个节点
        (table1, column1), (table2, column2) = edge

        # 获取表的列大小
        table1_obj = table_columns_map[table1]
        table2_obj = table_columns_map[table2]

        # 找到列的大小
        try:
            size1 = table1_obj.columns_size[table1_obj.columns.index(column1)]
        except ValueError:  # 如果 column1 不在 table1_obj.columns 中
            size1 = table2_obj.columns_size[table2_obj.columns.index(column1)]
        try:
            size2 = table1_obj.columns_size[table1_obj.columns.index(column2)]
        except ValueError:  # 如果 column1 不在 table1_obj.columns 中
            size2 = table2_obj.columns_size[table2_obj.columns.index(column2)]

        # 计算cost和权重
        cost = size1 + size2
        weight = cost * count

        edge_weights[edge] = weight

    return edge_weights


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
            "left_column": "o_entry_d",
            "right_column": "ol_delivery_d"
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
            "right_column": "i_id"
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
            "left_column": "i_id",
            "right_column": "s_i_id"
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
    edges, degrees = construct_graph(Join_Conditions)
    print("Edges with counts:")
    for edge, count in edges.items():
        print(f"{edge}: {count}")
    print("\nDegrees of nodes:")
    for node, degree in degrees.items():
        print(f"{node}: {degree}")


    edge_weights = calculate_edge_weights(edges)
    print("Edge weights:")
    for edge, weight in edge_weights.items():
        print(f"{edge}: {weight}")