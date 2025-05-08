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

