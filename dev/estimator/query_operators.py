query_operators = [
    {
        "operators": ["TableScan", "Selection"],
        "tables": ["order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "TableReader", "Selection", "TableScan", "Selection"],
        "tables": ["region", "region", "nation", "nation", "supplier", "supplier", "stock", "stock", "stock", "item", "item"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["order_line", "order_line", "new_order", "new_order", "customer", "customer", "orders", "orders"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["orders", "orders", "order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["region", "region", "nation", "nation", "supplier", "supplier", "stock", "stock", "order_line", "order_line", "customer", "customer", "orders", "orders"]
    },
    {
        "operators": ["TableScan", "Selection"],
        "tables": ["order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["nation", "nation", "supplier", "supplier", "stock", "stock", "orders", "orders", "order_line", "order_line", "customer", "customer"]
    },
    {
        "operators": ["TableScan", "TableReader", "TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["nation", "nation", "region", "region", "customer", "customer", "supplier", "supplier", "item", "item", "order_line", "order_line", "stock", "stock", "orders", "orders"]
    },
    {
        "operators": ["TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["orders", "orders", "nation", "nation", "supplier", "supplier", "order_line", "order_line", "item", "item", "stock", "stock"]
    },
    {
        "operators": ["TableScan", "TableReader", "TableScan", "Selection", "TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["nation", "nation", "order_line", "order_line", "orders", "orders", "customer", "customer"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader"],
        "tables": ["nation", "nation", "supplier", "supplier", "stock", "stock"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["orders", "orders", "order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["orders", "orders", "customer", "customer"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["order_line", "order_line", "item", "item"]
    },
    {
        "operators": ["TableScan", "TableReader", "TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["supplier", "supplier", "order_line", "order_line", "stock", "stock"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection", "TableScan", "TableReader"],
        "tables": ["supplier", "supplier", "item", "item", "stock", "stock"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader"],
        "tables": ["supplier", "supplier", "item", "item", "stock", "stock", "order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "Selection"],
        "tables": ["order_line", "order_line", "customer", "customer", "orders", "orders"]
    },
    {
        "operators": ["TableScan", "Selection", "TableScan", "Selection"],
        "tables": ["item", "item", "order_line", "order_line"]
    },
    {
        "operators": ["TableScan", "TableReader", "Selection", "TableScan", "TableReader", "Selection", "TableScan", "TableReader", "Selection", "TableScan", "TableReader", "Selection"],
        "tables": ["order_line", "order_line", "order_line", "supplier", "supplier", "supplier", "item", "item", "item", "nation", "nation", "nation"]
    },
    {
        "operators": ["TableScan", "TableReader", "Selection", "TableScan", "Selection", "TableScan", "TableReader", "TableScan", "TableReader", "TableScan", "Selection"],
        "tables": ["order_line", "order_line", "order_line", "nation", "nation", "supplier", "supplier", "stock", "stock", "orders", "orders"]
    },
    {
        "operators": ["TableScan", "Selection"],
        "tables": ["customer", "customer"]
    }
]
