package com.example;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.*;

import java.io.*;

public class Main {
    public static void printSqlNodeStructure(SqlNode node, int level) {
        if (node == null) return;
    
        // 缩进以显示层级
        String indent = "  ".repeat(level);
        System.out.println(indent + node.getClass().getSimpleName() + ": " + node.toString());
    
        // 处理不同类型的 SQL 语法节点
        if (node instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) node;
            printSqlNodeStructure(select.getSelectList(), level + 1);
            printSqlNodeStructure(select.getFrom(), level + 1);
            printSqlNodeStructure(select.getWhere(), level + 1);
        } else if (node instanceof SqlBasicCall) { // 处理运算符调用
            SqlBasicCall call = (SqlBasicCall) node;
            for (SqlNode operand : call.getOperandList()) {
                printSqlNodeStructure(operand, level + 1);
            }
        } else if (node instanceof SqlJoin) { // 处理 JOIN 结构
            SqlJoin join = (SqlJoin) node;
            printSqlNodeStructure(join.getLeft(), level + 1);
            printSqlNodeStructure(join.getRight(), level + 1);
            printSqlNodeStructure(join.getCondition(), level + 1);
        } else if (node instanceof SqlOrderBy) { // 处理 ORDER BY 结构
            SqlOrderBy orderBy = (SqlOrderBy) node;
            printSqlNodeStructure(orderBy.query, level + 1);
        }
    }
    
    
    // 抽取sql的列和表, 已废弃
    public static void extractTablesAndColumns(SqlNode node, Set<String> tables, Set<String> columns) {
        if (node == null) return;

        if (node instanceof SqlIdentifier) {  // 处理表名或列名
            SqlIdentifier identifier = (SqlIdentifier) node;
            if (identifier.isStar()) return; // 忽略 COUNT(*)

            if (identifier.names.size() == 1) {
                columns.add(identifier.toString()); // 可能是列
            } else if (identifier.names.size() == 2) {
                tables.add(identifier.names.get(0));  // 表名
                columns.add(identifier.toString());   // 表.列
            }
        } else if (node instanceof SqlSelect) { // 处理 SELECT 语句
            SqlSelect select = (SqlSelect) node;
            extractTablesAndColumns(select.getSelectList(), tables, columns);
            extractTablesAndColumns(select.getFrom(), tables, columns);
            extractTablesAndColumns(select.getWhere(), tables, columns);
            extractTablesAndColumns(select.getGroup(), tables, columns); // 处理 GROUP BY
            extractTablesAndColumns(select.getOrderList(), tables, columns); // 处理 ORDER BY
        } else if (node instanceof SqlJoin) { // 处理 JOIN 语句
            SqlJoin join = (SqlJoin) node;
            extractTablesAndColumns(join.getLeft(), tables, columns);
            extractTablesAndColumns(join.getRight(), tables, columns);
            extractTablesAndColumns(join.getCondition(), tables, columns);
        } else if (node instanceof SqlBasicCall) { // 处理函数调用（SUM、AVG、COUNT）
            SqlBasicCall call = (SqlBasicCall) node;
            String operatorName = call.getOperator().getName(); // 获取函数名（SUM、AVG、COUNT）

            // 忽略 COUNT(*)，但处理 COUNT(column)
            if (operatorName.equalsIgnoreCase("COUNT") && call.operandCount() == 1) {
                if (call.operand(0) instanceof SqlIdentifier && !((SqlIdentifier) call.operand(0)).isStar()) {
                    columns.add(call.operand(0).toString()); // 仅添加 COUNT(column)
                }
            } else {
                for (SqlNode operand : call.getOperandList()) {
                    extractTablesAndColumns(operand, tables, columns);
                }
            }
        } else if (node instanceof SqlNodeList) { // 处理列列表（SELECT、ORDER BY、GROUP BY）
            SqlNodeList nodeList = (SqlNodeList) node;
            for (SqlNode sqlNode : nodeList) {
                extractTablesAndColumns(sqlNode, tables, columns);
            }
        } else if (node instanceof SqlOrderBy) { // 处理 ORDER BY 语句
            SqlOrderBy orderBy = (SqlOrderBy) node;
            extractTablesAndColumns(orderBy.query, tables, columns); // 解析 SELECT 或子查询
            extractTablesAndColumns(orderBy.orderList, tables, columns); // 解析 ORDER BY 列
        }
    }

    // 提取列名和表名
    public static Set<String> extractIdentifierSql(String originalSql, Map<String, List<String>> originalTable, Map<String, List<String>> primaryKey, Map<String, List<String>> splitTables) throws Exception {

        // 解析 SQL
        SqlParser parser = SqlParser.create(originalSql);
        SqlNode sqlNode = parser.parseQuery();

        // 创建Identifier集合
        Set<String> identifier_set = new HashSet<>(); 

        // 自定义一个访问器来遍历语法树
        SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                System.out.println("Node type: " + call.getClass().getSimpleName());
                System.out.println("Operator: " + call.getOperator().getName());
                System.out.println("Operands: " + call.getOperandList());
                // 递归访问子节点
                for (SqlNode operand : call.getOperandList()) {
                    // System.out.println(operand.toString());
                    if (operand instanceof SqlIdentifier){
                        System.out.println("SqlIdentifier: " + operand);
                        identifier_set.add(operand.toString());
                    }
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
                return null;
            }

            @Override
            public Void visit(SqlIdentifier identifier) {
                System.out.println("SqlIdentifier: " + identifier);
                identifier_set.add(identifier.toString()); // 记录列名
                return null;
            }            
        }; 

        // 调用 accept 方法进行访问
        sqlNode.accept(visitor);      

        return identifier_set;
    }  


    public static boolean containsIgnoreCase(Collection<String> collection, String target) {
        for (String item : collection) {
            if (item.equalsIgnoreCase(target)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsKeyIgnoreCase(Map<String, ?> map, String targetKey) {
        for (String key : map.keySet()) {
            if (key.equalsIgnoreCase(targetKey)) {
                return true;
            }
        }
        return false;
    }

    public static String getKeyIgnoreCase(Map<String, ?> map, String targetKey) {
        for (String key : map.keySet()) {
            if (key.equalsIgnoreCase(targetKey)) {
                return key;
            }
        }
        return null;
    }

    public static Map<String, Set<String>> findColumnsInTables(Set<String> identifierSet, Map<String, List<String>> originalTables, Map<String, List<String>> primaryKeys, Map<String, List<String>> splitTables) {
        Set<String> tables = new HashSet<>();
        Set<String> columns = new HashSet<>();

        // 针对T.column这样的identifier进行处理
        Set<String> processed_identifierSet = new HashSet<>();

        for (String identifier : identifierSet) {
            if (identifier.contains(".")) {
                String[] parts = identifier.split("\\.");
                if (parts.length == 2) {
                    // 插入.后面的真正列名
                    processed_identifierSet.add(parts[1]);
                } else {
                    System.out.println("Invalid identifier: " + identifier);
                }
            } else {
                processed_identifierSet.add(identifier);
            }
        }

        // 遍历 identifierSet，判断是表名还是列名
        for (String identifier : processed_identifierSet) {
            boolean isTable = false;
            for (Map.Entry<String, List<String>> entry : originalTables.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(identifier)) {
                    tables.add(identifier);
                    isTable = true;
                    break;
                }
            }     
            if (!isTable){
                for (Map.Entry<String, List<String>> entry : originalTables.entrySet()) {
                    if (containsIgnoreCase(entry.getValue(), identifier)) {
                        columns.add(identifier);
                        break;
                    }
                }
            }
        }

        // 输出列名
        System.out.println("Columns: " + columns);
        // 输出表名
        System.out.println("Tables: " + tables);

        // 找到所有列所在的表
        Map<String, Set<String>> columnTableMap = new HashMap<>();
        for (String column : columns) {
            boolean foundInSplitTables = false;
            for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                if (containsIgnoreCase(entry.getValue(), column)) {
                    if (primaryKeys.values().stream().anyMatch(pk -> pk.stream().anyMatch(f -> f.equalsIgnoreCase(column)))) {
                        columnTableMap.computeIfAbsent(column, k -> new HashSet<>()).add(entry.getKey());
                    } else {
                        columnTableMap.computeIfAbsent(column, k -> new HashSet<>()).add(entry.getKey());
                    }
                    foundInSplitTables = true;
                }
            }
            if (!foundInSplitTables) {
                for (Map.Entry<String, List<String>> entry : originalTables.entrySet()) {
                    if (containsIgnoreCase(entry.getValue(), column)) {
                        columnTableMap.computeIfAbsent(column, k -> new HashSet<>()).add(entry.getKey());
                    }
                }
            }
        }

        // 输出列所在的表
        for (Map.Entry<String, Set<String>> entry : columnTableMap.entrySet()) {
            System.out.println("Column: " + entry.getKey() + " is in tables: " + entry.getValue());
        }

        return columnTableMap;
    }

    public static String replaceIgnoreCase(String original, String target, String replacement) {
        return original.replaceAll("(?i)\\b" + target + "\\b", replacement);
    }

    public static String rewriteSql(String originalSql, Map<String, List<String>> originalTables, Map<String, List<String>> primaryKeys, Map<String, List<String>> splitTables) throws Exception {
        // 提取列名和表名
        Set<String> identifierSet = extractIdentifierSql(originalSql, originalTables, primaryKeys, splitTables);
        System.out.println("identifier_set: " + identifierSet);

        // 找到 column 到 splitTable 的 Map
        Map<String, Set<String>> columnTableMap = findColumnsInTables(identifierSet, originalTables, primaryKeys, splitTables);

        // 1. 维护原表到 column 的 Map
        Map<String, Set<String>> tableToColumnsMap = new HashMap<>();
        for (String column : columnTableMap.keySet()) {
            for (String originalTable : originalTables.keySet()) {
                if (containsIgnoreCase(originalTables.get(originalTable), column)) {
                    tableToColumnsMap.computeIfAbsent(originalTable, k -> new HashSet<>()).add(column);
                }
            }
        }
        System.out.println("tableToColumnsMap: " + tableToColumnsMap);

        // // 2. 排除 primaryKeys 的列
        // for (Map.Entry<String, List<String>> entry : primaryKeys.entrySet()) {
        //     String table = entry.getKey();
        //     List<String> primaryKeyColumns = entry.getValue();
        //     if (containsKeyIgnoreCase(tableToColumnsMap, table)) {
        //         tableToColumnsMap.get(getKeyIgnoreCase(tableToColumnsMap, table)).removeAll(primaryKeyColumns);
        //     }
        // }
        // System.out.println("tableToColumnsMap: " + tableToColumnsMap);

        // 3. 根据sql出现的列, 计算原表->列对应的子表 Map
        Map<String, Set<String>> tableToSplitTablesMap = new HashMap<>();
        for (String table : tableToColumnsMap.keySet()) {
            Set<String> columns = tableToColumnsMap.get(table);
            Set<String> intersectedSplitTables = new HashSet<>();
            boolean isempty = true;
            for (String column : columns) {
                // 判断是否是primary_keys, 如果是不处理
                if (primaryKeys.values().stream().anyMatch(pk -> pk.stream().anyMatch(f -> f.equalsIgnoreCase(column)))) {
                    System.out.println("column: " + column + " is primary key, skip");
                    continue;
                }
                Set<String> splitTablesForColumn = columnTableMap.get(column);
                intersectedSplitTables.addAll(splitTablesForColumn);
                isempty = false;
            }
            // isempty说明columns都是主键, 插入一个子表即可
            if (isempty) {
                intersectedSplitTables.add(columnTableMap.get(columns.iterator().next()).iterator().next());
            }
            tableToSplitTablesMap.put(table, intersectedSplitTables);
        }
        System.out.println("tableToSplitTablesMap: " + tableToSplitTablesMap);

        // 4. 替换原 SQL 的表名
        StringBuilder joinClauses = new StringBuilder();
        for (String table : tableToSplitTablesMap.keySet()) {
            Set<String> splitTablesValueSet = tableToSplitTablesMap.get(table);
            if (splitTablesValueSet.size() == 1) {  // 只对应一个子表
                String splitTable = splitTablesValueSet.iterator().next();
                originalSql = replaceIgnoreCase(originalSql, table, splitTable);
            } else if (splitTablesValueSet.size() > 1) {
                Iterator<String> iterator = splitTablesValueSet.iterator();
                String splitTable1 = iterator.next();
                String splitTable2 = iterator.next();
                String splitTable = splitTable1 + "," + splitTable2;
                originalSql = replaceIgnoreCase(originalSql, table, splitTable);
                List<String> primaryKeyColumns = primaryKeys.get(table);
                if (primaryKeyColumns != null) {
                    for (int i = 0; i < primaryKeyColumns.size(); i++) {
                        if (i > 0) joinClauses.append(" AND ");
                        joinClauses.append(splitTable1).append(".").append(primaryKeyColumns.get(i))
                                .append(" = ").append(splitTable2).append(".").append(primaryKeyColumns.get(i));
                    }
                }
            }
        }
        System.out.println("joinClauses: " + joinClauses);

        // 如果有 JOIN 子句，添加到 WHERE 或 ON 条件中
        if (joinClauses.length() > 0) {
            if (originalSql.toUpperCase().contains("WHERE")) {
                originalSql = originalSql.replaceFirst("(?i)WHERE", "WHERE " + joinClauses + " AND ");
            } else {
                originalSql += " WHERE " + joinClauses;
            }
        }

        
        // 5. 替换原 SQL 中的列名
        for (String column : columnTableMap.keySet()) {
            Set<String> splitTablesValueSet = columnTableMap.get(column);
            if (splitTablesValueSet.size() == 1) {
                String splitTable = splitTablesValueSet.iterator().next();
                originalSql = replaceIgnoreCase(originalSql, column, splitTable + "." + column);
            } else if (splitTablesValueSet.size() > 1) {

                for (String splitTable : splitTablesValueSet) {
                    originalSql = replaceIgnoreCase(originalSql, column, splitTable + "." + column);
                    break;
                    // if (originalSql.contains(splitTable + "." + column)) {
                    //     originalSql = replaceIgnoreCase(originalSql, column, splitTable + "." + column);
                    //     break;
                    // }
                }
                // System.out.println("splitTablesValueSet.size() > 1 originalSql: " + originalSql);
            } 
        }

        // 6. 返回重写后的 SQL
        return originalSql.toLowerCase();
    }

    public static void init_table(Map<String, List<String>> originalTables, Map<String, List<String>> primaryKeys) {
        originalTables.put("customer", Arrays.asList("c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"));
        originalTables.put("district", Arrays.asList("d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"));
        originalTables.put("item", Arrays.asList("i_id", "i_im_id", "i_name", "i_price", "i_data"));
        originalTables.put("new_order", Arrays.asList("no_o_id", "no_d_id", "no_w_id"));
        originalTables.put("orders", Arrays.asList("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"));
        originalTables.put("order_line", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"));
        originalTables.put("stock", Arrays.asList("s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"));
        originalTables.put("warehouse", Arrays.asList("w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"));
        originalTables.put("history", Arrays.asList("h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"));
        originalTables.put("nation", Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment"));
        originalTables.put("supplier", Arrays.asList("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"));
        originalTables.put("region", Arrays.asList("r_regionkey", "r_name", "r_comment"));

        primaryKeys.put("customer", Arrays.asList("c_id", "c_d_id", "c_w_id"));
        primaryKeys.put("district", Arrays.asList("d_id", "d_w_id"));
        primaryKeys.put("item", Arrays.asList("i_id"));
        primaryKeys.put("new_order", Arrays.asList("no_o_id", "no_d_id", "no_w_id"));
        primaryKeys.put("orders", Arrays.asList("o_id", "o_d_id", "o_w_id"));
        primaryKeys.put("order_line", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number"));
        primaryKeys.put("stock", Arrays.asList("s_i_id", "s_w_id"));
        primaryKeys.put("warehouse", Arrays.asList("w_id"));
        primaryKeys.put("history", Arrays.asList("h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date"));
        primaryKeys.put("nation", Arrays.asList("n_nationkey"));
        primaryKeys.put("supplier", Arrays.asList("s_suppkey"));
        primaryKeys.put("region", Arrays.asList("r_regionkey"));
    }

    public static void processSqlFile(String inputFilePath, String outputFilePath, Map<String, List<String>> originalTables, Map<String, List<String>> primaryKeys, Map<String, List<String>> splitTables) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {

            String sql;
            int num = 1;
            while ((sql = reader.readLine()) != null) {
                System.out.println("SQL: " + num);
                num += 1;

                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1);
                }
                if (!sql.trim().isEmpty()) {
                    try {
                        String rewrittenSql = rewriteSql(sql, originalTables, primaryKeys, splitTables);
                        writer.write(rewrittenSql);
                        writer.newLine();
                    } catch (Exception e) {
                        System.err.println("Error rewriting SQL: " + sql);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // String originalSql = "select   ol_o_id, ol_w_id, ol_d_id,sum(ol_amount) as revenue, o_entry_d from customer, new_order, orders, order_line where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d > '2024-10-28 17:00:00' group by ol_o_id, ol_w_id, ol_d_id, o_entry_d order by revenue desc, o_entry_d";
        //"select   ol_number,  sum(ol_quantity) as sum_qty,  sum(ol_amount) as sum_amount,  avg(ol_quantity) as avg_qty,  avg(ol_amount) as avg_amount,  count(*) as count_order from order_line where ol_delivery_d > '2024-10-28 17:00:00' group by ol_number order by ol_number";
        // "select   ol_o_id, ol_w_id, ol_d_id,sum(ol_amount) as revenue, o_entry_d from customer, new_order, orders, order_line where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d > '2024-10-28 17:00:00' group by ol_o_id, ol_w_id, ol_d_id, o_entry_d order by revenue desc, o_entry_d";
        Map<String, List<String>> originalTables = new HashMap<>();
        Map<String, List<String>> primaryKeys = new HashMap<>();
        init_table(originalTables, primaryKeys);

        Map<String, List<String>> splitTables = new HashMap<>();
        splitTables.put("order_line_part1", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity"));
        splitTables.put("order_line_part2", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_amount", "ol_dist_info"));

        // String rewrittenSql = rewriteSql(originalSql, originalTables, primaryKeys, splitTables);
        // System.out.println("Rewritten SQL: " + rewrittenSql);


        String inputFilePath = "/data3/dzh/project/grep/dev/workload/workloadd.sql";
        String outputFilePath = "/data3/dzh/project/grep/dev/workload/workloadd_rewrite_0319.sql";
        processSqlFile(inputFilePath, outputFilePath, originalTables, primaryKeys, splitTables);
    }
}