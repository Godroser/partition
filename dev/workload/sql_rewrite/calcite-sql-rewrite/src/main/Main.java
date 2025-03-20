package com.example;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import java.util.*;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.*;

public class Main {
    public static String rewriteSql(String originalSql, String originalTable, List<String> primaryKey, Map<String, List<String>> splitTables) throws Exception {
        // 解析 SQL
        SqlParser parser = SqlParser.create(originalSql);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode);

        // 自定义一个访问器来遍历语法树
        SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                // System.out.println("Node type: " + call.getClass().getSimpleName());
                System.out.println("Operator: " + call.getOperator().getName());
                // System.out.println("Operands: " + call.getOperandList());
                // 递归访问子节点
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
                return null;
            }
        }; 
        // 调用 accept 方法进行访问
        sqlNode.accept(visitor);       
      

        // Class<? extends SqlNode> nodeClass = sqlNode.getClass();
        // if (nodeClass == SqlSelect.class) {
        //     System.out.println("The SqlNode is a SqlSelect.");
        // } else if (nodeClass == SqlInsert.class) {
        //     System.out.println("The SqlNode is a SqlInsert.");
        // } else if (nodeClass == SqlDelete.class) {
        //     System.out.println("The SqlNode is a SqlDelete.");
        // } else {
        //     System.out.println("The SqlNode is of other type: " + nodeClass.getSimpleName());
        // }                    

        
        
        // 解析 SELECT 字段        
        List<String> selectFields = new ArrayList<>();

        // 自定义一个访问器来遍历语法树
        SqlBasicVisitor<Void> select_parse_visitor = new SqlBasicVisitor<Void>() {
            @Override    
            public Void visit(SqlCall call) {
                if (call instanceof SqlSelect) {
                    System.out.println("sqlNode is SqlSelect");
                    SqlSelect sqlSelect = (SqlSelect) call;
                    sqlSelect.getSelectList().accept(new SqlShuttle() {
                        @Override
                        public SqlNode visit(SqlCall call) {
                            if (call.getOperator().getName().equalsIgnoreCase("AS")) {
                                SqlNode left = call.getOperandList().get(0);
                                SqlNode right = call.getOperandList().get(1);
                                if (left instanceof SqlCall) {
                                    SqlCall leftCall = (SqlCall) left;
                                    String functionName = leftCall.getOperator().getName();
                                    String fieldName = leftCall.getOperandList().get(0).toString();
                                    for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                        if (entry.getValue().contains(fieldName)) {
                                            selectFields.add(functionName + "(" + entry.getKey() + "." + fieldName + ") AS " + right.toString());
                                            break;
                                        }
                                    }
                                }
                            } else if (call.getOperator().getName().matches("SUM|AVG|COUNT|MIN|MAX")) {
                                String functionName = call.getOperator().getName();
                                String fieldName = call.getOperandList().get(0).toString();
                                for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                    if (entry.getValue().contains(fieldName)) {
                                        selectFields.add(functionName + "(" + entry.getKey() + "." + fieldName + ")");
                                        break;
                                    }
                                }
                            } else {
                                String fieldName = call.toString();
                                for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                    if (entry.getValue().contains(fieldName)) {
                                        selectFields.add(entry.getKey() + "." + fieldName);
                                        break;
                                    }
                                }
                            }
                            return super.visit(call);
                        }

                        @Override
                        public SqlNode visit(SqlIdentifier id) {
                            String fieldName = id.toString();
                            for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                if (entry.getValue().contains(fieldName)) {
                                    selectFields.add(entry.getKey() + "." + fieldName);
                                    break;
                                }
                            }
                            return super.visit(id);
                        }
                    });
                }    
                // 递归访问子节点
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
                return null;
            }
        }; 
        // 调用 accept 方法进行访问
        sqlNode.accept(select_parse_visitor);           

        System.out.println("selectFields: ");
        System.out.println(selectFields);

        // 生成 SELECT 字段
        List<String> mappedFields = new ArrayList<>(selectFields);
        System.out.println(mappedFields);

        // 选择FROM表, 认选择第一个表
        String fromTable = splitTables.get("order_line_part1").get(0);
        

        // 生成 JOIN 语句
        List<String> joinClauses = new ArrayList<>();
        for (String table : splitTables.keySet()) {
            if (!table.equals(fromTable)) {
                joinClauses.add("JOIN " + table + " ON ");
                for (int i = 0; i < primaryKey.size(); i++) {
                    joinClauses.add(fromTable + "." + primaryKey.get(i) + " = " + table + "." + primaryKey.get(i));
                    if (i < primaryKey.size() - 1) {
                        joinClauses.add(" AND ");
                    }
                }
            }
        }

        // 提取 WHERE 子句（如果有的话）
        String whereClause = "";
        if (originalSql.toUpperCase().contains("WHERE")) {
            whereClause = originalSql.substring(originalSql.toUpperCase().indexOf("WHERE"));
            for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                for (String field : entry.getValue()) {
                    whereClause = whereClause.replaceAll("\\b" + field + "\\b", entry.getKey() + "." + field);
                }
            }
        }

        // 生成最终 SQL
        StringBuilder newSql = new StringBuilder("SELECT ");
        newSql.append(String.join(", ", mappedFields));
        newSql.append(" FROM ").append(fromTable);
        if (!joinClauses.isEmpty()) {
            newSql.append(" ").append(String.join(" ", joinClauses));
        }
        if (!whereClause.isEmpty()) {
            newSql.append(" ").append(whereClause);
        }

        return newSql.toString();
    }

    public static void main(String[] args) throws Exception {
        // String originalSql = "SELECT id, name, age FROM users WHERE age > 18 and name = 'Alice'";
        // String originalTable = "users";
        // String primaryKey = "id";

        // Map<String, List<String>> splitTables = new HashMap<>();
        // splitTables.put("users_info", Arrays.asList("id", "name"));
        // splitTables.put("users_detail", Arrays.asList("id", "age"));


        String originalSql = "select   ol_number,  sum(ol_quantity) as sum_qty,  sum(ol_amount) as sum_amount,  avg(ol_quantity) as avg_qty,  avg(ol_amount) as avg_amount,  count(*) as count_order from order_line where ol_delivery_d > '2024-10-28 17:00:00' group by ol_number order by ol_number";
        String originalTable = "order_line";
        List<String> primaryKey = Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number");
        // ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"]
        Map<String, List<String>> splitTables = new HashMap<>();
        splitTables.put("order_line_part1", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity"));
        splitTables.put("order_line_part2", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_amount", "ol_dist_info"));        

        String rewrittenSql = rewriteSql(originalSql, originalTable, primaryKey, splitTables);
        System.out.println("Rewritten SQL: " + rewrittenSql);
    }
}
