// 

//***************************************************** */

// package com.example;
// import org.apache.calcite.sql.*;
// import org.apache.calcite.sql.parser.*;
// import org.apache.calcite.sql.util.SqlBasicVisitor;
// import org.apache.calcite.sql.util.SqlShuttle;
// import java.util.*;

// import org.apache.calcite.sql.*;
// import org.apache.calcite.sql.parser.SqlParser;
// import org.apache.calcite.sql.util.SqlShuttle;

// import java.util.*;

// public class Main {
//     public static String rewriteSql(String originalSql, String originalTable, List<String> primaryKey, Map<String, List<String>> splitTables) throws Exception {
//         // 解析 SQL
//         SqlParser parser = SqlParser.create(originalSql);
//         SqlNode sqlNode = parser.parseQuery();
//         System.out.println(sqlNode);

//         // 自定义一个访问器来遍历语法树
//         SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
//             @Override
//             public Void visit(SqlCall call) {
//                 // System.out.println("Node type: " + call.getClass().getSimpleName());
//                 System.out.println("Operator: " + call.getOperator().getName());
//                 // System.out.println("Operands: " + call.getOperandList());
//                 // 递归访问子节点
//                 for (SqlNode operand : call.getOperandList()) {
//                     if (operand != null) {
//                         operand.accept(this);
//                     }
//                 }
//                 return null;
//             }
//         }; 
//         // 调用 accept 方法进行访问
//         sqlNode.accept(visitor);       
      

//         // Class<? extends SqlNode> nodeClass = sqlNode.getClass();
//         // if (nodeClass == SqlSelect.class) {
//         //     System.out.println("The SqlNode is a SqlSelect.");
//         // } else if (nodeClass == SqlInsert.class) {
//         //     System.out.println("The SqlNode is a SqlInsert.");
//         // } else if (nodeClass == SqlDelete.class) {
//         //     System.out.println("The SqlNode is a SqlDelete.");
//         // } else {
//         //     System.out.println("The SqlNode is of other type: " + nodeClass.getSimpleName());
//         // }                    

        
        
//         // 解析 SELECT 字段        
//         List<String> selectFields = new ArrayList<>();

//         // 自定义一个访问器来遍历语法树
//         SqlBasicVisitor<Void> select_parse_visitor = new SqlBasicVisitor<Void>() {
//             @Override    
//             public Void visit(SqlCall call) {
//                 if (call instanceof SqlSelect) {
//                     System.out.println("sqlNode is SqlSelect");
//                     SqlSelect sqlSelect = (SqlSelect) call;
//                     sqlSelect.getSelectList().accept(new SqlShuttle() {
//                         @Override
//                         public SqlNode visit(SqlIdentifier id) {
//                             selectFields.add(id.toString());
//                             return super.visit(id);
//                         }
//                     });
//                 }    
//                 // 递归访问子节点
//                 for (SqlNode operand : call.getOperandList()) {
//                     if (operand != null) {
//                         operand.accept(this);
//                     }
//                 }
//                 return null;
//             }
//         }; 
//         // 调用 accept 方法进行访问
//         sqlNode.accept(select_parse_visitor);           




//         // sqlNode.accept(new SqlShuttle() {
//         //     @Override
//         //     public SqlNode visit(SqlIdentifier id) {
//         //         // 仅添加 SELECT 字段
//         //         if (id.getComponent(0).names.size() == 1) {
//         //             selectFields.add(id.toString());
//         //         }
//         //         return super.visit(id);
//         //     }
//         // });
//         System.out.println("selectFields: ");
//         System.out.println(selectFields);

//         // for (Map.Entry<String, List<String>> entry : splitTables.entrySet()){
//         //     System.out.println(entry.getKey());
//         // }
        
//         // 生成 SELECT 字段
//         List<String> mappedFields = new ArrayList<>();
//         for (String field : selectFields) {
//             boolean fieldMapped = false;
//             for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) { // 遍历split_table
//                 for (String f : entry.getValue()) { // 遍历split_table的列
//                     if (f.equalsIgnoreCase(field)) {
//                         // System.out.println(field);
//                         mappedFields.add(entry.getKey() + "." + field);
//                         fieldMapped = true;
//                         break;
//                     }
//                 }
//                 if (fieldMapped == true) {
//                     break;
//                 }
//             }
//             if (!fieldMapped) {
//                 // System.out.println("1");
//                 mappedFields.add(field); // 如果字段没有找到对应的表，保持原样
//             }
//         }
//         System.out.println(mappedFields);

//         // 选择FROM表, 认选择第一个表
//         String fromTable = splitTables.get("order_line_part1").get(0);
        

//         // 生成 JOIN 语句
//         List<String> joinClauses = new ArrayList<>();
//         for (String table : splitTables.keySet()) {
//             if (!table.equals(fromTable)) {
//                 joinClauses.add("JOIN " + table + " ON ");
//                 for (int i = 0; i < primaryKey.size(); i++) {
//                     joinClauses.add(fromTable + "." + primaryKey.get(i) + " = " + table + "." + primaryKey.get(i));
//                     if (i < primaryKey.size() - 1) {
//                         joinClauses.add(" AND ");
//                     }
//                 }
//             }
//         }

//         // 提取 WHERE 子句（如果有的话）
//         String whereClause = "";
//         if (originalSql.toUpperCase().contains("WHERE")) {
//             whereClause = originalSql.substring(originalSql.toUpperCase().indexOf("WHERE"));
//             for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
//                 for (String field : entry.getValue()) {
//                     whereClause = whereClause.replaceAll("\\b" + field + "\\b", entry.getKey() + "." + field);
//                 }
//             }
//         }

//         // 生成最终 SQL
//         StringBuilder newSql = new StringBuilder("SELECT ");
//         newSql.append(String.join(", ", mappedFields));
//         newSql.append(" FROM ").append(fromTable);
//         if (!joinClauses.isEmpty()) {
//             newSql.append(" ").append(String.join(" ", joinClauses));
//         }
//         if (!whereClause.isEmpty()) {
//             newSql.append(" ").append(whereClause);
//         }

//         return newSql.toString();
//     }

//     public static void main(String[] args) throws Exception {
//         // String originalSql = "SELECT id, name, age FROM users WHERE age > 18 and name = 'Alice'";
//         // String originalTable = "users";
//         // String primaryKey = "id";

//         // Map<String, List<String>> splitTables = new HashMap<>();
//         // splitTables.put("users_info", Arrays.asList("id", "name"));
//         // splitTables.put("users_detail", Arrays.asList("id", "age"));


//         String originalSql = "select   ol_number,  sum(ol_quantity) as sum_qty,  sum(ol_amount) as sum_amount,  avg(ol_quantity) as avg_qty,  avg(ol_amount) as avg_amount,  count(*) as count_order from order_line where ol_delivery_d > '2024-10-28 17:00:00' group by ol_number order by ol_number";
//         String originalTable = "order_line";
//         List<String> primaryKey = Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number");
//         // ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"]
//         Map<String, List<String>> splitTables = new HashMap<>();
//         splitTables.put("order_line_part1", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity"));
//         splitTables.put("order_line_part2", Arrays.asList("ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_amount", "ol_dist_info"));        

//         String rewrittenSql = rewriteSql(originalSql, originalTable, primaryKey, splitTables);
//         System.out.println("Rewritten SQL: " + rewrittenSql);
//     }
// }
// ****************************************************************************

package com.example;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import java.util.*;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.*;

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

    


    public static String rewriteSql(String originalSql, String originalTable, List<String> primaryKey, Map<String, List<String>> splitTables) throws Exception {
        // 解析 SQL
        SqlParser parser = SqlParser.create(originalSql);
        SqlNode sqlNode = parser.parseQuery();
        // System.out.println(sqlNode);
        printSqlNodeStructure(sqlNode, 0);

        // // 存储表和列
        // Set<String> tables = new HashSet<>();
        // Set<String> columns = new HashSet<>();
        // extractTablesAndColumns(sqlNode, tables, columns);    
        // System.out.println("Tables: " + tables);
        // System.out.println("Columns: " + columns);

        // 自定义一个访问器来遍历语法树
        SqlBasicVisitor<Void> visitor = new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                System.out.println("Node type: " + call.getClass().getSimpleName());
                System.out.println("Operator: " + call.getOperator().getName());
                System.out.println("Operands: " + call.getOperandList());
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
                            // System.out.println("Operator: " + call.getOperator().getName());
                            if (call.getOperator().getName().equalsIgnoreCase("AS")) {
                                System.out.println("AS Operator: " + call.getOperator().getName());
                                SqlNode left = call.getOperandList().get(0);
                                SqlNode right = call.getOperandList().get(1);
                                if (left instanceof SqlCall) {
                                    SqlCall leftCall = (SqlCall) left;
                                    String functionName = leftCall.getOperator().getName();
                                    String fieldName = leftCall.getOperandList().get(0).toString();
                                    // System.out.println("fieldName: " + fieldName);

                                    if (fieldName.equals("*")) {
                                        if (!selectFields.contains(functionName + "(*)")) {
                                            selectFields.add(functionName + "(*) AS " + right.toString());
                                        }
                                    } else {
                                        boolean fieldMapped = false;
                                        for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                            for (String f : entry.getValue()) { 
                                                if (f.equalsIgnoreCase(fieldName)) {
                                                    selectFields.add(functionName + "(" + entry.getKey() + "." + fieldName + ") AS " + right.toString());
                                                    fieldMapped = true;
                                                    break;
                                                }
                                            }
                                            if (fieldMapped == true) {
                                                break;
                                            }
                                        }
                                    }
                                }
                                // return null; // 不再处理子节点
                            } else if (call.getOperator().getName().matches("SUM|AVG|COUNT|MIN|MAX")) {
                                System.out.println("Agg Operator: " + call.getOperator().getName());
                                String functionName = call.getOperator().getName();
                                String fieldName = call.getOperandList().get(0).toString();

                                if (fieldName.equals("*")) {
                                    if (!selectFields.contains(functionName + "(*)")) {
                                        selectFields.add(functionName + "(*)");
                                    }
                                } else {
                                    boolean fieldMapped = false;
                                    for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                        for (String f : entry.getValue()) { 
                                            if (f.equalsIgnoreCase(fieldName)) {
                                                if (!selectFields.contains(functionName + "(" + entry.getKey() + "." + fieldName + ")")) {
                                                    selectFields.add(functionName + "(" + entry.getKey() + "." + fieldName + ")");
                                                }
                                                fieldMapped = true;
                                                break;
                                            }
                                        }
                                        if (fieldMapped == true) {
                                            break;
                                        }                                    
                                    }
                                }
                            } else {
                                String fieldName = call.toString();
                                System.out.println("else operator: " + call.getOperator().getName());

                                boolean fieldMapped = false;
                                for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                    for (String f : entry.getValue()) { 
                                        if (f.equalsIgnoreCase(fieldName)) {
                                            if (!selectFields.contains(entry.getKey() + "." + fieldName)) {
                                                selectFields.add(entry.getKey() + "." + fieldName);
                                            }
                                            fieldMapped = true;
                                            break;
                                        }
                                    }
                                    if (fieldMapped == true) {
                                        break;
                                    }                                        
                                }
                            }
                            return super.visit(call);
                        }

                        @Override
                        public SqlNode visit(SqlIdentifier id) {
                            String fieldName = id.toString();
                            System.out.println("fieldName: " + fieldName);

                            boolean fieldMapped = false;
                            for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                                for (String f : entry.getValue()) { 
                                    if (f.equalsIgnoreCase(fieldName)){
                                        if (!selectFields.contains(entry.getKey() + "." + fieldName)) {
                                            selectFields.add(entry.getKey() + "." + fieldName);
                                        }
                                        fieldMapped = true;
                                        break;
                                    }
                                }
                                if (fieldMapped == true) {
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