package com.example;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class Main {
    public static void main(String[] args) {
        // 输入原表包含的列名，以及原表的主键列，垂直分表之后生成的两个子表的表名，各自包含的列
        String[] originalColumns = {"id", "name", "age", "address"};
        String primaryKey = "id";
        String table1 = "sub_table1";
        String[] table1Columns = {"id", "name"};
        String table2 = "sub_table2";
        String[] table2Columns = {"id", "age", "address"};

        // 示例SQL
        String sql = "SELECT id, name, age FROM original_table WHERE id = 1";

        try {
            // 解析SQL
            SqlParser parser = SqlParser.create(sql);
            SqlNode sqlNode = parser.parseQuery();

            // 重写SQL
            SqlNode rewrittenSql = rewriteSql(sqlNode, originalColumns, primaryKey, table1, table1Columns, table2, table2Columns);

            // 输出重写后的SQL
            SqlWriter writer = new SqlPrettyWriter(AnsiSqlDialect.DEFAULT);
            rewrittenSql.unparse(writer, 0, 0);
            System.out.println(writer.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SqlNode rewriteSql(SqlNode sqlNode, String[] originalColumns, String primaryKey, String table1, String[] table1Columns, String table2, String[] table2Columns) {
        // 创建列名到子表的映射
        Map<String, String> columnToTableMap = new HashMap<>();
        for (String column : table1Columns) {
            columnToTableMap.put(column, table1);
        }
        for (String column : table2Columns) {
            columnToTableMap.put(column, table2);
        }

        // 解析SELECT语句
        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNodeList selectList = select.getSelectList();
            SqlNode from = select.getFrom();

            // 重写列名
            for (int i = 0; i < selectList.size(); i++) {
                SqlNode column = selectList.get(i);
                if (column instanceof SqlIdentifier) {
                    SqlIdentifier identifier = (SqlIdentifier) column;
                    String columnName = identifier.getSimple();
                    String tableName = columnToTableMap.get(columnName);
                    if (tableName != null) {
                        selectList.set(i, new SqlIdentifier(tableName + "." + columnName, SqlParserPos.ZERO));
                    }
                }
            }

            // 重写表名
            if (from instanceof SqlIdentifier) {
                SqlIdentifier identifier = (SqlIdentifier) from;
                String tableName = identifier.getSimple();
                if (tableName.equals("original_table")) {
                    Set<String> involvedTables = new HashSet<>();
                    for (SqlNode column : selectList) {
                        if (column instanceof SqlIdentifier) {
                            SqlIdentifier id = (SqlIdentifier) column;
                            String colName = id.getSimple();
                            involvedTables.add(columnToTableMap.get(colName));
                        }
                    }

                    if (involvedTables.size() == 1) {
                        select.setFrom(new SqlIdentifier(involvedTables.iterator().next(), SqlParserPos.ZERO));
                    } else if (involvedTables.size() == 2) {
                        SqlIdentifier table1Id = new SqlIdentifier(table1, SqlParserPos.ZERO);
                        SqlIdentifier table2Id = new SqlIdentifier(table2, SqlParserPos.ZERO);
                        SqlNode joinCondition = new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[]{new SqlIdentifier(table1 + "." + primaryKey, SqlParserPos.ZERO), new SqlIdentifier(table2 + "." + primaryKey, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
                        SqlJoin join = new SqlJoin(SqlParserPos.ZERO, table1Id, SqlLiteral.createBoolean(false, SqlParserPos.ZERO), SqlLiteral.createSymbol(SqlKind.INNER_JOIN, SqlParserPos.ZERO), table2Id, joinCondition);
                        select.setFrom(join);
                    }
                }
            }
        }

        return sqlNode;
    }
}
