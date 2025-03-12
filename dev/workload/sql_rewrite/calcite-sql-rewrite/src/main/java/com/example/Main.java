package com.example;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlCall;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;

public class Main {

    public String rewriteSQL(String originalSQL, Map<String, List<String>> originalTableColumns, 
                             Map<String, String> primaryKeyInfo, Map<String, String> shardedTables, 
                             Map<String, List<String>> shardedTableColumns) throws SqlParseException {
        SqlParser parser = SqlParser.create(originalSQL);
        SqlNode sqlNode = parser.parseQuery();

        SqlRewriterVisitor visitor = new SqlRewriterVisitor(originalTableColumns, primaryKeyInfo, shardedTables, shardedTableColumns);
        sqlNode.accept(visitor);

        return sqlNode.toString();
    }

    private static class SqlRewriterVisitor implements SqlVisitor<Void> {
        private final Map<String, List<String>> originalTableColumns;
        private final Map<String, String> primaryKeyInfo;
        private final Map<String, String> shardedTables;
        private final Map<String, List<String>> shardedTableColumns;

        public SqlRewriterVisitor(Map<String, List<String>> originalTableColumns, Map<String, String> primaryKeyInfo, 
                                  Map<String, String> shardedTables, Map<String, List<String>> shardedTableColumns) {
            this.originalTableColumns = originalTableColumns;
            this.primaryKeyInfo = primaryKeyInfo;
            this.shardedTables = shardedTables;
            this.shardedTableColumns = shardedTableColumns;
        }

        // @Override
        // public Void visit(SqlSelect select) {
        //     // ...existing code...
        //     select.getFrom().accept(this);
        //     // ...existing code...
        //     return null;
        // }

        @Override
        public Void visit(SqlIdentifier identifier) {
            // ...existing code...
            String tableName = identifier.names.get(0);
            if (shardedTables.containsKey(tableName)) {
                identifier.setName(0, shardedTables.get(tableName));
            }
            // ...existing code...
            return null;
        }

        // @Override
        // public Void visit(SqlBasicCall call) {
        //     // ...existing code...
        //     if (call.getKind() == SqlKind.AS) {
        //         call.getOperandList().get(0).accept(this);
        //     }
        //     // ...existing code...
        //     return null;
        // }

        @Override
        public Void visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }

        @Override
        public Void visit(SqlDynamicParam dynamicParam) {
            return null;
        }

        @Override
        public Void visit(SqlDataTypeSpec dataTypeSpec) {
            return null;
        }

        @Override
        public Void visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public Void visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public Void visit(SqlCall call) {
            return null;
        }

        @Override
        public Void visitNode(SqlNode n) {
            return null;
        }

        // ...existing code...
    }

    public static void main(String[] args) {
        // 示例用法
        String originalSQL = "SELECT * FROM original_table WHERE id = 1";
        Map<String, List<String>> originalTableColumns = new HashMap<String, List<String>>();
        originalTableColumns.put("original_table", Arrays.asList("id", "name", "value"));
        Map<String, String> primaryKeyInfo = new HashMap<String, String>();
        primaryKeyInfo.put("original_table", "id");
        Map<String, String> shardedTables = new HashMap<String, String>();
        shardedTables.put("original_table", "sharded_table_1");
        Map<String, List<String>> shardedTableColumns = new HashMap<String, List<String>>();
        shardedTableColumns.put("sharded_table_1", Arrays.asList("id", "name"));
        
        Main main = new Main();
        try {
            String rewrittenSQL = main.rewriteSQL(originalSQL, originalTableColumns, primaryKeyInfo, shardedTables, shardedTableColumns);
            System.out.println("Rewritten SQL: " + rewrittenSQL);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }
}
