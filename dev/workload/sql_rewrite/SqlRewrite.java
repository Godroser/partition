import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.util.SqlShuttle;
import java.util.*;

public class SqlRewrite {
    public static String rewriteSql(String originalSql, String originalTable, String primaryKey, Map<String, List<String>> splitTables) throws Exception {
        // 解析 SQL
        SqlParser parser = SqlParser.create(originalSql);
        SqlNode sqlNode = parser.parseQuery();

        // 解析 SELECT 字段
        List<String> selectFields = new ArrayList<>();
        sqlNode.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlIdentifier id) {
                selectFields.add(id.toString());
                return super.visit(id);
            }
        });

        // 生成新 SQL
        StringBuilder newSql = new StringBuilder("SELECT ");
        List<String> mappedFields = new ArrayList<>();
        for (String field : selectFields) {
            for (Map.Entry<String, List<String>> entry : splitTables.entrySet()) {
                if (entry.getValue().contains(field)) {
                    mappedFields.add(entry.getKey() + "." + field);
                    break;
                }
            }
        }
        newSql.append(String.join(", ", mappedFields));
        newSql.append(" FROM ").append(originalTable);

        // 生成 JOIN 语句
        List<String> joinClauses = new ArrayList<>();
        for (String table : splitTables.keySet()) {
            if (!table.equals(originalTable)) {
                joinClauses.add("JOIN " + table + " ON " + originalTable + "." + primaryKey + " = " + table + "." + primaryKey);
            }
        }
        if (!joinClauses.isEmpty()) {
            newSql.append(" ").append(String.join(" ", joinClauses));
        }

        return newSql.toString();
    }

    public static void main(String[] args) throws Exception {
        String originalSql = "SELECT id, name, age FROM users WHERE age > 18";
        String originalTable = "users";
        String primaryKey = "id";

        Map<String, List<String>> splitTables = new HashMap<>();
        splitTables.put("users_info", Arrays.asList("id", "name"));
        splitTables.put("users_detail", Arrays.asList("id", "age"));

        String rewrittenSql = rewriteSql(originalSql, originalTable, primaryKey, splitTables);
        System.out.println("Rewritten SQL: " + rewrittenSql);
    }
}
