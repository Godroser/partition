import sqlparse
from sqlparse.sql import Identifier, IdentifierList, TokenList, Parenthesis
from sqlparse.tokens import Keyword, DML, Whitespace

def parse_sql_file(file_path):
    """Read and parse SQL statements from the workload.sql file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    statements = sqlparse.split(sql_content)
    parsed_statements = [sqlparse.parse(stmt)[0] for stmt in statements]
    return parsed_statements

def extract_columns(token_list):
    """Extract column names from SQL token list."""
    columns = []
    for token in token_list:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                columns.append(str(identifier))
        elif isinstance(token, Identifier):
            columns.append(str(token))
    return columns

def extract_table(token_list):
    """Extract table names from SQL token list."""
    tables = []
    from_seen = False
    for token in token_list:
        if from_seen and isinstance(token, Identifier):
            tables.append(str(token))
        elif token.ttype is Keyword and token.value.upper() == "FROM":
            from_seen = True
    return tables

def extract_conditions(token_list):
    """Extract predicate columns from WHERE conditions."""
    where_seen = False
    conditions = []
    for token in token_list:
        if where_seen:
            if isinstance(token, IdentifierList):
                conditions.extend([str(identifier) for identifier in token.get_identifiers()])
            elif isinstance(token, Identifier):
                conditions.append(str(token))
            elif token.is_group:
                conditions.extend(extract_conditions(token.tokens))
        elif token.ttype is Keyword and token.value.upper() == "WHERE":
            where_seen = True
    return conditions

def extract_order_by(token_list):
    """Extract columns from ORDER BY clause."""
    order_by_seen = False
    order_columns = []
    for token in token_list:
        if order_by_seen:
            if isinstance(token, IdentifierList):
                order_columns.extend([str(identifier) for identifier in token.get_identifiers()])
            elif isinstance(token, Identifier):
                order_columns.append(str(token))
        elif token.ttype is Keyword and token.value.upper() == "ORDER BY":
            order_by_seen = True
    return order_columns

def extract_group_by(token_list):
    """Extract columns from GROUP BY clause."""
    group_by_seen = False
    group_columns = []
    for token in token_list:
        if group_by_seen:
            if isinstance(token, IdentifierList):
                group_columns.extend([str(identifier) for identifier in token.get_identifiers()])
            elif isinstance(token, Identifier):
                group_columns.append(str(token))
        elif token.ttype is Keyword and token.value.upper() == "GROUP BY":
            group_by_seen = True
    return group_columns

def parse_sql_statement(statement):
    """Parse individual SQL statement to extract details."""
    if statement.token_first().ttype != DML or statement.token_first().value.upper() != "SELECT":
        return None  # Skip non-SELECT statements

    token_list = statement.tokens
    projection = extract_columns(token_list)
    tables = extract_table(token_list)
    predicates = extract_conditions(token_list)
    order_by = extract_order_by(token_list)
    group_by = extract_group_by(token_list)

    return {
        "Projection": projection,
        "Tables": tables,
        "Predicates": predicates,
        "OrderBy": order_by,
        "GroupBy": group_by,
    }

def parse_subqueries(token_list, results):
    """Iteratively parse subqueries in the SQL."""
    for token in token_list:
        if isinstance(token, Parenthesis):  # Detect subquery
            subquery_tokens = token.tokens
            for sub_token in subquery_tokens:
                if sub_token.ttype == DML and sub_token.value.upper() == "SELECT":
                    subquery_result = parse_sql_statement(sqlparse.parse(str(token))[0])
                    if subquery_result:
                        results.append(subquery_result)
        elif token.is_group:
            parse_subqueries(token.tokens, results)

def process_sql_file(file_path):
    """Process the SQL file and output results."""
    parsed_statements = parse_sql_file(file_path)
    all_results = []

    for statement in parsed_statements:
        if statement.token_first().ttype == DML and statement.token_first().value.upper() == "SELECT":
            main_result = parse_sql_statement(statement)
            if main_result:
                all_results.append(main_result)

            # Handle subqueries
            subquery_results = []
            parse_subqueries(statement.tokens, subquery_results)
            all_results.extend(subquery_results)

    return all_results

def write_results_to_file(results, output_file):
    """Write extracted SQL details to a file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        for i, result in enumerate(results, start=1):
            f.write(f"Statement {i}:\n")
            for key, value in result.items():
                f.write(f"  {key}: {value}\n")
            f.write("\n")
    print(f"Results written to {output_file}")

if __name__ == "__main__":
    sql_file_path = "workload/workload.bak.sql"  # Input SQL file
    output_file_path = "parsed_sql_results.txt"  # Output file

    # Parse SQL and extract details
    results = process_sql_file(sql_file_path)

    # Write results to output file
    write_results_to_file(results, output_file_path)
