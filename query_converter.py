from sqlglot import expressions as exp, parse_one
from neo4j_connector import Neo4jConnector

# Connection details
URI = "neo4j+s://d548bc3f.databases.neo4j.io"
USERNAME = "neo4j"
PASSWORD = "ueuvUU9i88g-viAppXOHDLkhSy3yZ2Wg7CBmiHiqXvo"

# Initialize connector
neo4j = Neo4jConnector(URI, USERNAME, PASSWORD)

class GraphSQLToCypher:
    def __init__(self, sql_query):
        self.sql_query = sql_query
        self.expression = parse_one(sql_query)

    def extract_tables(self):
        from_expr = self.expression.args.get("from")
        tables = []
        if from_expr:
            tables.append({
                "label": from_expr.this.name,
                "alias": from_expr.this.alias_or_name
            })
        return tables

    def extract_relationship_joins(self):
        joins = []
        for join in self.expression.find_all(exp.Join):
            on_expr = join.args.get("on")
            if isinstance(on_expr, exp.Anonymous) and on_expr.name.lower() == "relation":
                relation_args = on_expr.expressions
                if len(relation_args) >= 2:
                    raw_relation = relation_args[0].sql().strip().replace("'", "").replace('"', '')

                    # Replace ' OR ' / 'or' with |
                    relation_part = raw_relation.replace(" OR ", "|").replace(" or ", "|")

                    # Cypher allows depth inside [] if there is a pattern like FRIEND*1..3
                    # No extra wrapping needed, as Cypher syntax expects: :FRIEND*1..3
                    alias_name = relation_args[1].sql()

                    joins.append({
                        "relationship": relation_part,
                        "alias": alias_name,
                        "target_table": join.this.name,
                        "target_alias": join.this.alias_or_name
                    })
                else:
                    print(f"Warning: RELATION() in ON clause has fewer than 2 arguments: {relation_args}")
            else:
                print(f"Unsupported or missing ON clause format: {on_expr}")
        return joins

    def extract_where_conditions(self):
        where_expr = self.expression.args.get("where")
        return where_expr.sql() if where_expr else ""

    def extract_return_fields(self):
        select_exprs = self.expression.args.get("expressions")
        is_distinct = self.expression.args.get("distinct", False)
        fields = []
        if select_exprs:
            for expr in select_exprs:
                if isinstance(expr, exp.Alias):
                    fields.append(f"{expr.this.sql()} AS {expr.alias_or_name}")
                else:
                    fields.append(expr.sql())
        return fields, is_distinct

    def extract_order_by(self):
        order_expr = self.expression.args.get("order")
        if order_expr:
            order_items = []
            for e in order_expr.expressions:
                direction = "ASC"
                if isinstance(e, exp.Ordered):
                    direction = "DESC" if e.args.get("desc") else "ASC"
                    order_items.append(f"{e.this.sql()} {direction}")
                else:
                    order_items.append(e.sql())
            return "ORDER BY " + ", ".join(order_items)
        return ""

    def extract_limit(self):
        limit_expr = self.expression.args.get("limit")
        return limit_expr.sql() if limit_expr else ""

    def generate_cypher(self):
        tables = self.extract_tables()
        joins = self.extract_relationship_joins()
        where_clause = self.extract_where_conditions()
        return_fields, is_distinct = self.extract_return_fields()
        order_by_clause = self.extract_order_by()
        limit_clause = self.extract_limit()

        if not tables:
            return "Unsupported query: No table found."

        # Build MATCH path
        match_clause = f"({tables[0]['alias']}:{tables[0]['label']})"
        for join in joins:
            match_clause += f"-[{join['alias']}:{join['relationship']}]->({join['target_alias']}:{join['target_table']})"

        cypher_query = f"MATCH {match_clause}"
        if where_clause:
            cypher_query += f"\n{where_clause}"
        if return_fields:
            distinct_keyword = "DISTINCT " if is_distinct else ""
            cypher_query += f"\nRETURN {distinct_keyword}{', '.join(return_fields)}"
        else:
            all_aliases = [tables[0]['alias']] + [j['alias'] for j in joins] + [j['target_alias'] for j in joins]
            distinct_keyword = "DISTINCT " if is_distinct else ""
            cypher_query += f"\nRETURN {distinct_keyword}{', '.join(all_aliases)}"
        if order_by_clause:
            cypher_query += f"\n{order_by_clause}"
        if limit_clause:
            cypher_query += f"\n{limit_clause}"
        cypher_query += ";"
        return cypher_query

# === Example Usage ===

sql_query = """
SELECT DISTINCT p.name as person_name, f.name as friend_name, c.name AS company_name
FROM Person p
RIGHT JOIN Person f ON RELATION('FRIEND*3..3', _f)
RIGHT JOIN Company c ON RELATION(WORKS_AT, w)
WHERE c.name = 'ACME Corp' AND p.name != f.name
ORDER BY p.name;
"""

print("SQL: " + sql_query)

translator = GraphSQLToCypher(sql_query)
cypher_query = translator.generate_cypher()
print("Cypher:\n" + cypher_query)

neo4j.run_query(cypher_query)