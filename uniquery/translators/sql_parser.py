from sqlglot import expressions as exp, parse_one

class SqlParser:

    def extract_tables(self, expression):
        from_expr = expression.args.get("from")
        tables = []
        if from_expr:
            tables.append({
                "label": from_expr.this.name,
                "alias": from_expr.this.alias_or_name
            })
        return tables


    def extract_relationship_joins(self, expression):
        joins = []
        for join in expression.find_all(exp.Join):
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


    def extract_where_conditions(self, expression):
        where_expr = expression.args.get("where")
        return where_expr.sql() if where_expr else ""


    def extract_return_fields(self, expression):
        select_exprs = expression.args.get("expressions")
        is_distinct = expression.args.get("distinct", False)
        fields = []
        if select_exprs:
            for expr in select_exprs:
                if isinstance(expr, exp.Alias):
                    fields.append(f"{expr.this.sql()} AS {expr.alias_or_name}")
                else:
                    fields.append(expr.sql())
        return fields, is_distinct


    def extract_order_by(self, expression):
        order_expr = expression.args.get("order")
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


    def extract_limit(self, expression):
        limit_expr = expression.args.get("limit")
        return limit_expr.sql() if limit_expr else ""


    def parse(self, sql_query):
        expression = parse_one(sql_query)
        tables = self.extract_tables(expression)
        joins = self.extract_relationship_joins(expression)
        where_clause = self.extract_where_conditions(expression)
        return_fields, is_distinct = self.extract_return_fields(expression)
        order_by_clause = self.extract_order_by(expression)
        limit_clause = self.extract_limit(expression)
        return {
            "tables": tables,
            "joins": joins,
            "where_clause": where_clause,
            "return_fields": return_fields,
            "is_distinct": is_distinct,
            "order_by_clause": order_by_clause,
            "limit_clause": limit_clause
        }