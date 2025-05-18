class QueryGenerator:

    def get_cypher_query(self, parsed_data: dict):
        tables = parsed_data["tables"]
        joins = parsed_data["joins"]
        where_clause = parsed_data["where_clause"]
        return_fields = parsed_data["return_fields"]
        is_distinct = parsed_data["is_distinct"]
        order_by_clause = parsed_data["order_by_clause"]
        limit_clause = parsed_data["limit_clause"]

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