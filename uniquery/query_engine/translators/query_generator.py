class QueryGenerator:

    def get_mongodb_query(self, parsed_data: dict):
        tables = parsed_data["tables"]
        joins = parsed_data["joins"]
        where_clause = parsed_data["where_clause"]
        return_fields = parsed_data["return_fields"]
        is_distinct = parsed_data["is_distinct"]
        order_by_clause = parsed_data["order_by_clause"]
        limit_clause = parsed_data["limit_clause"]

        if not tables:
            return "Unsupported query: No collection found."

        # Basic query structure
        query = {
            "collection": tables[0]["label"],
            "operation": "find" if not joins else "aggregate",
            "filter": {}
        }

        # Handle where clause
        if where_clause:
            # Strip "WHERE" and parse the condition
            condition = where_clause.replace("WHERE ", "").strip()

            # Basic parser for simple binary conditions like "age >= 22"
            import re
            match = re.match(r"(\w+)\s*(>=|<=|!=|=|<|>)\s*(.+)", condition)
            if match:
                field, operator, value = match.groups()
                operator_map = {
                    "=": "$eq",
                    "!=": "$ne",
                    ">": "$gt",
                    "<": "$lt",
                    ">=": "$gte",
                    "<=": "$lte"
                }
                mongo_operator = operator_map.get(operator)
                if mongo_operator:
                    try:
                        # Convert numeric values
                        value = int(value)
                    except ValueError:
                        pass
                    query["filter"] = {field: {mongo_operator: value}}
            else:
                query["filter"] = {"$expr": {"$eq": [True, condition]}}  # fallback

        # Handle aggregation pipeline for joins
        if joins:
            pipeline = []
            for join in joins:
                lookup_stage = {
                    "$lookup": {
                        "from": join["target_table"],
                        "localField": join["relationship"],
                        "foreignField": "_id",
                        "as": join["target_alias"]
                    }
                }
                pipeline.append(lookup_stage)

            if where_clause:
                pipeline.append({"$match": query["filter"]})

            if return_fields:
                project_fields = {field: 1 for field in return_fields}
                pipeline.append({"$project": project_fields})

            if is_distinct:
                pipeline.append({"$group": {"_id": "$" + return_fields[0] if return_fields else "$_id"}})

            if order_by_clause:
                sort_fields = {}
                sort_parts = order_by_clause.replace("ORDER BY ", "").split(", ")
                for part in sort_parts:
                    field = part.split()[0]
                    direction = -1 if "DESC" in part else 1
                    sort_fields[field] = direction
                pipeline.append({"$sort": sort_fields})

            if limit_clause:
                limit = int(limit_clause.replace("LIMIT ", ""))
                pipeline.append({"$limit": limit})

            query["pipeline"] = pipeline

        return query

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
