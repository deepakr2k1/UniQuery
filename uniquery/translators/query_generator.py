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
            # Strip "WHERE" and convert to dict format
            filter_condition = where_clause.replace("WHERE ", "")

            # Handle common operators
            operators = {
                "==": "$eq",
                "!=": "$ne",
                ">": "$gt",
                "<": "$lt",
                ">=": "$gte",
                "<=": "$lte",
                "IN": "$in",
                "NOT IN": "$nin",
                "LIKE": "$regex",
                "AND": "$and",
                "OR": "$or",
                "CONTAINS": "$in",
                "STARTS WITH": {"$regex": "^"},
                "ENDS WITH": {"$regex": "$"},
                "BETWEEN": "$and",
                "IS NULL": {"$exists": False},
                "IS NOT NULL": {"$exists": True}
            }
            # Replace SQL operators with MongoDB operators
            for sql_op, mongo_op in operators.items():
                if isinstance(mongo_op, str):
                    filter_condition = filter_condition.replace(sql_op, mongo_op)
                elif isinstance(mongo_op, dict):
                    # Handle special cases that need full operator replacement
                    if sql_op in filter_condition:
                        field = filter_condition.split(sql_op)[0].strip()
                        filter_condition = {field: mongo_op}

            # Handle special LIKE patterns
            if "$regex" in filter_condition:
                filter_condition = filter_condition.replace("%", ".*")
                filter_condition = filter_condition.replace("_", ".")

            query["filter"] = {"$expr": {"$eq": [True, filter_condition]}}

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
