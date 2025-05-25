import re

def get_mongodb_query(parsed_data: dict):
    tables = parsed_data.get("tables", [])
    joins = parsed_data.get("joins", [])
    where_clause = parsed_data.get("where_clause", "")
    return_fields = parsed_data.get("return_fields", [])
    is_distinct = parsed_data.get("is_distinct", False)
    order_by_clause = parsed_data.get("order_by_clause", "")
    limit_clause = parsed_data.get("limit_clause", "")
    group_by_fields = parsed_data.get("group_by_fields", [])

    if not tables:
        return "Unsupported query: No collection found."

    # Determine if query has aggregation function like SUM or COUNT
    has_aggregation = any(re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\b", f, re.IGNORECASE) for f in return_fields)

    # Basic query structure
    query = {
        "collection": tables[0]["label"],
        "operation": "find" if not joins and not (group_by_fields or has_aggregation) else "aggregate",
        "filter": {}
    }

    # Build filter from where clause (supporting multiple AND conditions)
    if where_clause:
        condition = where_clause.replace("WHERE ", "").strip()
        conditions = [cond.strip() for cond in re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)]
        mongo_filter = {}
        for cond in conditions:
            match = re.match(r"(\w+)\s*(>=|<=|!=|=|<|>)\s*(.+)", cond)
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
                value = value.strip("'\"")
                try:
                    # Attempt to convert to int or float if possible
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    # Keep string as is
                    pass
                mongo_filter[field] = {mongo_operator: value}
            else:
                # fallback for complex expressions
                mongo_filter["$expr"] = {"$eq": [True, cond]}
        query["filter"] = mongo_filter

    # Handle aggregation pipeline
    if joins or group_by_fields or has_aggregation:
        pipeline = []

        # If joins exist, add $lookup stages
        if joins:
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

        # Add $match stage if filter is not empty
        if query.get("filter"):
            pipeline.append({"$match": query["filter"]})

        # Handle $group stage for GROUP BY and aggregation
        if group_by_fields or has_aggregation:
            group_stage = {
                "$group": {
                    "_id": None
                }
            }
            # Build _id for grouping
            if len(group_by_fields) == 1:
                group_stage["$group"]["_id"] = f"${group_by_fields[0]}"
            elif len(group_by_fields) > 1:
                group_stage["$group"]["_id"] = {field: f"${field}" for field in group_by_fields}
            else:
                # No group fields, fallback to null grouping
                group_stage["$group"]["_id"] = None

            # Add aggregation fields like SUM, COUNT
            for field in return_fields:
                agg_match_sum = re.match(r"SUM\((\w+)\)\s+AS\s+(\w+)", field, re.IGNORECASE)
                agg_match_count = re.match(r"COUNT\((\w+|\*)\)\s+AS\s+(\w+)", field, re.IGNORECASE)
                agg_match_avg = re.match(r"AVG\((\w+)\)\s+AS\s+(\w+)", field, re.IGNORECASE)
                agg_match_min = re.match(r"MIN\((\w+)\)\s+AS\s+(\w+)", field, re.IGNORECASE)
                agg_match_max = re.match(r"MAX\((\w+)\)\s+AS\s+(\w+)", field, re.IGNORECASE)

                if agg_match_sum:
                    src_field, alias = agg_match_sum.groups()
                    group_stage["$group"][alias] = {"$sum": f"${src_field}"}
                elif agg_match_count:
                    src_field, alias = agg_match_count.groups()
                    if src_field == "*":
                        group_stage["$group"][alias] = {"$sum": 1}
                    else:
                        group_stage["$group"][alias] = {"$sum": {"$cond": [{"$ifNull": [f"${src_field}", False]}, 1, 0]}}
                elif agg_match_avg:
                    src_field, alias = agg_match_avg.groups()
                    group_stage["$group"][alias] = {"$avg": f"${src_field}"}
                elif agg_match_min:
                    src_field, alias = agg_match_min.groups()
                    group_stage["$group"][alias] = {"$min": f"${src_field}"}
                elif agg_match_max:
                    src_field, alias = agg_match_max.groups()
                    group_stage["$group"][alias] = {"$max": f"${src_field}"}

            pipeline.append(group_stage)

            # Build $project stage to reshape output and map _id fields to top level
            project_stage = {"$project": {"_id": 0}}
            for field in return_fields:
                # If field is aggregation alias, project it as 1
                alias_match = re.match(r".+ AS (\w+)", field, re.IGNORECASE)
                if alias_match:
                    alias = alias_match.group(1)
                    project_stage["$project"][alias] = 1
                elif field in group_by_fields:
                    # Map grouped field from _id
                    if isinstance(group_stage["$group"]["_id"], dict):
                        project_stage["$project"][field] = f"$_id.{field}"
                    else:
                        project_stage["$project"][field] = "$_id"
            pipeline.append(project_stage)

        else:
            # No group by or aggregation, just projection stage if needed
            if return_fields and return_fields != ["*"]:
                projection = {}
                for field in return_fields:
                    if " AS " in field.upper():
                        parts = re.split(r"\s+AS\s+", field, flags=re.IGNORECASE)
                        original_field = parts[0].strip()
                        alias = parts[1].strip()
                        projection[alias] = f"${original_field}"
                    else:
                        projection[field] = 1
                pipeline.append({"$project": projection})

        # Handle order by
        if order_by_clause:
            sort_fields = {}
            order_by_clause_clean = order_by_clause.replace("ORDER BY ", "")
            sort_parts = [part.strip() for part in order_by_clause_clean.split(",")]
            for part in sort_parts:
                pieces = part.split()
                field = pieces[0]
                direction = -1 if len(pieces) > 1 and pieces[1].upper() == "DESC" else 1
                sort_fields[field] = direction
            pipeline.append({"$sort": sort_fields})

        # Handle limit
        if limit_clause:
            limit_value = limit_clause.replace("LIMIT ", "").strip()
            try:
                limit_int = int(limit_value)
                pipeline.append({"$limit": limit_int})
            except ValueError:
                pass

        query["pipeline"] = pipeline

    else:
        # No joins or aggregation, build find query projection
        if return_fields and return_fields != ["*"]:
            projection = {}
            for field in return_fields:
                if " AS " in field.upper():
                    parts = re.split(r"\s+AS\s+", field, flags=re.IGNORECASE)
                    original_field = parts[0].strip()
                    alias = parts[1].strip()
                    projection[alias] = f"${original_field}"
                else:
                    projection[field] = 1
            # Exclude _id unless explicitly requested
            original_fields = [re.split(r"\s+AS\s+", f, flags=re.IGNORECASE)[0].strip() for f in return_fields]
            if "_id" not in original_fields:
                projection["_id"] = 0
            query["projection"] = projection

    return query