from models import SemanticQueryRequest


class QueryBuilder:
    def __init__(self, config: dict):
        self._config = config
        self._models = {m["name"]: m for m in config.get("models", [])}

    def build_query(self, tenant_slug: str, request: SemanticQueryRequest) -> tuple[str, list]:
        model = self._models.get(request.model)
        if not model:
            raise ValueError(f"Model '{request.model}' not found in config")

        # Validate dimensions
        valid_dims = {d["name"] for d in model.get("dimensions", [])}
        for d in request.dimensions:
            if d not in valid_dims:
                raise ValueError(f"Unknown dimension '{d}' for model '{request.model}'")

        # Validate measures
        valid_measures = {m["name"] for m in model.get("measures", [])}
        for m in request.measures:
            if m not in valid_measures:
                raise ValueError(f"Unknown measure '{m}' for model '{request.model}'")

        # Validate calculated measures
        valid_calc = {c["name"] for c in model.get("calculated_measures", [])}
        for c in request.calculated_measures:
            if c not in valid_calc:
                raise ValueError(f"Unknown calculated measure '{c}' for model '{request.model}'")

        # Validate joins
        valid_joins = {j["to"] for j in model.get("joins", [])}
        for j in request.joins:
            if j not in valid_joins:
                raise ValueError(f"Unknown join target '{j}' for model '{request.model}'")

        has_joins = len(request.joins) > 0
        prefix = "base." if has_joins else ""

        # --- Build measure config lookups ---
        measure_config = {m["name"]: m for m in model.get("measures", [])}
        calc_config = {c["name"]: c for c in model.get("calculated_measures", [])}

        # --- SELECT ---
        select_parts = []
        for d in request.dimensions:
            select_parts.append(f"{prefix}{d}")
        for m in request.measures:
            mc = measure_config[m]
            agg = mc.get("agg", "sum")
            source_col = mc.get("source_column", m)
            col_ref = f"{prefix}{source_col}"
            if agg == "count_distinct":
                select_parts.append(f"COUNT(DISTINCT {col_ref}) AS {m}")
            elif agg == "count":
                select_parts.append(f"COUNT({col_ref}) AS {m}")
            elif agg == "avg":
                select_parts.append(f"AVG({col_ref}) AS {m}")
            else:
                select_parts.append(f"SUM({col_ref}) AS {m}")
        for c in request.calculated_measures:
            cc = calc_config[c]
            select_parts.append(f"{cc['sql']} AS {c}")

        if not select_parts:
            select_parts.append("*")

        sql_parts = [f"SELECT {', '.join(select_parts)}"]

        # --- FROM ---
        if has_joins:
            sql_parts.append(f"FROM {request.model} AS base")
        else:
            sql_parts.append(f"FROM {request.model}")

        # --- JOINs ---
        join_config = {j["to"]: j for j in model.get("joins", [])}
        for j in request.joins:
            jc = join_config[j]
            join_type = jc.get("type", "left").upper()
            on_clauses = []
            # YAML parses "on" key as boolean True — handle both
            on_mapping = jc.get("on") or jc.get(True, {})
            for left_col, right_col in on_mapping.items():
                on_clauses.append(f"base.{left_col} = {j}.{right_col}")
            sql_parts.append(f"{join_type} JOIN {j} ON {' AND '.join(on_clauses)}")

        # --- WHERE (tenant isolation always first) ---
        params = []
        where_clauses = [f"{prefix}tenant_slug = ?"]
        params.append(tenant_slug)

        # Validate filter fields against known dimensions + measures
        all_fields = valid_dims | valid_measures
        for f in request.filters:
            if f.field not in all_fields and f.field != "tenant_slug":
                raise ValueError(
                    f"Unknown filter field '{f.field}' for model '{request.model}'. "
                    f"Valid fields: {', '.join(sorted(all_fields))}"
                )
            col_ref = f"{prefix}{f.field}"
            if f.op in ("IS NULL", "IS NOT NULL"):
                where_clauses.append(f"{col_ref} {f.op}")
            elif f.op == "IN":
                values = f.value if isinstance(f.value, list) else [f.value]
                placeholders = ", ".join("?" for _ in values)
                where_clauses.append(f"{col_ref} IN ({placeholders})")
                params.extend(values)
            elif f.op == "BETWEEN":
                values = f.value if isinstance(f.value, list) else [f.value]
                where_clauses.append(f"{col_ref} BETWEEN ? AND ?")
                params.extend(values[:2])
            else:
                where_clauses.append(f"{col_ref} {f.op} ?")
                params.append(f.value)

        sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")

        # --- GROUP BY ---
        if request.dimensions and (request.measures or request.calculated_measures):
            positions = ", ".join(str(i + 1) for i in range(len(request.dimensions)))
            sql_parts.append(f"GROUP BY {positions}")

        # --- ORDER BY ---
        if request.order_by:
            order_parts = []
            all_selectable = all_fields | valid_calc
            for ob in request.order_by:
                if ob.field not in all_selectable:
                    raise ValueError(
                        f"Unknown order_by field '{ob.field}' for model '{request.model}'. "
                        f"Valid fields: {', '.join(sorted(all_selectable))}"
                    )
                order_parts.append(f"{ob.field} {ob.dir.upper()}")
            sql_parts.append(f"ORDER BY {', '.join(order_parts)}")

        # --- LIMIT ---
        if request.limit is not None:
            sql_parts.append(f"LIMIT {request.limit}")

        return "\n".join(sql_parts), params

    def get_model_summary(self, model_name: str) -> dict:
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")
        return {
            "name": model["name"],
            "label": model.get("label", model["name"]),
            "description": model.get("description", ""),
            "dimension_count": len(model.get("dimensions", [])),
            "measure_count": len(model.get("measures", [])),
            "has_joins": len(model.get("joins", [])) > 0,
        }

    def get_model_detail(self, model_name: str) -> dict:
        model = self._models.get(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found")
        return {
            "name": model["name"],
            "label": model.get("label", model["name"]),
            "description": model.get("description", ""),
            "dimensions": model.get("dimensions", []),
            "measures": model.get("measures", []),
            "calculated_measures": model.get("calculated_measures", []),
            "joins": model.get("joins", []),
        }

    def list_models(self) -> list[dict]:
        return [self.get_model_summary(name) for name in self._models]
