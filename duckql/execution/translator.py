"""GraphQL to SQL query translator."""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
import sqlglot
from sqlglot import exp, parse_one
from sqlglot.expressions import Select, Column, Table, Condition


@dataclass
class QueryContext:
    """Context for building SQL queries."""
    table_name: str
    selections: List[str]
    where_conditions: List[str]
    order_by: List[tuple[str, str]]
    limit: Optional[int] = None
    offset: Optional[int] = None
    params: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.params is None:
            self.params = {}


class GraphQLToSQLTranslator:
    """Translates GraphQL queries to SQL."""
    
    def __init__(self):
        self.param_counter = 0
    
    def translate_query(
        self,
        table_name: str,
        selections: List[str],
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> tuple[str, Dict[str, Any]]:
        """Translate a GraphQL query to SQL."""
        context = QueryContext(
            table_name=table_name,
            selections=selections,
            where_conditions=[],
            order_by=[],
            limit=limit,
            offset=offset,
        )
        
        # Reset param counter for each query
        self.param_counter = 0
        
        # Build WHERE clause
        if where:
            self._build_where_conditions(where, context)
        
        # Build ORDER BY clause
        if order_by:
            for field, direction in order_by.items():
                context.order_by.append((field, direction))
        
        # Generate SQL using sqlglot
        sql = self._build_sql(context)
        
        return sql, context.params
    
    def translate_where(self, where: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        """Translate just the WHERE clause conditions."""
        context = QueryContext(
            table_name="",  # Not needed for WHERE only
            selections=[],
            where_conditions=[],
            order_by=[],
        )
        
        # Reset param counter
        self.param_counter = 0
        
        # Build WHERE conditions
        self._build_where_conditions(where, context)
        
        # Join conditions
        where_sql = " AND ".join(context.where_conditions) if context.where_conditions else ""
        
        return where_sql, context.params
    
    def _build_where_conditions(self, where: Dict[str, Any], context: QueryContext) -> None:
        """Build WHERE conditions from GraphQL filter."""
        for key, value in where.items():
            if key == "_and":
                # Handle AND conditions
                and_conditions = []
                for sub_where in value:
                    sub_context = QueryContext(
                        table_name=context.table_name,
                        selections=[],
                        where_conditions=[],
                        order_by=[],
                    )
                    sub_context.params = context.params
                    self._build_where_conditions(sub_where, sub_context)
                    if sub_context.where_conditions:
                        and_conditions.append(f"({' AND '.join(sub_context.where_conditions)})")
                if and_conditions:
                    context.where_conditions.append(f"({' AND '.join(and_conditions)})")
            
            elif key == "_or":
                # Handle OR conditions
                or_conditions = []
                for sub_where in value:
                    sub_context = QueryContext(
                        table_name=context.table_name,
                        selections=[],
                        where_conditions=[],
                        order_by=[],
                    )
                    sub_context.params = context.params
                    self._build_where_conditions(sub_where, sub_context)
                    if sub_context.where_conditions:
                        or_conditions.append(f"({' AND '.join(sub_context.where_conditions)})")
                if or_conditions:
                    context.where_conditions.append(f"({' OR '.join(or_conditions)})")
            
            elif key == "_not":
                # Handle NOT conditions
                sub_context = QueryContext(
                    table_name=context.table_name,
                    selections=[],
                    where_conditions=[],
                    order_by=[],
                )
                sub_context.params = context.params
                self._build_where_conditions(value, sub_context)
                if sub_context.where_conditions:
                    context.where_conditions.append(f"NOT ({' AND '.join(sub_context.where_conditions)})")
            
            else:
                # Handle field conditions
                self._build_field_condition(key, value, context)
    
    def _build_field_condition(self, key: str, value: Any, context: QueryContext) -> None:
        """Build a condition for a specific field."""
        if value is None:
            return
        
        # Parse field name and operator
        parts = key.split("_")
        
        # Check for operators at the end
        if len(parts) > 1:
            # Check for not_in operator first
            if len(parts) >= 2 and "_".join(parts[-2:]) == "not_in":
                field_name = "_".join(parts[:-2])
                operator = "not_in"
            else:
                last_part = parts[-1]
                if last_part in ["eq", "ne", "gt", "gte", "lt", "lte", "like", "ilike", "in"]:
                    field_name = "_".join(parts[:-1])
                    operator = last_part
                else:
                    field_name = key
                    operator = "eq"
        else:
            field_name = key
            operator = "eq"
        
        # Create parameter placeholder
        param_name = f"p{self.param_counter}"
        self.param_counter += 1
        context.params[param_name] = value
        
        # Build condition based on operator (quote field names)
        quoted_field = f'"{field_name}"'
        if operator == "eq" or operator == "":
            condition = f"{quoted_field} = ${param_name}"
        elif operator == "ne":
            condition = f"{quoted_field} != ${param_name}"
        elif operator == "gt":
            condition = f"{quoted_field} > ${param_name}"
        elif operator == "gte":
            condition = f"{quoted_field} >= ${param_name}"
        elif operator == "lt":
            condition = f"{quoted_field} < ${param_name}"
        elif operator == "lte":
            condition = f"{quoted_field} <= ${param_name}"
        elif operator == "like":
            condition = f"{quoted_field} LIKE ${param_name}"
        elif operator == "ilike":
            condition = f"{quoted_field} ILIKE ${param_name}"
        elif operator == "in":
            if isinstance(value, list):
                placeholders = []
                for i, v in enumerate(value):
                    p = f"p{self.param_counter}"
                    self.param_counter += 1
                    context.params[p] = v
                    placeholders.append(f"${p}")
                condition = f"{quoted_field} IN ({', '.join(placeholders)})"
            else:
                condition = f"{quoted_field} = ${param_name}"
        elif operator == "not_in":
            if isinstance(value, list):
                placeholders = []
                for i, v in enumerate(value):
                    p = f"p{self.param_counter}"
                    self.param_counter += 1
                    context.params[p] = v
                    placeholders.append(f"${p}")
                condition = f"{quoted_field} NOT IN ({', '.join(placeholders)})"
            else:
                condition = f"{quoted_field} != ${param_name}"
        else:
            # Default to equality
            condition = f"{quoted_field} = ${param_name}"
        
        context.where_conditions.append(condition)
    
    def _build_sql(self, context: QueryContext) -> str:
        """Build SQL query from context using sqlglot."""
        # Start with basic SELECT
        query = Select()
        
        # Add columns
        if context.selections:
            for col in context.selections:
                # Quote column names to handle reserved words
                if col != '*':
                    query = query.select(f'"{col}"')
                else:
                    query = query.select(col)
        else:
            query = query.select("*")
        
        # Add FROM (quote table name)
        query = query.from_(f'"{context.table_name}"')
        
        # Add WHERE conditions
        if context.where_conditions:
            # Join all conditions with AND
            where_clause = " AND ".join(context.where_conditions)
            # Parse the where clause safely
            query = query.where(where_clause)
        
        # Add ORDER BY (quote field names)
        if context.order_by:
            for field, direction in context.order_by:
                query = query.order_by(f'"{field}" {direction}')
        
        # Add LIMIT
        if context.limit is not None:
            query = query.limit(context.limit)
        
        # Add OFFSET
        if context.offset is not None:
            query = query.offset(context.offset)
        
        # Generate SQL
        sql = query.sql(dialect="duckdb", pretty=True)
        
        return sql


class AggregationTranslator:
    """Translates GraphQL aggregation queries to SQL."""
    
    def translate_aggregation(
        self,
        table_name: str,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, List[str]]] = None,
        where: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, Dict[str, Any]]:
        """Translate an aggregation query to SQL."""
        translator = GraphQLToSQLTranslator()
        
        # Build base context
        context = QueryContext(
            table_name=table_name,
            selections=[],
            where_conditions=[],
            order_by=[],
        )
        
        # Add GROUP BY fields to selections
        if group_by:
            context.selections.extend(group_by)
        
        # Add aggregation functions
        if aggregations:
            for field, functions in aggregations.items():
                for func in functions:
                    if func.upper() in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
                        context.selections.append(f"{func.upper()}({field}) AS {field}_{func.lower()}")
        else:
            # Default to COUNT(*)
            context.selections.append("COUNT(*) AS count")
        
        # Build WHERE conditions
        if where:
            translator._build_where_conditions(where, context)
        
        # Build SQL with GROUP BY
        query = Select()
        
        # Add columns
        for col in context.selections:
            query = query.select(col)
        
        # Add FROM
        query = query.from_(table_name)
        
        # Add WHERE
        if context.where_conditions:
            where_clause = " AND ".join(context.where_conditions)
            query = query.where(where_clause)
        
        # Add GROUP BY
        if group_by:
            for field in group_by:
                query = query.group_by(field)
        
        sql = query.sql(dialect="duckdb", pretty=True)
        
        return sql, context.params