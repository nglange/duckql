"""GraphQL type generation for aggregation queries."""

from typing import Dict, Any, List, Type, Optional
import strawberry
from strawberry.scalars import JSON
from decimal import Decimal
from datetime import datetime, date
import dateutil.parser

from .types import TypeBuilder, duckdb_to_graphql_type


class AggregateTypeBuilder:
    """Builds GraphQL types for aggregation queries."""
    
    def __init__(self, type_builder: TypeBuilder):
        self.type_builder = type_builder
        self._aggregate_types: Dict[str, Type] = {}
        self._aggregate_input_types: Dict[str, Type] = {}
    
    def build_aggregate_type(self, table_name: str, table_info) -> Type:
        """Build an aggregate result type for a table."""
        type_name = f"{self.type_builder._to_pascal_case(table_name)}Aggregate"
        
        if type_name in self._aggregate_types:
            return self._aggregate_types[type_name]
        
        
        # Build annotations for aggregate fields
        annotations = {}
        
        # Add grouping fields (all columns can be grouped by)
        for column in table_info.columns:
            field_name = self.type_builder._to_field_name(column.name)
            field_type = duckdb_to_graphql_type(column.data_type, True)
            annotations[field_name] = Optional[field_type]
        
        # Add aggregate function results for numeric columns
        numeric_types = {int, float, Decimal}
        for column in table_info.columns:
            field_name = self.type_builder._to_field_name(column.name)
            base_type = duckdb_to_graphql_type(column.data_type, False)
            
            if base_type in numeric_types:
                # Create a nested type for aggregate functions
                agg_type_name = f"{type_name}{self.type_builder._to_pascal_case(field_name)}Agg"
                agg_annotations = {
                    'sum': Optional[float],
                    'avg': Optional[float],
                    'min': Optional[base_type],
                    'max': Optional[base_type],
                    'stddev': Optional[float],
                    'variance': Optional[float],
                }
                
                agg_type = type(agg_type_name, (), {
                    '__annotations__': agg_annotations,
                })
                agg_type = strawberry.type(agg_type)
                
                annotations[f"{field_name}_agg"] = Optional[agg_type]
        
        # Add count field with a different name that won't be converted
        annotations['count_'] = int
        
        # Create the aggregate type
        aggregate_type = type(type_name, (), {
            '__annotations__': annotations,
        })
        
        # Apply strawberry decorator
        aggregate_type = strawberry.type(aggregate_type)
        
        # Override field names to preserve original database column names  
        for field_def in aggregate_type.__strawberry_definition__.fields:
            if field_def.python_name == 'count_':
                field_def.graphql_name = '_count'
            else:
                # For other fields, preserve the original column name
                field_def.graphql_name = field_def.python_name
            
        
        self._aggregate_types[type_name] = aggregate_type
        return aggregate_type
    
    def build_having_input_type(self, table_name: str, table_info) -> Type:
        """Build input type for HAVING clauses."""
        type_name = f"{self.type_builder._to_pascal_case(table_name)}HavingInput"
        
        if type_name in self._aggregate_input_types:
            return self._aggregate_input_types[type_name]
        
        # Build HAVING fields - these are aggregate function results
        annotations = {}
        
        # Add count field (as 'count' since '_count' is not valid in GraphQL JSON)
        annotations['count_gt'] = Optional[int]
        annotations['count_gte'] = Optional[int]
        annotations['count_lt'] = Optional[int]
        annotations['count_lte'] = Optional[int]
        annotations['count_eq'] = Optional[int]
        annotations['count_ne'] = Optional[int]
        
        # Add aggregate functions for numeric columns
        numeric_types = {int, float, Decimal}
        for column in table_info.columns:
            base_type = duckdb_to_graphql_type(column.data_type, False)
            if base_type in numeric_types:
                field_name = self.type_builder._to_field_name(column.name)
                # Add operators for SUM, AVG, MIN, MAX aggregates
                for func in ['sum', 'avg', 'min', 'max']:
                    for op in ['gt', 'gte', 'lt', 'lte', 'eq', 'ne']:
                        annotations[f"{field_name}_{func}_{op}"] = Optional[float]
        
        # Create input type with defaults
        class_dict = {'__annotations__': annotations}
        for field_name in annotations.keys():
            class_dict[field_name] = None
            
        having_type = type(type_name, (), class_dict)
        having_type = strawberry.input(having_type)
        
        # Override field names to preserve original database column names
        for field_def in having_type.__strawberry_definition__.fields:
            field_def.graphql_name = field_def.python_name
        
        self._aggregate_input_types[type_name] = having_type
        return having_type
    
    def create_aggregate_resolver(self, table_name: str, table_info, executor, translator):
        """Create a resolver for aggregate queries."""
        aggregate_type = self.build_aggregate_type(table_name, table_info)
        filter_type = self.type_builder.get_filter_type(table_name)
        having_type = self.build_having_input_type(table_name, table_info)
        
        # Helper to convert input to dict
        def _input_to_dict(input_obj: Any) -> Dict[str, Any]:
            if input_obj is None:
                return {}
            
            result = {}
            for key, value in vars(input_obj).items():
                if value is not None:
                    if hasattr(value, '__dict__'):
                        result[key] = _input_to_dict(value)
                    elif isinstance(value, list):
                        result[key] = [
                            _input_to_dict(item) if hasattr(item, '__dict__') else item
                            for item in value
                        ]
                    else:
                        result[key] = value
            
            return result

        async def aggregate_resolver(
            root: Any,
            info: Any,
            group_by: Optional[List[str]] = None,
            where: Optional[filter_type] = None,
            having: Optional[having_type] = None,
            functions: Optional[List[str]] = None,
        ) -> List[aggregate_type]:
            # Build SQL query for aggregation
            sql_parts = ["SELECT"]
            
            # Selections
            selections = []
            
            # Add GROUP BY columns
            if group_by:
                for col in group_by:
                    selections.append(col)
            
            # Add aggregate functions
            if not functions:
                # Default aggregates for numeric columns
                functions = []
                numeric_types = {int, float, Decimal}
                for column in table_info.columns:
                    base_type = duckdb_to_graphql_type(column.data_type, False)
                    if base_type in numeric_types:
                        field_name = column.name
                        functions.extend([
                            f"SUM({field_name}) as {field_name}_sum",
                            f"AVG({field_name}) as {field_name}_avg",
                            f"MIN({field_name}) as {field_name}_min",
                            f"MAX({field_name}) as {field_name}_max",
                        ])
            
            # Add COUNT
            selections.append("COUNT(*) as _count")
            
            # Add function results
            if functions:
                selections.extend(functions)
            
            sql_parts.append(", ".join(selections))
            sql_parts.append(f"FROM {table_name}")
            
            # WHERE clause
            params = {}
            if where:
                where_dict = _input_to_dict(where)
                where_sql, where_params = translator.translate_where(where_dict)
                if where_sql:
                    sql_parts.append(f"WHERE {where_sql}")
                    params.update(where_params)
            
            # GROUP BY clause
            if group_by:
                sql_parts.append(f"GROUP BY {', '.join(group_by)}")
            
            # HAVING clause
            if having:
                having_conditions = []
                having_dict = _input_to_dict(having)
                
                for key, value in having_dict.items():
                    if value is not None:
                        param_name = f"p{len(params)}"
                        params[param_name] = value
                        
                        # Parse the field name and operator
                        if key.startswith('count_'):
                            # Handle count aggregates
                            operator = key[6:]  # Remove 'count_' prefix
                            aggregate_field = "COUNT(*)"
                        elif '_' in key:
                            # Handle column aggregates like 'balance_sum_gt'
                            parts = key.split('_')
                            if len(parts) >= 3:
                                field_name = '_'.join(parts[:-2])
                                func_name = parts[-2].upper()
                                operator = parts[-1]
                                aggregate_field = f"{func_name}({field_name})"
                            else:
                                continue
                        else:
                            continue
                        
                        # Generate SQL condition based on operator
                        if operator == 'gt':
                            having_conditions.append(f"{aggregate_field} > ${param_name}")
                        elif operator == 'gte':
                            having_conditions.append(f"{aggregate_field} >= ${param_name}")
                        elif operator == 'lt':
                            having_conditions.append(f"{aggregate_field} < ${param_name}")
                        elif operator == 'lte':
                            having_conditions.append(f"{aggregate_field} <= ${param_name}")
                        elif operator == 'eq':
                            having_conditions.append(f"{aggregate_field} = ${param_name}")
                        elif operator == 'ne':
                            having_conditions.append(f"{aggregate_field} != ${param_name}")
                
                if having_conditions:
                    sql_parts.append(f"HAVING {' AND '.join(having_conditions)}")
            
            # Execute query
            sql = " ".join(sql_parts)
            result = await executor.execute_query(sql, params)
            
            # Convert results to aggregate type instances
            instances = []
            for row in result.rows:
                # Restructure row data for nested aggregate fields
                instance_data = {}
                
                for key, value in row.items():
                    if key == '_count':
                        instance_data['count_'] = value
                    elif '_' in key and any(key.endswith(f"_{fn}") for fn in ['sum', 'avg', 'min', 'max', 'stddev', 'variance']):
                        # This is an aggregate function result
                        field_name = key.rsplit('_', 1)[0]
                        func_name = key.rsplit('_', 1)[1]
                        
                        if f"{field_name}_agg" not in instance_data:
                            instance_data[f"{field_name}_agg"] = {}
                        
                        instance_data[f"{field_name}_agg"][func_name] = value
                    else:
                        # Regular field or group by column
                        instance_data[key] = value
                
                # Create instance with default values for missing fields
                # Get all field names from the aggregate type
                type_fields = aggregate_type.__annotations__.keys()
                full_instance_data = {}
                
                for field_name in type_fields:
                    if field_name in instance_data:
                        value = instance_data[field_name]
                        
                        # Convert string timestamps to datetime objects if needed
                        if value is not None and isinstance(value, str):
                            # Check if this field should be a datetime based on its annotation
                            field_annotation = aggregate_type.__annotations__.get(field_name)
                            if field_annotation:
                                # Handle Optional[datetime] by extracting the inner type
                                inner_type = field_annotation
                                if hasattr(field_annotation, '__args__') and field_annotation.__args__:
                                    inner_type = field_annotation.__args__[0]
                                
                                if inner_type == datetime:
                                    try:
                                        value = dateutil.parser.parse(value)
                                    except (ValueError, TypeError):
                                        # If parsing fails, keep as string
                                        pass
                                elif inner_type == date:
                                    try:
                                        value = dateutil.parser.parse(value).date()
                                    except (ValueError, TypeError):
                                        # If parsing fails, keep as string
                                        pass
                        # Check if this is a nested aggregate object (dictionary)
                        if isinstance(value, dict) and field_name.endswith('_agg'):
                            # Find the corresponding aggregate type  
                            base_field_name = field_name[:-4]  # Remove '_agg' suffix
                            current_type_name = aggregate_type.__name__
                            agg_type_name = f"{current_type_name}{self.type_builder._to_pascal_case(base_field_name)}Agg"
                            
                            # Find the aggregate type class
                            agg_type_class = None
                            for ann_name, ann_type in aggregate_type.__annotations__.items():
                                if ann_name == field_name:
                                    # Extract the inner type from Optional[Type]
                                    if hasattr(ann_type, '__args__') and ann_type.__args__:
                                        agg_type_class = ann_type.__args__[0]
                                    break
                            
                            if agg_type_class:
                                # Create the nested aggregate object with default values
                                agg_data = {}
                                for agg_field in ['sum', 'avg', 'min', 'max', 'stddev', 'variance']:
                                    agg_data[agg_field] = value.get(agg_field, None)
                                full_instance_data[field_name] = agg_type_class(**agg_data)
                            else:
                                full_instance_data[field_name] = value
                        else:
                            full_instance_data[field_name] = value
                    else:
                        # Set default value for missing optional fields
                        full_instance_data[field_name] = None
                
                instances.append(aggregate_type(**full_instance_data))
            
            return instances
        
        return strawberry.field(
            resolver=aggregate_resolver,
            description=f"Aggregate data from {table_name}"
        )