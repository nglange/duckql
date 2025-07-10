"""GraphQL type generation from DuckDB schema."""

from typing import Dict, Any, Optional, List, Type
import strawberry
from strawberry import field
from strawberry.scalars import JSON
# Date/time types are handled as strings since DuckDB returns them as ISO strings
from decimal import Decimal
import uuid
from enum import Enum

# Create OrderDirection enum once
OrderDirection = strawberry.enum(
    Enum("OrderDirection", {"ASC": "ASC", "DESC": "DESC"})
)

from .introspection import TableInfo, ColumnInfo


# Type mapping from DuckDB to Python/GraphQL types
DUCKDB_TYPE_MAP = {
    # Numeric types
    'BIGINT': int,
    'INTEGER': int,
    'INT': int,
    'SMALLINT': int,
    'TINYINT': int,
    'UBIGINT': int,
    'UINTEGER': int,
    'USMALLINT': int,
    'UTINYINT': int,
    'HUGEINT': int,
    'UHUGEINT': int,
    
    # Floating point
    'DOUBLE': float,
    'REAL': float,
    'FLOAT': float,
    'DECIMAL': Decimal,
    'NUMERIC': Decimal,
    
    # Boolean
    'BOOLEAN': bool,
    'BOOL': bool,
    
    # String types
    'VARCHAR': str,
    'TEXT': str,
    'STRING': str,
    'CHAR': str,
    'BPCHAR': str,
    
    # Date/Time types  
    'DATE': str,  # DuckDB returns dates as strings
    'TIMESTAMP': str,  # DuckDB returns timestamps as strings
    'TIMESTAMP WITHOUT TIME ZONE': str,
    'TIMESTAMP WITH TIME ZONE': str,
    'TIMESTAMPTZ': str,
    'TIME': str,  # GraphQL doesn't have native time type
    'INTERVAL': str,  # Store as string
    
    # Binary
    'BLOB': str,  # Base64 encoded
    'BYTEA': str,
    
    # JSON
    'JSON': JSON,
    
    # UUID
    'UUID': str,  # GraphQL doesn't have native UUID
    
    # Arrays - handled separately
    # STRUCT/MAP - handled as JSON
}


def duckdb_to_graphql_type(duckdb_type: str, is_nullable: bool = True) -> Any:
    """Convert DuckDB type to GraphQL type."""
    # Clean up type string
    base_type = duckdb_type.upper().strip()
    
    # Handle array types
    if base_type.endswith('[]'):
        element_type = base_type[:-2]
        inner_type = duckdb_to_graphql_type(element_type, False)
        return Optional[List[inner_type]] if is_nullable else List[inner_type]
    
    # Handle parameterized types
    if '(' in base_type:
        base_type = base_type.split('(')[0]
    
    # Map to Python type
    python_type = DUCKDB_TYPE_MAP.get(base_type, JSON)
    
    # Wrap in Optional if nullable
    return Optional[python_type] if is_nullable else python_type


class TypeBuilder:
    """Builds GraphQL types from database schema."""
    
    def __init__(self):
        self._types: Dict[str, Type] = {}
        self._filter_types: Dict[str, Type] = {}
        self._order_by_types: Dict[str, Type] = {}
        self._computed_fields: Dict[str, Dict[str, Any]] = {}
    
    def build_type(self, table_info: TableInfo) -> Type:
        """Build a GraphQL type from table information."""
        # Convert table name to PascalCase
        type_name = self._to_pascal_case(table_info.name)
        
        # Check if already built
        if type_name in self._types:
            return self._types[type_name]
        
        # Build field annotations
        annotations = {}
        for column in table_info.columns:
            field_name = self._to_field_name(column.name)
            field_type = duckdb_to_graphql_type(column.data_type, column.is_nullable)
            annotations[field_name] = field_type
        
        # Create the type dynamically with default values
        class_dict = {'__annotations__': annotations}
        
        # Add default values for all fields to handle partial selection
        for field_name in annotations.keys():
            class_dict[field_name] = None
            
        graphql_type = type(type_name, (), class_dict)
        
        # Apply strawberry decorator
        graphql_type = strawberry.type(graphql_type)
        
        # Override field names to preserve original database column names
        for field_def in graphql_type.__strawberry_definition__.fields:
            # Preserve the original field name instead of converting to camelCase
            field_def.graphql_name = field_def.python_name
        
        # Store for reuse
        self._types[type_name] = graphql_type
        
        # Also build filter and order by types
        self._build_filter_type(table_info)
        self._build_order_by_type(table_info)
        
        return graphql_type
    
    def _build_filter_type(self, table_info: TableInfo) -> Type:
        """Build a filter input type for WHERE clauses."""
        type_name = f"{self._to_pascal_case(table_info.name)}Filter"
        
        if type_name in self._filter_types:
            return self._filter_types[type_name]
        
        # Build filter fields
        annotations = {}
        
        for column in table_info.columns:
            field_name = self._to_field_name(column.name)
            base_type = duckdb_to_graphql_type(column.data_type, False)
            
            # Add comparison operators based on type
            if base_type in (int, float, Decimal):
                annotations[f"{field_name}"] = Optional[base_type]
                annotations[f"{field_name}_eq"] = Optional[base_type]
                annotations[f"{field_name}_ne"] = Optional[base_type]
                annotations[f"{field_name}_gt"] = Optional[base_type]
                annotations[f"{field_name}_gte"] = Optional[base_type]
                annotations[f"{field_name}_lt"] = Optional[base_type]
                annotations[f"{field_name}_lte"] = Optional[base_type]
                annotations[f"{field_name}_in"] = Optional[List[base_type]]
                annotations[f"{field_name}_not_in"] = Optional[List[base_type]]
            elif base_type == str:
                annotations[f"{field_name}"] = Optional[base_type]
                annotations[f"{field_name}_eq"] = Optional[base_type]
                annotations[f"{field_name}_ne"] = Optional[base_type]
                annotations[f"{field_name}_like"] = Optional[base_type]
                annotations[f"{field_name}_ilike"] = Optional[base_type]
                annotations[f"{field_name}_in"] = Optional[List[base_type]]
                annotations[f"{field_name}_not_in"] = Optional[List[base_type]]
            elif base_type == bool:
                annotations[f"{field_name}"] = Optional[base_type]
                annotations[f"{field_name}_eq"] = Optional[base_type]
                annotations[f"{field_name}_ne"] = Optional[base_type]
            elif column.data_type in ('DATE', 'TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ'):
                # Date/time fields get comparison operators
                annotations[f"{field_name}"] = Optional[base_type]
                annotations[f"{field_name}_eq"] = Optional[base_type]
                annotations[f"{field_name}_ne"] = Optional[base_type]
                annotations[f"{field_name}_gt"] = Optional[base_type]
                annotations[f"{field_name}_gte"] = Optional[base_type]
                annotations[f"{field_name}_lt"] = Optional[base_type]
                annotations[f"{field_name}_lte"] = Optional[base_type]
            else:
                # Default: equality only
                annotations[f"{field_name}"] = Optional[base_type]
                annotations[f"{field_name}_eq"] = Optional[base_type]
        
        # Create a unique class for each filter type with annotations and defaults
        class_dict = {'__annotations__': annotations}
        
        # Add default values for all fields
        for field_name in annotations.keys():
            class_dict[field_name] = None
            
        filter_class = type(type_name, (), class_dict)
        
        # Apply strawberry decorator
        filter_class = strawberry.input(filter_class)
        
        # Override field names to preserve original database column names
        for field_def in filter_class.__strawberry_definition__.fields:
            # Preserve the original field name instead of converting to camelCase
            field_def.graphql_name = field_def.python_name
        
        # TODO: Add logical operators (_and, _or, _not) with self-reference
        # This is complex with Strawberry's type system
        
        self._filter_types[type_name] = filter_class
        return filter_class
    
    def _build_order_by_type(self, table_info: TableInfo) -> Type:
        """Build an order by input type."""
        type_name = f"{self._to_pascal_case(table_info.name)}OrderBy"
        
        if type_name in self._order_by_types:
            return self._order_by_types[type_name]
        
        
        # Build order by fields
        annotations = {}
        for column in table_info.columns:
            field_name = self._to_field_name(column.name)
            annotations[field_name] = Optional[OrderDirection]
        
        # Create the order by type with default values
        class_dict = {'__annotations__': annotations}
        
        # Add default values for all fields
        for field_name in annotations.keys():
            class_dict[field_name] = None
            
        order_by_type = type(type_name, (), class_dict)
        
        # Apply strawberry decorator
        order_by_type = strawberry.input(order_by_type)
        
        # Override field names to preserve original database column names
        for field_def in order_by_type.__strawberry_definition__.fields:
            # Preserve the original field name instead of converting to camelCase
            field_def.graphql_name = field_def.python_name
        
        self._order_by_types[type_name] = order_by_type
        return order_by_type
    
    def get_filter_type(self, table_name: str) -> Optional[Type]:
        """Get the filter type for a table."""
        type_name = f"{self._to_pascal_case(table_name)}Filter"
        return self._filter_types.get(type_name)
    
    def get_order_by_type(self, table_name: str) -> Optional[Type]:
        """Get the order by type for a table."""
        type_name = f"{self._to_pascal_case(table_name)}OrderBy"
        return self._order_by_types.get(type_name)
    
    def add_computed_field(self, table_name: str, field_name: str, resolver: Any) -> None:
        """Add a computed field to a type."""
        type_name = self._to_pascal_case(table_name)
        if type_name not in self._computed_fields:
            self._computed_fields[type_name] = {}
        self._computed_fields[type_name][field_name] = resolver
    
    def get_computed_fields(self, table_name: str) -> Dict[str, Any]:
        """Get computed fields for a table."""
        type_name = self._to_pascal_case(table_name)
        return self._computed_fields.get(type_name, {})
    
    @staticmethod
    def _to_pascal_case(snake_str: str) -> str:
        """Convert snake_case to PascalCase."""
        return ''.join(word.capitalize() for word in snake_str.split('_'))
    
    @staticmethod
    def _to_field_name(column_name: str) -> str:
        """Convert column name to valid GraphQL field name."""
        # Replace invalid characters
        field_name = column_name.replace('-', '_').replace(' ', '_')
        
        # Handle names that start with numbers
        if field_name and field_name[0].isdigit():
            field_name = f"field_{field_name}"
        
        # Handle Python reserved words
        import keyword
        if keyword.iskeyword(field_name):
            field_name = f"{field_name}_"
        
        return field_name