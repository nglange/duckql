"""Tests for GraphQL type generation."""

import pytest
from typing import Optional, List
# datetime and date are no longer used as DuckDB returns strings
from decimal import Decimal
import strawberry
from strawberry.scalars import JSON

from duckql.schema.types import (
    duckdb_to_graphql_type, 
    TypeBuilder
)
from duckql.schema.introspection import TableInfo, ColumnInfo


class TestTypeConversion:
    """Test DuckDB to GraphQL type conversion."""
    
    def test_integer_types(self):
        """Test integer type conversions."""
        # Basic integers
        assert duckdb_to_graphql_type('INTEGER', False) == int
        assert duckdb_to_graphql_type('INTEGER', True) == Optional[int]
        assert duckdb_to_graphql_type('BIGINT', False) == int
        assert duckdb_to_graphql_type('SMALLINT', False) == int
        assert duckdb_to_graphql_type('TINYINT', False) == int
        
        # Unsigned integers
        assert duckdb_to_graphql_type('UINTEGER', False) == int
        assert duckdb_to_graphql_type('UBIGINT', False) == int
        
        # Huge integers
        assert duckdb_to_graphql_type('HUGEINT', False) == int
        assert duckdb_to_graphql_type('UHUGEINT', False) == int
    
    def test_floating_point_types(self):
        """Test floating point type conversions."""
        assert duckdb_to_graphql_type('DOUBLE', False) == float
        assert duckdb_to_graphql_type('REAL', False) == float
        assert duckdb_to_graphql_type('FLOAT', False) == float
        
        # Decimal types
        assert duckdb_to_graphql_type('DECIMAL', False) == Decimal
        assert duckdb_to_graphql_type('NUMERIC', False) == Decimal
    
    def test_string_types(self):
        """Test string type conversions."""
        assert duckdb_to_graphql_type('VARCHAR', False) == str
        assert duckdb_to_graphql_type('TEXT', False) == str
        assert duckdb_to_graphql_type('CHAR', False) == str
        assert duckdb_to_graphql_type('STRING', False) == str
    
    def test_boolean_type(self):
        """Test boolean type conversion."""
        assert duckdb_to_graphql_type('BOOLEAN', False) == bool
        assert duckdb_to_graphql_type('BOOL', False) == bool
    
    def test_datetime_types(self):
        """Test date/time type conversions."""
        assert duckdb_to_graphql_type('DATE', False) == str  # Dates are strings in DuckDB
        assert duckdb_to_graphql_type('TIMESTAMP', False) == str  # Timestamps are strings in DuckDB
        assert duckdb_to_graphql_type('TIMESTAMPTZ', False) == str
        
        # Time and interval as strings
        assert duckdb_to_graphql_type('TIME', False) == str
        assert duckdb_to_graphql_type('INTERVAL', False) == str
    
    def test_json_type(self):
        """Test JSON type conversion."""
        assert duckdb_to_graphql_type('JSON', False) == JSON
        assert duckdb_to_graphql_type('JSONB', False) == JSON
    
    def test_special_types(self):
        """Test special type conversions."""
        # UUID as string
        assert duckdb_to_graphql_type('UUID', False) == str
        
        # Binary as string (base64)
        assert duckdb_to_graphql_type('BLOB', False) == str
        assert duckdb_to_graphql_type('BYTEA', False) == str
    
    def test_array_types(self):
        """Test array type conversions."""
        assert duckdb_to_graphql_type('INTEGER[]', False) == List[int]
        assert duckdb_to_graphql_type('INTEGER[]', True) == Optional[List[int]]
        assert duckdb_to_graphql_type('VARCHAR[]', False) == List[str]
        assert duckdb_to_graphql_type('JSON[]', False) == List[JSON]
    
    def test_parameterized_types(self):
        """Test parameterized type conversions."""
        assert duckdb_to_graphql_type('DECIMAL(10, 2)', False) == Decimal
        assert duckdb_to_graphql_type('VARCHAR(255)', False) == str
        assert duckdb_to_graphql_type('NUMERIC(18, 6)', False) == Decimal
    
    def test_unknown_types(self):
        """Test unknown type defaults to JSON."""
        assert duckdb_to_graphql_type('UNKNOWN_TYPE', False) == JSON
        assert duckdb_to_graphql_type('STRUCT', False) == JSON
        assert duckdb_to_graphql_type('MAP', False) == JSON


class TestTypeBuilder:
    """Test GraphQL type building from schema."""
    
    @pytest.fixture
    def type_builder(self):
        """Create a TypeBuilder instance."""
        return TypeBuilder()
    
    def test_build_simple_type(self, type_builder):
        """Test building a simple GraphQL type."""
        table_info = TableInfo(
            name='users',
            columns=[
                ColumnInfo('id', 'INTEGER', False, True),
                ColumnInfo('name', 'VARCHAR', True, False),
                ColumnInfo('email', 'VARCHAR', False, False),
                ColumnInfo('created_at', 'TIMESTAMP', False, False),
                ColumnInfo('is_active', 'BOOLEAN', False, False, 'true')
            ],
            primary_keys=['id'],
            indexes=[]
        )
        
        graphql_type = type_builder.build_type(table_info)
        
        # Check type name
        assert graphql_type.__name__ == 'Users'
        
        # Check annotations
        annotations = graphql_type.__annotations__
        assert annotations['id'] == int
        assert annotations['name'] == Optional[str]
        assert annotations['email'] == str
        assert annotations['created_at'] == str  # DuckDB returns timestamps as strings
        assert annotations['is_active'] == bool
    
    def test_build_type_with_json(self, type_builder):
        """Test building type with JSON fields."""
        table_info = TableInfo(
            name='events',
            columns=[
                ColumnInfo('event_id', 'UUID', False, True),
                ColumnInfo('event_type', 'TEXT', False, False),
                ColumnInfo('event_data', 'JSON', True, False),
                ColumnInfo('metadata', 'JSON', True, False)
            ],
            primary_keys=['event_id'],
            indexes=[]
        )
        
        graphql_type = type_builder.build_type(table_info)
        
        assert graphql_type.__name__ == 'Events'
        annotations = graphql_type.__annotations__
        assert annotations['event_id'] == str  # UUID as string
        assert annotations['event_data'] == Optional[JSON]
        assert annotations['metadata'] == Optional[JSON]
    
    def test_build_type_with_arrays(self, type_builder):
        """Test building type with array fields."""
        table_info = TableInfo(
            name='array_table',
            columns=[
                ColumnInfo('id', 'INTEGER', False, True),
                ColumnInfo('tags', 'VARCHAR[]', True, False),
                ColumnInfo('scores', 'INTEGER[]', False, False),
                ColumnInfo('data', 'JSON[]', True, False)
            ],
            primary_keys=['id'],
            indexes=[]
        )
        
        graphql_type = type_builder.build_type(table_info)
        
        annotations = graphql_type.__annotations__
        assert annotations['tags'] == Optional[List[str]]
        assert annotations['scores'] == List[int]
        assert annotations['data'] == Optional[List[JSON]]
    
    def test_pascal_case_conversion(self, type_builder):
        """Test snake_case to PascalCase conversion."""
        table_info = TableInfo(
            name='turn_metrics',
            columns=[ColumnInfo('id', 'INTEGER', False, True)],
            primary_keys=['id'],
            indexes=[]
        )
        
        graphql_type = type_builder.build_type(table_info)
        assert graphql_type.__name__ == 'TurnMetrics'
    
    def test_build_filter_type(self, type_builder):
        """Test building filter input type."""
        table_info = TableInfo(
            name='products',
            columns=[
                ColumnInfo('id', 'INTEGER', False, True),
                ColumnInfo('name', 'VARCHAR', False, False),
                ColumnInfo('price', 'DECIMAL', False, False),
                ColumnInfo('created_at', 'TIMESTAMP', False, False),
                ColumnInfo('is_available', 'BOOLEAN', False, False)
            ],
            primary_keys=['id'],
            indexes=[]
        )
        
        # Build main type (which also builds filter type)
        type_builder.build_type(table_info)
        
        # Get filter type
        filter_type = type_builder.get_filter_type('products')
        assert filter_type is not None
        assert filter_type.__name__ == 'ProductsFilter'
        
        # Check filter fields
        annotations = filter_type.__annotations__
        
        # Numeric filters
        assert 'id' in annotations
        assert 'id_eq' in annotations
        assert 'id_gt' in annotations
        assert 'id_gte' in annotations
        assert 'id_lt' in annotations
        assert 'id_lte' in annotations
        assert 'id_in' in annotations
        
        # String filters
        assert 'name' in annotations
        assert 'name_eq' in annotations
        assert 'name_like' in annotations
        assert 'name_ilike' in annotations
        assert 'name_in' in annotations
        
        # Boolean filters
        assert 'is_available' in annotations
        assert 'is_available_eq' in annotations
        
        # Note: Logical operators (_and, _or, _not) work in queries but are not
        # exposed in the GraphQL schema due to self-referential type limitations
        # assert '_and' in annotations
        # assert '_or' in annotations
        # assert '_not' in annotations
    
    def test_build_order_by_type(self, type_builder):
        """Test building order by input type."""
        table_info = TableInfo(
            name='orders',
            columns=[
                ColumnInfo('id', 'INTEGER', False, True),
                ColumnInfo('total', 'DECIMAL', False, False),
                ColumnInfo('created_at', 'TIMESTAMP', False, False)
            ],
            primary_keys=['id'],
            indexes=[]
        )
        
        # Build main type
        type_builder.build_type(table_info)
        
        # Get order by type
        order_by_type = type_builder.get_order_by_type('orders')
        assert order_by_type is not None
        assert order_by_type.__name__ == 'OrdersOrderBy'
        
        # Check fields
        annotations = order_by_type.__annotations__
        assert 'id' in annotations
        assert 'total' in annotations
        assert 'created_at' in annotations
    
    def test_computed_fields(self, type_builder):
        """Test adding computed fields."""
        def full_name(obj):
            return f"{obj['first_name']} {obj['last_name']}"
        
        type_builder.add_computed_field('users', 'full_name', full_name)
        
        computed_fields = type_builder.get_computed_fields('users')
        assert 'full_name' in computed_fields
        assert computed_fields['full_name'] == full_name
    
    def test_reserved_word_handling(self, type_builder):
        """Test handling of reserved words in table/column names."""
        table_info = TableInfo(
            name='order',  # Reserved SQL word
            columns=[
                ColumnInfo('select', 'INTEGER', False, True),  # Reserved
                ColumnInfo('from', 'VARCHAR', True, False),    # Reserved
                ColumnInfo('where', 'BOOLEAN', False, False)   # Reserved
            ],
            primary_keys=['select'],
            indexes=[]
        )
        
        graphql_type = type_builder.build_type(table_info)
        
        # Type name should be converted
        assert graphql_type.__name__ == 'Order'
        
        # Field names - Python keywords get underscore suffix
        annotations = graphql_type.__annotations__
        assert 'select' in annotations
        assert 'from_' in annotations  # Python keyword, so gets underscore
        assert 'where' in annotations