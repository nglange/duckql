"""Tests for database introspection."""

import pytest
from duckql.schema.introspection import DuckDBIntrospector, TableInfo, ColumnInfo
from .test_database import create_test_database, create_complex_analytics_database


class TestDuckDBIntrospector:
    """Test database introspection functionality."""
    
    @pytest.fixture
    def test_db(self):
        """Create test database."""
        return create_test_database()
    
    @pytest.fixture
    def complex_db(self):
        """Create complex analytics database."""
        return create_complex_analytics_database()
    
    @pytest.fixture
    def introspector(self, test_db):
        """Create introspector for test database."""
        return DuckDBIntrospector(test_db)
    
    def test_get_tables(self, introspector):
        """Test table discovery."""
        tables = introspector.get_tables()
        
        expected_tables = [
            'simple_types', 'edge_types', 'json_types', 'special_chars',
            'parent_table', 'child_table', 'wide_table', 'order',
            'numeric_precision', 'empty_table'
        ]
        
        # Check all expected tables exist
        for table in expected_tables:
            assert table in tables, f"Table {table} not found"
        
        # Should not include views
        assert 'active_users' not in tables
        assert 'parent_child_view' not in tables
    
    def test_get_views(self, introspector):
        """Test view discovery."""
        views = introspector.get_views()
        
        assert 'active_users' in views
        assert 'parent_child_view' in views
        
        # Should not include tables
        assert 'simple_types' not in views
    
    def test_get_table_info_simple(self, introspector):
        """Test getting info for simple table."""
        info = introspector.get_table_info('simple_types')
        
        assert info.name == 'simple_types'
        assert len(info.columns) == 8
        
        # Check column details
        columns_by_name = {col.name: col for col in info.columns}
        
        # Check primary key
        assert columns_by_name['id'].is_primary_key
        assert columns_by_name['id'].data_type in ['INTEGER', 'INT']
        assert not columns_by_name['id'].is_nullable
        
        # Check nullable column
        assert columns_by_name['name'].data_type == 'VARCHAR'
        assert columns_by_name['name'].is_nullable
        
        # Check various types
        assert columns_by_name['balance'].data_type.startswith('DECIMAL')
        assert columns_by_name['is_active'].data_type in ['BOOLEAN', 'BOOL']
        assert columns_by_name['created_date'].data_type == 'DATE'
        assert columns_by_name['updated_at'].data_type == 'TIMESTAMP'
    
    def test_get_table_info_edge_types(self, introspector):
        """Test getting info for table with edge case types."""
        info = introspector.get_table_info('edge_types')
        
        columns_by_name = {col.name: col for col in info.columns}
        
        # Check UUID primary key
        assert columns_by_name['id'].is_primary_key
        assert columns_by_name['id'].data_type == 'UUID'
        
        # Check big integer types
        assert columns_by_name['big_number'].data_type == 'BIGINT'
        assert columns_by_name['huge_number'].data_type == 'HUGEINT'
        assert columns_by_name['tiny_number'].data_type == 'TINYINT'
        
        # Check floating point types (DuckDB may return FLOAT instead of REAL)
        assert columns_by_name['real_number'].data_type in ['REAL', 'FLOAT']
        assert columns_by_name['double_number'].data_type == 'DOUBLE'
        
        # Check binary and time types
        assert columns_by_name['binary_data'].data_type == 'BLOB'
        assert columns_by_name['time_only'].data_type == 'TIME'
        assert columns_by_name['interval_data'].data_type == 'INTERVAL'
    
    def test_get_table_info_json_types(self, introspector):
        """Test getting info for table with JSON and array types."""
        info = introspector.get_table_info('json_types')
        
        columns_by_name = {col.name: col for col in info.columns}
        
        # Check JSON columns
        assert columns_by_name['config'].data_type == 'JSON'
        assert columns_by_name['metadata'].data_type == 'JSON'
        assert columns_by_name['nested_json'].data_type == 'JSON'
        
        # Check array columns
        assert columns_by_name['int_array'].data_type == 'INTEGER[]'
        assert columns_by_name['string_array'].data_type == 'VARCHAR[]'
    
    def test_get_table_info_reserved_words(self, introspector):
        """Test getting info for table with reserved word names."""
        info = introspector.get_table_info('order')
        
        assert info.name == 'order'
        
        # Check reserved word column names
        columns_by_name = {col.name: col for col in info.columns}
        assert 'select' in columns_by_name
        assert 'from' in columns_by_name
        assert 'where' in columns_by_name
        
        # Check mixed case columns
        assert 'CamelCase' in columns_by_name
        assert 'snake_case' in columns_by_name
        
        # Check numeric column names
        assert 'column123' in columns_by_name
        assert '123start' in columns_by_name
    
    def test_get_table_info_relationships(self, introspector):
        """Test getting info for tables with relationships."""
        parent_info = introspector.get_table_info('parent_table')
        child_info = introspector.get_table_info('child_table')
        
        # Check primary keys
        parent_columns = {col.name: col for col in parent_info.columns}
        assert parent_columns['parent_id'].is_primary_key
        
        child_columns = {col.name: col for col in child_info.columns}
        assert child_columns['child_id'].is_primary_key
        
        # Foreign key column should exist (though FK constraint might not be enforced)
        assert 'parent_id' in child_columns
    
    def test_complex_schema_introspection(self, complex_db):
        """Test introspection of complex analytics schema."""
        introspector = DuckDBIntrospector(complex_db)
        
        # Check table exists
        tables = introspector.get_tables()
        assert 'wide_metrics' in tables
        
        # Check wide_metrics with 50+ columns
        wide_metrics = introspector.get_table_info('wide_metrics')
        assert len(wide_metrics.columns) > 50, "wide_metrics should have 50+ columns"
        
        # Check composite primary key
        pk_columns = [col.name for col in wide_metrics.columns if col.is_primary_key]
        assert 'id' in pk_columns
        assert 'timestamp' in pk_columns
        
        # Check JSON columns exist
        columns_by_name = {col.name: col for col in wide_metrics.columns}
        assert columns_by_name['properties'].data_type == 'JSON'
        assert columns_by_name['metadata'].data_type == 'JSON'
    
    def test_numeric_precision_types(self, introspector):
        """Test all numeric type variations."""
        info = introspector.get_table_info('numeric_precision')
        
        columns_by_name = {col.name: col for col in info.columns}
        
        # Signed integers
        assert columns_by_name['tinyint_col'].data_type == 'TINYINT'
        assert columns_by_name['smallint_col'].data_type == 'SMALLINT'
        assert columns_by_name['integer_col'].data_type in ['INTEGER', 'INT']
        assert columns_by_name['bigint_col'].data_type == 'BIGINT'
        assert columns_by_name['hugeint_col'].data_type == 'HUGEINT'
        
        # Unsigned integers
        assert columns_by_name['utinyint_col'].data_type == 'UTINYINT'
        assert columns_by_name['usmallint_col'].data_type == 'USMALLINT'
        assert columns_by_name['uinteger_col'].data_type in ['UINTEGER', 'UINT']
        assert columns_by_name['ubigint_col'].data_type == 'UBIGINT'
        
        # Floating point
        assert columns_by_name['real_col'].data_type in ['REAL', 'FLOAT']
        assert columns_by_name['double_col'].data_type == 'DOUBLE'
        
        # Fixed precision (DuckDB treats NUMERIC and DECIMAL as the same)
        assert 'DECIMAL' in columns_by_name['decimal_col'].data_type
        assert 'DECIMAL' in columns_by_name['numeric_col'].data_type  # DuckDB converts NUMERIC to DECIMAL
    
    def test_empty_table(self, introspector):
        """Test introspection of empty table."""
        info = introspector.get_table_info('empty_table')
        
        assert info.name == 'empty_table'
        assert len(info.columns) == 2
        
        # Schema should be discoverable even with no data
        columns_by_name = {col.name: col for col in info.columns}
        assert columns_by_name['id'].is_primary_key
        assert columns_by_name['data'].data_type == 'VARCHAR'
    
    def test_get_schema(self, introspector):
        """Test getting complete schema."""
        schema = introspector.get_schema()
        
        assert isinstance(schema, dict)
        assert len(schema) >= 10  # At least 10 tables
        
        # Check a few tables
        assert 'simple_types' in schema
        assert isinstance(schema['simple_types'], TableInfo)
        assert schema['simple_types'].name == 'simple_types'
        
        assert 'json_types' in schema
        assert len(schema['json_types'].columns) >= 9