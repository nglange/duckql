"""Tests for GraphQL to SQL translation."""

import pytest
from duckql.execution.translator import GraphQLToSQLTranslator, QueryContext


class TestGraphQLToSQLTranslator:
    """Test GraphQL to SQL query translation."""
    
    @pytest.fixture
    def translator(self):
        """Create a translator instance."""
        return GraphQLToSQLTranslator()
    
    def test_simple_select_all(self, translator):
        """Test simple SELECT * query."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*']
        )
        
        assert 'SELECT *' in sql
        assert 'FROM users' in sql
        assert len(params) == 0
    
    def test_select_specific_fields(self, translator):
        """Test selecting specific fields."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['id', 'name', 'email']
        )
        
        assert 'SELECT' in sql
        assert 'id' in sql
        assert 'name' in sql
        assert 'email' in sql
        assert 'FROM users' in sql
    
    def test_simple_where_clause(self, translator):
        """Test simple WHERE clause."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={'id': 1}
        )
        
        assert 'WHERE' in sql
        assert 'id = $p0' in sql
        assert params['p0'] == 1
    
    def test_multiple_where_conditions(self, translator):
        """Test multiple WHERE conditions."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'is_active': True,
                'age_gte': 18
            }
        )
        
        assert 'WHERE' in sql
        assert 'is_active = $p0' in sql
        assert 'age >= $p1' in sql
        assert params['p0'] is True
        assert params['p1'] == 18
    
    def test_comparison_operators(self, translator):
        """Test various comparison operators."""
        sql, params = translator.translate_query(
            table_name='products',
            selections=['*'],
            where={
                'price_gt': 10,
                'price_lt': 100,
                'stock_gte': 5,
                'stock_lte': 50,
                'name_ne': 'test'
            }
        )
        
        assert 'price > $p0' in sql
        assert 'price < $p1' in sql
        assert 'stock >= $p2' in sql
        assert 'stock <= $p3' in sql
        assert 'name != $p4' in sql
        
        assert params['p0'] == 10
        assert params['p1'] == 100
        assert params['p2'] == 5
        assert params['p3'] == 50
        assert params['p4'] == 'test'
    
    def test_string_operators(self, translator):
        """Test string-specific operators."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'name_like': '%john%',
                'email_ilike': '%@EXAMPLE.COM'
            }
        )
        
        assert 'name LIKE $p0' in sql
        assert 'email ILIKE $p1' in sql
        assert params['p0'] == '%john%'
        assert params['p1'] == '%@EXAMPLE.COM'
    
    def test_in_operator(self, translator):
        """Test IN operator."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'status_in': ['active', 'pending', 'verified']
            }
        )
        
        assert 'status IN ($p1, $p2, $p3)' in sql
        assert params['p1'] == 'active'
        assert params['p2'] == 'pending'
        assert params['p3'] == 'verified'
    
    def test_not_in_operator(self, translator):
        """Test NOT IN operator."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'role_not_in': ['admin', 'superuser']
            }
        )
        
        assert 'role NOT IN ($p1, $p2)' in sql
        assert params['p1'] == 'admin'
        assert params['p2'] == 'superuser'
    
    def test_and_operator(self, translator):
        """Test _and logical operator."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                '_and': [
                    {'age_gte': 18},
                    {'age_lt': 65},
                    {'is_active': True}
                ]
            }
        )
        
        assert 'WHERE' in sql
        assert '(age >= $p0 AND age < $p1 AND is_active = $p2)' in sql
        assert params['p0'] == 18
        assert params['p1'] == 65
        assert params['p2'] is True
    
    def test_or_operator(self, translator):
        """Test _or logical operator."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                '_or': [
                    {'role': 'admin'},
                    {'role': 'moderator'},
                    {'is_superuser': True}
                ]
            }
        )
        
        assert 'WHERE' in sql
        assert '((role = $p0) OR (role = $p1) OR (is_superuser = $p2))' in sql
        assert params['p0'] == 'admin'
        assert params['p1'] == 'moderator'
        assert params['p2'] is True
    
    def test_not_operator(self, translator):
        """Test _not logical operator."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                '_not': {
                    'status': 'deleted',
                    'is_banned': True
                }
            }
        )
        
        assert 'WHERE' in sql
        assert 'NOT (status = $p0 AND is_banned = $p1)' in sql
        assert params['p0'] == 'deleted'
        assert params['p1'] is True
    
    def test_complex_nested_conditions(self, translator):
        """Test complex nested logical conditions."""
        sql, params = translator.translate_query(
            table_name='products',
            selections=['*'],
            where={
                '_and': [
                    {'category': 'electronics'},
                    {
                        '_or': [
                            {'price_lt': 100},
                            {'discount_gte': 0.5}
                        ]
                    },
                    {
                        '_not': {
                            'status': 'discontinued'
                        }
                    }
                ]
            }
        )
        
        assert 'WHERE' in sql
        assert 'category = $p0' in sql
        assert 'price < $p1' in sql
        assert 'discount >= $p2' in sql
        assert 'status = $p3' in sql
        assert 'NOT' in sql
        assert 'OR' in sql
        assert 'AND' in sql
    
    def test_order_by(self, translator):
        """Test ORDER BY clause."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            order_by={
                'created_at': 'DESC',
                'name': 'ASC'
            }
        )
        
        assert 'ORDER BY' in sql
        assert 'created_at DESC' in sql
        assert 'name ASC' in sql
    
    def test_limit_offset(self, translator):
        """Test LIMIT and OFFSET."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            limit=10,
            offset=20
        )
        
        assert 'LIMIT 10' in sql
        assert 'OFFSET 20' in sql
    
    def test_full_query(self, translator):
        """Test complete query with all features."""
        sql, params = translator.translate_query(
            table_name='events',
            selections=['event_id', 'event_type', 'timestamp'],
            where={
                '_and': [
                    {'event_type_in': ['click', 'view', 'conversion']},
                    {'timestamp_gte': '2024-01-01'},
                    {'timestamp_lt': '2024-02-01'}
                ]
            },
            order_by={'timestamp': 'DESC'},
            limit=100,
            offset=0
        )
        
        assert 'SELECT' in sql
        assert 'event_id' in sql
        assert 'event_type' in sql
        assert 'timestamp' in sql
        assert 'FROM events' in sql
        assert 'WHERE' in sql
        assert 'ORDER BY timestamp DESC' in sql
        assert 'LIMIT 100' in sql
        assert 'OFFSET 0' in sql
    
    def test_special_characters_in_values(self, translator):
        """Test handling of special characters in values."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'name': "O'Reilly",
                'email': 'user@example.com; DROP TABLE users;--',
                'bio_like': '%50% discount%'
            }
        )
        
        # Values should be parameterized, not embedded
        assert params['p0'] == "O'Reilly"
        assert params['p1'] == 'user@example.com; DROP TABLE users;--'
        assert params['p2'] == '%50% discount%'
        
        # SQL should use parameter placeholders
        assert '$p0' in sql
        assert '$p1' in sql
        assert '$p2' in sql
        
        # No raw values in SQL
        assert "O'Reilly" not in sql
        assert 'DROP TABLE' not in sql
    
    def test_null_values(self, translator):
        """Test handling of null values."""
        sql, params = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={
                'deleted_at': None,  # Should be ignored
                'is_active': True
            }
        )
        
        # Null values should be filtered out
        assert 'deleted_at' not in sql
        assert 'is_active = $p0' in sql
        assert len(params) == 1
        assert params['p0'] is True
    
    def test_empty_where_clause(self, translator):
        """Test query with empty or None where clause."""
        sql1, params1 = translator.translate_query(
            table_name='users',
            selections=['*'],
            where={}
        )
        
        sql2, params2 = translator.translate_query(
            table_name='users',
            selections=['*'],
            where=None
        )
        
        # Both should produce same query without WHERE
        assert 'WHERE' not in sql1
        assert 'WHERE' not in sql2
        assert len(params1) == 0
        assert len(params2) == 0
    
    def test_field_name_with_underscore_suffix(self, translator):
        """Test fields that end with operator-like suffixes."""
        sql, params = translator.translate_query(
            table_name='metrics',
            selections=['*'],
            where={
                'value_min': 10,        # Field named value_min, not value with min operator
                'value_min_gt': 5,      # value_min field with gt operator
                'name_prefix': 'test',  # Field named name_prefix
                'name_prefix_like': 'test%'  # name_prefix field with like operator
            }
        )
        
        assert 'value_min = $p0' in sql
        assert 'value_min > $p1' in sql
        assert 'name_prefix = $p2' in sql
        assert 'name_prefix LIKE $p3' in sql