"""Integration tests for full GraphQL query flow."""

import pytest
import asyncio
from typing import Any, Dict
import duckdb
import json

from duckql import DuckQL
from .test_database import (
    create_test_database,
    create_ecommerce_database,
    create_analytics_database,
    create_complex_analytics_database
)


class TestIntegration:
    """Test full GraphQL query execution flow."""
    
    @pytest.fixture
    def test_db(self):
        """Create test database."""
        return create_test_database()
    
    @pytest.fixture
    def ecommerce_db(self):
        """Create e-commerce test database."""
        return create_ecommerce_database()
    
    @pytest.fixture
    def analytics_db(self):
        """Create analytics test database."""
        return create_analytics_database()
    
    @pytest.fixture
    def complex_db(self):
        """Create complex analytics database for stress testing."""
        return create_complex_analytics_database()
    
    @pytest.fixture
    def duckql(self, test_db):
        """Create DuckQL instance."""
        return DuckQL(test_db)
    
    @pytest.fixture
    def ecommerce_duckql(self, ecommerce_db):
        """Create DuckQL instance for e-commerce database."""
        return DuckQL(ecommerce_db)
    
    @pytest.fixture
    def analytics_duckql(self, analytics_db):
        """Create DuckQL instance for analytics database."""
        return DuckQL(analytics_db)
    
    async def execute_query(self, duckql: DuckQL, query: str, variables: Dict[str, Any] = None):
        """Execute a GraphQL query and return the result."""
        schema = duckql.get_schema()
        result = await schema.execute(query, variable_values=variables)
        return result
    
    @pytest.mark.asyncio
    async def test_simple_query(self, duckql):
        """Test simple table query."""
        query = """
        query {
            simpleTypes {
                id
                name
                age
                is_active
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert result.data is not None
        assert 'simpleTypes' in result.data
        assert len(result.data['simpleTypes']) == 5
        
        # Check first record
        first = result.data['simpleTypes'][0]
        assert first['id'] == 1
        assert first['name'] == 'Alice'
        assert first['age'] == 25
        assert first['is_active'] is True
    
    @pytest.mark.asyncio
    async def test_query_with_filter(self, duckql):
        """Test query with WHERE clause."""
        query = """
        query {
            simpleTypes(where: { is_active_eq: true }) {
                id
                name
                balance
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert result.data is not None
        
        active_users = result.data['simpleTypes']
        assert len(active_users) == 3  # Alice, Bob, and empty name user
        assert all(user['name'] != 'Charlie' for user in active_users)
    
    @pytest.mark.asyncio
    async def test_query_with_complex_filter(self, duckql):
        """Test query with complex filter conditions."""
        query = """
        query {
            simpleTypes(
                where: {
                    age_gte: 25
                    balance_gt: 0
                }
            ) {
                name
                age
                balance
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        users = result.data['simpleTypes']
        assert len(users) == 2  # Alice and Bob
        
        for user in users:
            assert user['age'] >= 25
            # Balance is a Decimal, which gets serialized as string in JSON
            assert float(user['balance']) > 0
    
    @pytest.mark.asyncio
    async def test_query_with_ordering(self, duckql):
        """Test query with ORDER BY."""
        query = """
        query {
            simpleTypes(orderBy: { age: DESC }) {
                name
                age
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        users = result.data['simpleTypes']
        
        # Check ordering
        ages = [u['age'] for u in users if u['age'] is not None]
        assert ages == sorted(ages, reverse=True)
    
    @pytest.mark.asyncio
    async def test_query_with_pagination(self, duckql):
        """Test query with LIMIT and OFFSET."""
        query = """
        query {
            simpleTypes(limit: 2, offset: 1, orderBy: { id: ASC }) {
                id
                name
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        users = result.data['simpleTypes']
        assert len(users) == 2
        assert users[0]['id'] == 2
        assert users[1]['id'] == 3
    
    @pytest.mark.asyncio
    async def test_json_field_query(self, duckql):
        """Test querying JSON fields."""
        query = """
        query {
            jsonTypes {
                id
                config
                nested_json
                array_json
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        records = result.data['jsonTypes']
        assert len(records) > 0
        
        # Check JSON fields are properly returned
        first = records[0]
        # JSON fields are returned as strings from DuckDB
        import json
        assert json.loads(first['config']) == {"key": "value"}
        assert json.loads(first['nested_json']) == {"nested": {"deep": {"value": 42}}}
        assert json.loads(first['array_json']) == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_array_field_query(self, duckql):
        """Test querying array fields."""
        query = """
        query {
            jsonTypes {
                id
                int_array
                string_array
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        records = result.data['jsonTypes']
        
        first = records[0]
        assert first['int_array'] == [1, 2, 3]
        assert first['string_array'] == ['a', 'b', 'c']
    
    @pytest.mark.asyncio
    async def test_special_characters_query(self, duckql):
        """Test handling of special characters."""
        query = """
        query {
            specialChars(where: { id_eq: 1 }) {
                normal_text
                text_with_quotes
                text_with_unicode
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        records = result.data['specialChars']
        assert len(records) == 1
        
        record = records[0]
        assert record['normal_text'] == 'Normal text'
        assert record['text_with_quotes'] == 'Text with "quotes"'
        assert record['text_with_unicode'] == 'Unicode: 你好世界 🌍'
    
    @pytest.mark.asyncio
    async def test_reserved_words_table(self, duckql):
        """Test querying table with reserved word names."""
        query = """
        query {
            order {
                select
                from_
                where
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert 'order' in result.data
        records = result.data['order']
        assert len(records) == 2
    
    @pytest.mark.asyncio
    async def test_ecommerce_query(self, ecommerce_duckql):
        """Test querying e-commerce schema."""
        query = """
        query {
            orders(orderBy: { order_date: DESC }, limit: 5) {
                order_id
                customer_id
                order_date
                status
                total_amount
            }
        }
        """
        result = await self.execute_query(ecommerce_duckql, query)
        
        assert not result.errors
        orders = result.data['orders']
        assert len(orders) == 5
        
        # Check first order
        first = orders[0]
        assert first['order_id'] == 5
        assert first['status'] in ['completed', 'processing', 'cancelled']
    
    @pytest.mark.asyncio
    async def test_analytics_events_query(self, analytics_duckql):
        """Test querying analytics events."""
        query = """
        query {
            events(where: { event_type_eq: "page_view" }) {
                event_type
                event_timestamp
                user_id
                properties
            }
        }
        """
        result = await self.execute_query(analytics_duckql, query)
        
        assert not result.errors
        events = result.data['events']
        assert len(events) == 3  # We have 3 page_view events in our test data
        
        # Check event properties
        for event in events:
            assert event['event_type'] == 'page_view'
            assert json.loads(event['properties'])['page'] in ['/home', '/signup', '/products']
    
    @pytest.mark.skip(reason="Complex schema stress test - may cause issues")
    @pytest.mark.asyncio
    async def test_complex_wide_table_query(self, complex_db):
        """Test querying wide table with 50+ columns."""
        duckql = DuckQL(complex_db)
        query = """
        query {
            wideMetrics(limit: 10) {
                id
                timestamp
                metric_001
                metric_002
                metric_003
                category
                region
                properties
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        metrics = result.data['wideMetrics']
        assert len(metrics) == 10
    
    @pytest.mark.skip(reason="Computed fields not yet implemented - requires dynamic schema modification")
    @pytest.mark.asyncio
    async def test_computed_field(self, test_db):
        """Test computed fields functionality."""
        duckql = DuckQL(test_db)
        
        # Add computed field
        @duckql.computed_field("simple_types", "full_description")
        def full_description(obj) -> str:
            return f"{obj['name']} - Age: {obj['age']}, Active: {obj['is_active']}"
        
        # Add another computed field for age in months
        @duckql.computed_field("simple_types", "ageInMonths")
        def age_in_months(obj) -> int:
            return obj['age'] * 12 if obj['age'] else 0
        
        query = """
        query {
            simpleTypes(where: { id_eq: 1 }) {
                name
                age
                full_description
                ageInMonths
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        user = result.data['simpleTypes'][0]
        assert user['full_description'] == "Alice - Age: 25, Active: True"
        assert user['ageInMonths'] == 300  # 25 * 12
    
    @pytest.mark.skip(reason="Custom resolvers not yet implemented - requires dynamic schema modification")
    @pytest.mark.asyncio
    async def test_custom_resolver(self, test_db):
        """Test custom resolver functionality."""
        duckql = DuckQL(test_db)
        
        # Add custom resolver
        @duckql.resolver("userStats")
        async def user_stats(root, info) -> dict:
            # Execute custom analytical query
            result = await duckql.executor.execute_query("""
                SELECT 
                    COUNT(*) as total_users,
                    COUNT(CASE WHEN is_active THEN 1 END) as active_users,
                    AVG(balance) as avg_balance
                FROM simple_types
            """)
            return result.rows[0]
        
        query = """
        query {
            userStats {
                total_users
                active_users
                avg_balance
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        stats = result.data['userStats']
        assert stats['total_users'] == 5
        assert stats['active_users'] == 3
        assert stats['avg_balance'] is not None
    
    @pytest.mark.asyncio
    async def test_error_handling(self, duckql):
        """Test error handling for invalid queries."""
        # Non-existent table
        query = """
        query {
            nonExistentTable {
                id
            }
        }
        """
        result = await self.execute_query(duckql, query)
        assert result.errors is not None
        
        # Invalid field
        query = """
        query {
            simpleTypes {
                non_existent_field
            }
        }
        """
        result = await self.execute_query(duckql, query)
        assert result.errors is not None
    
    @pytest.mark.asyncio
    async def test_concurrent_queries(self, duckql):
        """Test concurrent query execution."""
        queries = [
            """query { simpleTypes { id, name } }""",
            """query { jsonTypes { id, config } }""",
            """query { specialChars { id, normal_text } }""",
        ]
        
        # Execute queries concurrently
        tasks = [self.execute_query(duckql, q) for q in queries]
        results = await asyncio.gather(*tasks)
        
        # All should succeed
        for result in results:
            assert not result.errors
            assert result.data is not None


class TestNotebookUsage:
    """Test notebook-friendly usage patterns."""
    
    def test_basic_notebook_workflow(self):
        """Test typical notebook usage pattern."""
        # 1. Connect to database
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE sales (id INT, product TEXT, amount DECIMAL)")
        conn.execute("INSERT INTO sales VALUES (1, 'Widget', 100.50), (2, 'Gadget', 200.75)")
        
        # 2. Create DuckQL instance
        duckql = DuckQL(conn)
        
        # 3. Get schema to explore
        schema = duckql.get_schema()
        assert schema is not None
        
        # 4. Simple query execution (sync wrapper for notebook)
        async def query(q):
            return await schema.execute(q)
        
        # In a real notebook, users might use nest_asyncio or similar
        import asyncio
        result = asyncio.run(query("query { sales { id, product, amount } }"))
        
        assert not result.errors
        assert len(result.data['sales']) == 2
    
    def test_dataframe_conversion(self):
        """Test converting results to pandas DataFrame (common in notebooks)."""
        import pandas as pd
        
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE metrics (
                date DATE,
                metric_name TEXT,
                value DOUBLE
            )
        """)
        conn.execute("""
            INSERT INTO metrics VALUES 
            ('2024-01-01', 'revenue', 1000),
            ('2024-01-01', 'users', 50),
            ('2024-01-02', 'revenue', 1200),
            ('2024-01-02', 'users', 55)
        """)
        
        duckql = DuckQL(conn)
        
        # Execute query
        async def get_metrics():
            schema = duckql.get_schema()
            result = await schema.execute("""
                query {
                    metrics(orderBy: { date: ASC, metric_name: ASC }) {
                        date
                        metric_name
                        value
                    }
                }
            """)
            return result.data['metrics']
        
        metrics = asyncio.run(get_metrics())
        
        # Convert to DataFrame
        df = pd.DataFrame(metrics)
        assert len(df) == 4
        assert list(df.columns) == ['date', 'metric_name', 'value']
        
        # Pivot for analysis
        pivot = df.pivot(index='date', columns='metric_name', values='value')
        assert 'revenue' in pivot.columns
        assert 'users' in pivot.columns