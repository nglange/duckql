"""Tests for aggregation queries."""

import pytest
import asyncio
from duckql import DuckQL
from .test_database import create_test_database


class TestAggregation:
    """Test aggregation functionality."""
    
    @pytest.fixture
    def test_db(self):
        """Create test database."""
        return create_test_database()
    
    @pytest.fixture
    def duckql(self, test_db):
        """Create DuckQL instance."""
        return DuckQL(test_db)
    
    async def execute_query(self, duckql: DuckQL, query: str):
        """Execute a GraphQL query and return the result."""
        schema = duckql.get_schema()
        result = await schema.execute(query)
        return result
    
    @pytest.mark.asyncio
    async def test_simple_aggregation(self, duckql):
        """Test basic aggregation query."""
        query = """
        query {
            simpleTypesAggregate {
                _count
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert result.data is not None
        
        agg_results = result.data['simpleTypesAggregate']
        assert len(agg_results) == 1  # One row for total count
        assert agg_results[0]['_count'] == 5
    
    @pytest.mark.asyncio 
    async def test_group_by_aggregation(self, duckql):
        """Test aggregation with GROUP BY."""
        query = """
        query {
            simpleTypesAggregate(groupBy: ["is_active"]) {
                is_active
                _count
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        
        agg_results = result.data['simpleTypesAggregate']
        assert len(agg_results) == 3  # true, false, and null groups
        
        # Check counts
        counts_by_active = {r['is_active']: r['_count'] for r in agg_results}
        assert counts_by_active[True] == 3
        assert counts_by_active[False] == 1
        assert counts_by_active[None] == 1
    
    @pytest.mark.asyncio
    async def test_numeric_aggregations(self, duckql):
        """Test numeric aggregation functions."""
        query = """
        query {
            simpleTypesAggregate {
                _count
                balance_agg {
                    sum
                    avg
                    min
                    max
                }
                age_agg {
                    sum
                    avg
                    min
                    max
                }
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        
        agg = result.data['simpleTypesAggregate'][0]
        
        # Check balance aggregations
        balance_agg = agg['balance_agg']
        assert balance_agg['sum'] is not None
        assert balance_agg['avg'] is not None
        assert float(balance_agg['min']) == -100.50
        assert float(balance_agg['max']) == 2500.00
        
        # Check age aggregations (with nulls)
        age_agg = agg['age_agg']
        assert age_agg['sum'] is not None
        assert age_agg['avg'] is not None
        assert age_agg['min'] == -1
        assert age_agg['max'] == 30
    
    @pytest.mark.asyncio
    async def test_aggregation_with_filter(self, duckql):
        """Test aggregation with WHERE clause."""
        query = """
        query {
            simpleTypesAggregate(
                where: { is_active_eq: true }
            ) {
                _count
                balance_agg {
                    sum
                    avg
                }
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        
        agg = result.data['simpleTypesAggregate'][0]
        assert agg['_count'] == 3  # Only active users
    
    @pytest.mark.asyncio
    async def test_multi_column_group_by(self, duckql):
        """Test GROUP BY with multiple columns."""
        query = """
        query {
            wideTableAggregate(
                groupBy: ["category"]
            ) {
                category
                _count
                value_agg {
                    sum
                    avg
                    min
                    max
                }
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert len(result.data['wideTableAggregate']) > 0
    
    @pytest.mark.asyncio
    async def test_having_clause(self, duckql):
        """Test aggregation with HAVING clause."""
        query = """
        query {
            simpleTypesAggregate(
                groupBy: ["is_active"],
                having: { count_gt: 2 }
            ) {
                is_active
                _count
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        
        # Should only return groups with count > 2
        agg_results = result.data['simpleTypesAggregate']
        for result in agg_results:
            assert result['_count'] > 2
    
    @pytest.mark.asyncio
    async def test_json_table_aggregation(self, duckql):
        """Test aggregation on tables with JSON columns."""
        query = """
        query {
            jsonTypesAggregate {
                _count
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        assert result.data['jsonTypesAggregate'][0]['_count'] == 2
    
    @pytest.mark.asyncio
    async def test_empty_result_aggregation(self, test_db):
        """Test aggregation that returns no results."""
        # Add a filter that matches nothing
        duckql = DuckQL(test_db)
        
        query = """
        query {
            simpleTypesAggregate(
                where: { id_gt: 1000 }
            ) {
                _count
            }
        }
        """
        result = await self.execute_query(duckql, query)
        
        assert not result.errors
        # Should return one row with count 0
        assert result.data['simpleTypesAggregate'][0]['_count'] == 0