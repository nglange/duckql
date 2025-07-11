"""Tests for enhanced error handling and exceptions."""

import pytest
import duckdb
import logging
from unittest.mock import Mock, patch

from duckql.exceptions import (
    DuckQLError, SchemaError, QueryError, ConnectionError, 
    ValidationError, FilterError, enhance_duckdb_error
)
from duckql import DuckQL


class TestExceptionClasses:
    """Test custom exception classes."""
    
    def test_duckql_error_basic(self):
        """Test basic DuckQLError functionality."""
        error = DuckQLError(
            message="Test error",
            error_code="TEST_ERROR",
            context={"table": "users"},
            suggestions=["Check the table name", "Verify permissions"]
        )
        
        assert error.message == "Test error"
        assert error.error_code == "TEST_ERROR"
        assert error.context["table"] == "users"
        assert len(error.suggestions) == 2
        assert error.correlation_id is not None
    
    def test_error_to_dict(self):
        """Test error serialization to dict."""
        error = QueryError(
            message="Query failed",
            query="SELECT * FROM users",
            table_name="users"
        )
        
        error_dict = error.to_dict()
        assert error_dict["error"] == "QUERY_ERROR"
        assert error_dict["message"] == "Query failed"
        assert error_dict["context"]["table"] == "users"
        assert "SELECT" in error_dict["context"]["query"]
    
    def test_error_string_representation(self):
        """Test error string formatting."""
        error = SchemaError(
            message="Column not found",
            table_name="orders",
            column_name="total",
            suggestions=["Check column spelling", "Use 'duckql tables' command"]
        )
        
        error_str = str(error)
        assert "[SCHEMA_ERROR]" in error_str
        assert "Column not found" in error_str
        assert "orders" in error_str
        assert "total" in error_str
        assert "Check column spelling" in error_str
    
    def test_connection_error_default_suggestions(self):
        """Test ConnectionError with default suggestions."""
        error = ConnectionError(
            message="Cannot connect to database",
            database_path="/path/to/db.duckdb"
        )
        
        assert len(error.suggestions) > 0
        assert any("exist" in s for s in error.suggestions)
        assert any("permission" in s for s in error.suggestions)


class TestErrorEnhancement:
    """Test DuckDB error enhancement."""
    
    def test_enhance_column_not_found_error(self):
        """Test enhancement of column not found errors."""
        original = Exception("Could not find column 'username' in table 'users'")
        enhanced = enhance_duckdb_error(original, table="users", database="test.db")
        
        assert isinstance(enhanced, SchemaError)
        assert enhanced.message == "Column 'username' not found"
        assert enhanced.context["table"] == "users"
        assert enhanced.context["column"] == "username"
        assert len(enhanced.suggestions) > 0
    
    def test_enhance_type_mismatch_error(self):
        """Test enhancement of type mismatch errors."""
        original = Exception("Cannot compare values of type VARCHAR and type INTEGER")
        enhanced = enhance_duckdb_error(original)
        
        assert isinstance(enhanced, FilterError)
        assert "Type mismatch" in enhanced.message
        assert len(enhanced.suggestions) > 0
        assert any("compatible types" in s for s in enhanced.suggestions)
    
    def test_enhance_table_not_found_error(self):
        """Test enhancement of table not found errors."""
        original = Exception("Catalog Error: Table with name 'products' does not exist!")
        enhanced = enhance_duckdb_error(original, database="store.db")
        
        assert isinstance(enhanced, SchemaError)
        assert enhanced.message == "Table 'products' not found"
        assert enhanced.context["table"] == "products"
        assert any("duckql tables" in s for s in enhanced.suggestions)
    
    def test_enhance_generic_error(self):
        """Test enhancement of generic errors."""
        original = Exception("Something went wrong")
        enhanced = enhance_duckdb_error(original, custom_context="value")
        
        assert isinstance(enhanced, QueryError)
        assert "Database error:" in enhanced.message
        assert enhanced.context["custom_context"] == "value"


class TestLoggingIntegration:
    """Test logging functionality."""
    
    @pytest.fixture
    def db_connection(self):
        """Create test database."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
        return conn
    
    @pytest.mark.asyncio
    async def test_query_logging(self, db_connection, caplog):
        """Test query logging functionality."""
        with caplog.at_level(logging.DEBUG):
            server = DuckQL(db_connection, log_queries=True)
            
            # Execute a query through the GraphQL schema
            schema = server.get_schema()
            result = await schema.execute("""
                query {
                    testTable {
                        id
                        name
                    }
                }
            """)
            
            # Check logs
            assert any("Executing query:" in record.message for record in caplog.records)
            assert any("Query completed" in record.message for record in caplog.records)
    
    @pytest.mark.asyncio
    async def test_slow_query_logging(self, db_connection, caplog):
        """Test slow query logging."""
        with caplog.at_level(logging.WARNING):
            # Set very low threshold to trigger slow query logging
            server = DuckQL(
                db_connection, 
                log_slow_queries=True,
                slow_query_ms=0.1  # Very low threshold
            )
            
            schema = server.get_schema()
            result = await schema.execute("""
                query {
                    testTable {
                        id
                        name
                    }
                }
            """)
            
            # Should have slow query warning
            slow_query_logs = [r for r in caplog.records if "Slow query detected" in r.message]
            assert len(slow_query_logs) > 0
    
    @pytest.mark.asyncio 
    async def test_error_logging_with_correlation_id(self, db_connection, caplog):
        """Test error logging includes correlation IDs."""
        # Create table with proper column but will cause a runtime error
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30)")
        
        server = DuckQL(conn, log_queries=True)
        schema = server.get_schema()
        
        with caplog.at_level(logging.DEBUG):
            # Execute query that will work but we can check for correlation IDs in logs
            result = await schema.execute("""
                query {
                    users {
                        id
                        name
                        age
                    }
                }
            """)
            
            # Check that we have logs with correlation IDs
            all_logs = caplog.records
            
            # Look for logs that contain correlation IDs (they appear as [uuid] in messages)
            import re
            uuid_pattern = r'\[([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\]'
            logs_with_correlation = [
                r for r in all_logs 
                if re.search(uuid_pattern, r.message)
            ]
            
            # Should have at least one log entry with correlation ID
            assert len(logs_with_correlation) > 0, f"No logs with correlation IDs found. All logs: {[r.message for r in all_logs]}"


class TestPerformanceTracking:
    """Test performance tracking functionality."""
    
    @pytest.fixture
    def db_connection(self):
        """Create test database."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE metrics (id INTEGER, value DOUBLE)")
        for i in range(100):
            conn.execute(f"INSERT INTO metrics VALUES ({i}, {i * 1.5})")
        return conn
    
    @pytest.mark.asyncio
    async def test_query_statistics(self, db_connection):
        """Test query execution statistics."""
        server = DuckQL(db_connection)
        schema = server.get_schema()
        
        # Reset stats
        server.reset_stats()
        initial_stats = server.get_stats()
        assert initial_stats["query_count"] == 0
        
        # Execute some queries
        for i in range(5):
            await schema.execute("""
                query {
                    metrics(limit: 10) {
                        id
                        value
                    }
                }
            """)
        
        # Check stats
        stats = server.get_stats()
        assert stats["query_count"] == 5
        assert stats["total_query_time_ms"] > 0
        assert stats["average_query_time_ms"] > 0
    
    def test_stats_reset(self, db_connection):
        """Test statistics reset functionality."""
        server = DuckQL(db_connection)
        
        # Manually increment stats
        server.executor._query_count = 10
        server.executor._total_query_time = 1000.0
        
        # Reset
        server.reset_stats()
        stats = server.get_stats()
        
        assert stats["query_count"] == 0
        assert stats["total_query_time_ms"] == 0.0