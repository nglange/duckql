"""Tests for query executor with retry logic."""

import pytest
import asyncio
import duckdb
from unittest.mock import Mock, patch, MagicMock
import time

from duckql.execution.executor import QueryExecutor, QueryResult, with_retry, DEFAULT_RETRYABLE_ERRORS
from duckql.exceptions import QueryError


class TestRetryDecorator:
    """Test the retry decorator functionality."""
    
    def test_successful_execution_no_retry(self):
        """Test that successful execution doesn't trigger retries."""
        call_count = 0
        
        @with_retry(max_retries=3, delay=0.01)
        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = successful_func()
        assert result == "success"
        assert call_count == 1
    
    def test_retry_on_retryable_error(self):
        """Test that retryable errors trigger retries."""
        call_count = 0
        
        @with_retry(max_retries=3, delay=0.01)
        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise duckdb.ConnectionException("Connection failed")
            return "success"
        
        result = failing_func()
        assert result == "success"
        assert call_count == 3
    
    def test_non_retryable_error_fails_immediately(self):
        """Test that non-retryable errors fail immediately without retries."""
        call_count = 0
        
        @with_retry(max_retries=3, delay=0.01)
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Invalid value")
        
        with pytest.raises(ValueError):
            failing_func()
        
        assert call_count == 1  # No retries
    
    def test_max_retries_exceeded(self):
        """Test that max retries are respected."""
        call_count = 0
        
        @with_retry(max_retries=2, delay=0.01)
        def always_failing_func():
            nonlocal call_count
            call_count += 1
            raise duckdb.ConnectionException("Connection failed")
        
        with pytest.raises(duckdb.ConnectionException):
            always_failing_func()
        
        assert call_count == 3  # Initial + 2 retries
    
    def test_exponential_backoff(self):
        """Test that exponential backoff is applied."""
        delays = []
        
        @with_retry(max_retries=3, delay=0.1, backoff=2.0)
        def failing_func():
            if delays:  # Not first call
                delays.append(time.time())
            else:
                delays.append(time.time())
            raise duckdb.ConnectionException("Connection failed")
        
        with pytest.raises(duckdb.ConnectionException):
            failing_func()
        
        # Check delays are increasing
        if len(delays) >= 3:
            # First retry delay should be ~0.1s
            assert 0.05 < (delays[1] - delays[0]) < 0.15
            # Second retry delay should be ~0.2s (0.1 * 2)
            assert 0.15 < (delays[2] - delays[1]) < 0.25
    
    @pytest.mark.asyncio
    async def test_async_retry(self):
        """Test retry decorator works with async functions."""
        call_count = 0
        
        @with_retry(max_retries=2, delay=0.01)
        async def async_failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise duckdb.IOException("IO error")
            return "async success"
        
        result = await async_failing_func()
        assert result == "async success"
        assert call_count == 2


class TestQueryExecutor:
    """Test QueryExecutor with retry logic."""
    
    @pytest.fixture
    def mock_connection(self):
        """Create a mock DuckDB connection."""
        conn = MagicMock(spec=duckdb.DuckDBPyConnection)
        return conn
    
    @pytest.fixture
    def executor(self, mock_connection):
        """Create a QueryExecutor instance."""
        return QueryExecutor(
            mock_connection,
            max_workers=2,
            max_retries=2,
            retry_delay=0.01,
            retry_backoff=2.0
        )
    
    def test_executor_initialization(self, mock_connection):
        """Test executor initializes with retry configuration."""
        executor = QueryExecutor(
            mock_connection,
            max_retries=5,
            retry_delay=0.5,
            retry_backoff=1.5
        )
        
        assert executor.max_retries == 5
        assert executor.retry_delay == 0.5
        assert executor.retry_backoff == 1.5
        assert executor.retryable_errors == DEFAULT_RETRYABLE_ERRORS
    
    def test_custom_retryable_errors(self, mock_connection):
        """Test executor with custom retryable errors."""
        custom_errors = {RuntimeError, ValueError}
        executor = QueryExecutor(
            mock_connection,
            retryable_errors=custom_errors
        )
        
        assert executor.retryable_errors == custom_errors
    
    @pytest.mark.asyncio
    async def test_query_with_transient_error(self, executor):
        """Test query execution with transient error that succeeds on retry."""
        call_count = 0
        
        def mock_execute(sql, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise duckdb.ConnectionException("Temporary connection error")
            
            # Return mock result
            mock_result = Mock()
            mock_result.fetchall.return_value = [("test", 123)]
            mock_result.description = [("column1",), ("column2",)]
            return mock_result
        
        # Patch connection pool to return a mock connection
        with patch.object(executor.connection_pool, 'get_connection') as mock_get:
            mock_conn = Mock()
            mock_conn.execute.side_effect = mock_execute
            mock_get.return_value = mock_conn
            
            with patch.object(executor.connection_pool, 'return_connection'):
                result = await executor.execute_query("SELECT * FROM test")
        
        assert call_count == 2  # Failed once, succeeded on retry
        assert result.row_count == 1
        assert result.columns == ["column1", "column2"]
    
    @pytest.mark.asyncio
    async def test_query_with_non_retryable_error(self, executor):
        """Test query execution with non-retryable error fails immediately."""
        call_count = 0
        
        def mock_execute(sql, params=None):
            nonlocal call_count
            call_count += 1
            raise duckdb.InvalidInputException("Invalid SQL syntax")
        
        # Patch connection pool to return a mock connection
        with patch.object(executor.connection_pool, 'get_connection') as mock_get:
            mock_conn = Mock()
            mock_conn.execute.side_effect = mock_execute
            mock_get.return_value = mock_conn
            
            with patch.object(executor.connection_pool, 'return_connection'):
                with pytest.raises(QueryError) as exc_info:
                    await executor.execute_query("INVALID SQL")
                
                # Verify it's a non-retryable error that was enhanced
                assert "Invalid SQL syntax" in str(exc_info.value)
        
        assert call_count == 1  # No retries for non-retryable errors
    
    def test_connection_pool_cleanup_on_error(self, executor):
        """Test that connections are returned to pool even on errors."""
        return_called = False
        
        def mock_return_connection(conn):
            nonlocal return_called
            return_called = True
        
        with patch.object(executor.connection_pool, 'get_connection') as mock_get:
            mock_conn = Mock()
            mock_conn.execute.side_effect = duckdb.ConnectionException("Error")
            mock_get.return_value = mock_conn
            
            with patch.object(executor.connection_pool, 'return_connection', side_effect=mock_return_connection):
                # Use sync method directly to test
                with pytest.raises(duckdb.ConnectionException):
                    executor._execute_sync("SELECT 1")
        
        assert return_called  # Connection was returned despite error


class TestIntegrationWithRetry:
    """Integration tests with actual DuckDB connection."""
    
    @pytest.fixture
    def db_connection(self):
        """Create an in-memory DuckDB connection."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")
        return conn
    
    @pytest.fixture
    def executor(self, db_connection):
        """Create executor with retry enabled."""
        return QueryExecutor(
            db_connection,
            max_retries=2,
            retry_delay=0.01
        )
    
    @pytest.mark.asyncio
    async def test_successful_query_execution(self, executor):
        """Test successful query execution with real database."""
        result = await executor.execute_query("SELECT * FROM test_table ORDER BY id")
        
        assert result.row_count == 2
        assert result.columns == ["id", "name"]
        assert result.rows[0]["id"] == 1
        assert result.rows[0]["name"] == "Alice"
    
    @pytest.mark.asyncio
    async def test_concurrent_queries_with_retry(self, executor):
        """Test concurrent query execution with retry logic."""
        queries = [
            "SELECT COUNT(*) as count FROM test_table",
            "SELECT MAX(id) as max_id FROM test_table",
            "SELECT MIN(id) as min_id FROM test_table"
        ]
        
        tasks = [executor.execute_query(q) for q in queries]
        results = await asyncio.gather(*tasks)
        
        assert len(results) == 3
        assert results[0].rows[0]["count"] == 2
        assert results[1].rows[0]["max_id"] == 2
        assert results[2].rows[0]["min_id"] == 1