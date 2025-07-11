"""Async query execution for DuckDB."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Set, Type
import duckdb
from dataclasses import dataclass
import threading
import queue
import os
import time
import logging
import uuid
from functools import wraps
from datetime import datetime

from ..exceptions import (
    DuckQLError, QueryError, ConnectionError, 
    enhance_duckdb_error
)
from ..metrics import MetricsCollector, QueryMetrics

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a database query."""
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int


# Default retryable error types
DEFAULT_RETRYABLE_ERRORS = {
    duckdb.ConnectionException,
    duckdb.IOException,
    RuntimeError,  # For connection pool errors
}


def with_retry(max_retries: int = 3, 
               delay: float = 0.1, 
               backoff: float = 2.0,
               retryable_errors: Optional[Set[Type[Exception]]] = None):
    """
    Decorator to retry a function on specific errors with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for exponential backoff
        retryable_errors: Set of exception types to retry on
    """
    if retryable_errors is None:
        retryable_errors = DEFAULT_RETRYABLE_ERRORS
        
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if this is a retryable error
                    if not any(isinstance(e, error_type) for error_type in retryable_errors):
                        # Non-retryable error, raise immediately
                        raise
                    
                    if attempt < max_retries:
                        logger.warning(
                            f"Query failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {current_delay:.2f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Query failed after {max_retries + 1} attempts: {e}"
                        )
            
            # All retries exhausted
            raise last_exception
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if this is a retryable error
                    if not any(isinstance(e, error_type) for error_type in retryable_errors):
                        # Non-retryable error, raise immediately
                        raise
                    
                    if attempt < max_retries:
                        logger.warning(
                            f"Query failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {current_delay:.2f}s..."
                        )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Query failed after {max_retries + 1} attempts: {e}"
                        )
            
            # All retries exhausted
            raise last_exception
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper
    
    return decorator


class ConnectionPool:
    """Thread-safe connection pool for DuckDB."""
    
    def __init__(self, database_path: str, max_connections: int = 4):
        self.database_path = database_path
        self.max_connections = max_connections
        self._pool = queue.Queue(maxsize=max_connections)
        self._created_connections = 0
        self._lock = threading.Lock()
        
        # Pre-create connections
        for _ in range(max_connections):
            self._create_connection()
    
    def _create_connection(self) -> None:
        """Create a new connection and add it to the pool."""
        if self._created_connections < self.max_connections:
            if self.database_path == ":memory:":
                # For in-memory databases, we need to use a shared cache
                # or copy the schema to each connection
                conn = duckdb.connect(":memory:")
            else:
                conn = duckdb.connect(self.database_path)
            
            self._pool.put(conn)
            self._created_connections += 1
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a connection from the pool."""
        try:
            return self._pool.get(block=True, timeout=10)
        except queue.Empty:
            raise RuntimeError("Unable to get database connection from pool")
    
    def return_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Return a connection to the pool."""
        try:
            self._pool.put(conn, block=False)
        except queue.Full:
            # Pool is full, close this connection
            conn.close()
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                conn = self._pool.get(block=False)
                conn.close()
            except queue.Empty:
                break


class QueryExecutor:
    """Executes SQL queries against DuckDB with async support."""
    
    def __init__(self, 
                 connection: duckdb.DuckDBPyConnection, 
                 max_workers: int = 4,
                 max_retries: int = 3,
                 retry_delay: float = 0.1,
                 retry_backoff: float = 2.0,
                 retryable_errors: Optional[Set[Type[Exception]]] = None,
                 log_queries: bool = False,
                 log_slow_queries: bool = True,
                 slow_query_ms: int = 1000,
                 metrics_collector: Optional[MetricsCollector] = None):
        """
        Initialize the query executor with retry capabilities.
        
        Args:
            connection: DuckDB connection to use
            max_workers: Maximum number of worker threads
            max_retries: Maximum number of retry attempts for failed queries
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
            retryable_errors: Set of exception types to retry on
            log_queries: Whether to log all SQL queries at DEBUG level
            log_slow_queries: Whether to log slow queries at WARNING level
            slow_query_ms: Threshold in milliseconds for slow query logging
            metrics_collector: Optional metrics collector instance
        """
        # Store the original connection to copy its schema
        self.original_connection = connection
        
        # Retry configuration
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retryable_errors = retryable_errors or DEFAULT_RETRYABLE_ERRORS
        
        # Logging configuration
        self.log_queries = log_queries
        self.log_slow_queries = log_slow_queries
        self.slow_query_ms = slow_query_ms
        
        # Performance tracking
        self._query_count = 0
        self._total_query_time = 0.0
        self._lock = threading.Lock()
        
        # Metrics collection
        self.metrics = metrics_collector
        
        # Determine database path
        if hasattr(connection, 'db'):
            # Try to get the database path from the connection
            database_path = ":memory:"  # Default for in-memory
        else:
            database_path = ":memory:"
        
        # Create connection pool  
        self.connection_pool = ConnectionPool(database_path, max_workers)
        
        # If original is in-memory, copy schema to all pool connections
        if database_path == ":memory:":
            self._copy_schema_to_pool()
        
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def _copy_schema_to_pool(self) -> None:
        """Copy schema from original connection to all pool connections."""
        # For in-memory databases, we need to recreate the schema and data
        # in each connection since they can't share the same memory space
        
        try:
            # Get all table names
            result = self.original_connection.execute("SHOW TABLES")
            tables = [row[0] for row in result.fetchall()]
            
            # For each connection in pool, copy schema and data
            for _ in range(self.connection_pool.max_connections):
                conn = self.connection_pool.get_connection()
                try:
                    # Copy each table
                    for table in tables:
                        try:
                            # Quote table name to handle reserved words
                            quoted_table = f'"{table}"'
                            
                            # Get table schema by creating a temp table and copying structure
                            result = self.original_connection.execute(f"SELECT * FROM {quoted_table} LIMIT 0")
                            columns_info = result.description
                            
                            if columns_info:
                                # Use CREATE TABLE AS to preserve exact schema and copy data
                                create_sql = f"CREATE TABLE {table} AS SELECT * FROM original_db.{table}"
                                
                                # First attach the original database temporarily to copy from it
                                # Since we can't easily attach in-memory dbs, we'll use a different approach
                                # Let's copy the data with proper types by using the original connection's result
                                
                                # Get the full result with data
                                result = self.original_connection.execute(f"SELECT * FROM {quoted_table}")
                                rows = result.fetchall()
                                
                                if rows:
                                    # Use the first row to infer better column types
                                    sample_row = rows[0]
                                    column_defs = []
                                    
                                    for i, col_info in enumerate(columns_info):
                                        col_name = col_info[0]
                                        sample_value = sample_row[i] if i < len(sample_row) else None
                                        
                                        # Infer type from sample value
                                        if sample_value is None:
                                            col_type = "VARCHAR"
                                        elif isinstance(sample_value, bool):
                                            col_type = "BOOLEAN"
                                        elif isinstance(sample_value, int):
                                            col_type = "INTEGER"
                                        elif isinstance(sample_value, float):
                                            col_type = "DOUBLE"
                                        elif hasattr(sample_value, '__class__') and 'Decimal' in str(sample_value.__class__):
                                            col_type = "DECIMAL"
                                        elif isinstance(sample_value, list):
                                            col_type = "VARCHAR[]"  # Array type
                                        else:
                                            # Check string representation for numeric values
                                            if isinstance(sample_value, str):
                                                try:
                                                    # Test if it's a number
                                                    if '.' in sample_value:
                                                        float(sample_value)
                                                        col_type = "DOUBLE"
                                                    else:
                                                        int(sample_value)
                                                        col_type = "INTEGER"
                                                except ValueError:
                                                    col_type = "VARCHAR"
                                            else:
                                                col_type = "VARCHAR"
                                            
                                        # Quote column name to handle reserved words
                                        quoted_col_name = f'"{col_name}"'
                                        column_defs.append(f"{quoted_col_name} {col_type}")
                                    
                                    create_sql = f"CREATE TABLE {quoted_table} ({', '.join(column_defs)})"
                                else:
                                    # No data, fall back to VARCHAR columns
                                    column_defs = []
                                    for col_info in columns_info:
                                        col_name = col_info[0]
                                        quoted_col_name = f'"{col_name}"'
                                        column_defs.append(f"{quoted_col_name} VARCHAR")
                                    create_sql = f"CREATE TABLE {quoted_table} ({', '.join(column_defs)})"
                                
                                conn.execute(create_sql)
                                
                                # Copy data if we have it
                                if rows:
                                    # Insert data
                                    placeholders = ', '.join(['?' for _ in columns_info])
                                    insert_sql = f"INSERT INTO {quoted_table} VALUES ({placeholders})"
                                    
                                    for row in rows:
                                        conn.execute(insert_sql, row)
                                        
                        except Exception as e:
                            # Skip failed tables but continue with others
                            logger.warning(f"Failed to copy table {table}: {e}")
                            continue
                            
                finally:
                    self.connection_pool.return_connection(conn)
                    
        except Exception as e:
            logger.warning(f"Failed to copy schema to connection pool: {e}")
            # Continue with empty connections - at least they won't conflict
    
    async def execute_query(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute a SQL query asynchronously with logging and tracking."""
        correlation_id = context.get("correlation_id") if context else None
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
        
        # Start metrics tracking
        query_metrics = None
        if self.metrics and context:
            query_metrics = self.metrics.start_query(
                query_id=correlation_id,
                operation_type=context.get('operation', 'query'),
                table_name=context.get('table'),
                sql_query=sql,
                graphql_query=context.get('graphql_query'),
                context=context
            )
        
        # Log query if enabled
        if self.log_queries:
            logger.debug(
                f"[{correlation_id}] Executing query: {sql[:200]}{'...' if len(sql) > 200 else ''}", 
                extra={"correlation_id": correlation_id, "sql": sql, "params": params}
            )
        
        start_time = time.time()
        loop = asyncio.get_event_loop()
        
        try:
            # Run the query in a thread pool
            result = await loop.run_in_executor(
                self.executor,
                self._execute_sync,
                sql,
                params,
                correlation_id
            )
            
            # Track performance
            execution_time = (time.time() - start_time) * 1000  # Convert to ms
            
            with self._lock:
                self._query_count += 1
                self._total_query_time += execution_time
            
            # Complete metrics tracking
            if query_metrics:
                self.metrics.complete_query(query_metrics, row_count=result.row_count)
            
            # Log slow queries
            if self.log_slow_queries and execution_time > self.slow_query_ms:
                logger.warning(
                    f"[{correlation_id}] Slow query detected: {execution_time:.2f}ms - {sql[:200]}{'...' if len(sql) > 200 else ''}",
                    extra={
                        "correlation_id": correlation_id,
                        "execution_time_ms": execution_time,
                        "sql": sql,
                        "row_count": result.row_count
                    }
                )
            elif self.log_queries:
                logger.debug(
                    f"[{correlation_id}] Query completed in {execution_time:.2f}ms, returned {result.row_count} rows",
                    extra={
                        "correlation_id": correlation_id,
                        "execution_time_ms": execution_time,
                        "row_count": result.row_count
                    }
                )
            
            return result
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            # Complete metrics tracking with error
            if query_metrics:
                self.metrics.complete_query(query_metrics, error=str(e))
            
            # Log the error
            logger.error(
                f"[{correlation_id}] Query failed after {execution_time:.2f}ms: {str(e)}",
                extra={
                    "correlation_id": correlation_id,
                    "execution_time_ms": execution_time,
                    "sql": sql,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            
            # Re-raise with enhanced error
            error_context = context.copy() if context else {}
            error_context.update({
                'correlation_id': correlation_id,
                'sql': sql,
                'params': params,
                'execution_time_ms': execution_time
            })
            raise enhance_duckdb_error(e, **error_context)
    
    def _execute_sync(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None
    ) -> QueryResult:
        """Execute a SQL query synchronously using connection pool."""
        # Create a retryable version of the execution logic
        @with_retry(
            max_retries=self.max_retries,
            delay=self.retry_delay,
            backoff=self.retry_backoff,
            retryable_errors=self.retryable_errors
        )
        def execute_with_retry():
            # Get a connection from the pool
            conn = self.connection_pool.get_connection()
            
            try:
                # Replace parameter placeholders with DuckDB style
                if params:
                    # Convert $p0, $p1, etc. to $1, $2, etc.
                    sql_with_params = sql
                    param_values = []
                    for i in range(len(params)):
                        param_name = f"p{i}"
                        if param_name in params:
                            param_values.append(params[param_name])
                            sql_with_params = sql_with_params.replace(f"${param_name}", f"${i + 1}")
                    
                    # Execute with parameters
                    result = conn.execute(sql_with_params, param_values)
                else:
                    # Execute without parameters
                    result = conn.execute(sql)
                
                # Fetch all results
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description] if result.description else []
                
                return rows, columns
            finally:
                # Always return the connection to the pool
                self.connection_pool.return_connection(conn)
        
        # Execute with retry logic
        rows, columns = execute_with_retry()
        
        # Convert to list of dicts
        row_dicts = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert special types to JSON-serializable formats
                if isinstance(value, memoryview):
                    value = value.tobytes().decode('utf-8', errors='replace')
                elif hasattr(value, 'isoformat'):
                    value = value.isoformat()
                # DuckDB may return dates as strings already, leave them as is
                row_dict[col] = value
            row_dicts.append(row_dict)
        
        return QueryResult(
            rows=row_dicts,
            columns=columns,
            row_count=len(row_dicts)
        )
    
    async def execute_many(
        self, 
        queries: List[tuple[str, Optional[Dict[str, Any]]]]
    ) -> List[QueryResult]:
        """Execute multiple queries concurrently."""
        tasks = []
        for sql, params in queries:
            task = self.execute_query(sql, params)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get query execution statistics."""
        with self._lock:
            avg_time = self._total_query_time / self._query_count if self._query_count > 0 else 0
            return {
                "query_count": self._query_count,
                "total_query_time_ms": self._total_query_time,
                "average_query_time_ms": avg_time,
                "connection_pool_size": self.connection_pool.max_connections,
                "max_retries": self.max_retries,
                "slow_query_threshold_ms": self.slow_query_ms
            }
    
    def reset_stats(self) -> None:
        """Reset query execution statistics."""
        with self._lock:
            self._query_count = 0
            self._total_query_time = 0.0
    
    def close(self):
        """Close the executor and connection pool."""
        # Log final statistics
        stats = self.get_stats()
        if stats["query_count"] > 0:
            logger.info(
                f"QueryExecutor closing. Executed {stats['query_count']} queries, "
                f"average time: {stats['average_query_time_ms']:.2f}ms"
            )
        
        self.executor.shutdown(wait=True)
        self.connection_pool.close_all()