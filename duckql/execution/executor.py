"""Async query execution for DuckDB."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional
import duckdb
from dataclasses import dataclass


@dataclass
class QueryResult:
    """Result of a database query."""
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int


class QueryExecutor:
    """Executes SQL queries against DuckDB with async support."""
    
    def __init__(self, connection: duckdb.DuckDBPyConnection, max_workers: int = 4):
        self.connection = connection
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def execute_query(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute a SQL query asynchronously."""
        loop = asyncio.get_event_loop()
        
        # Run the query in a thread pool
        result = await loop.run_in_executor(
            self.executor,
            self._execute_sync,
            sql,
            params
        )
        
        return result
    
    def _execute_sync(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute a SQL query synchronously."""
        # Replace parameter placeholders with DuckDB style
        if params:
            # Convert $p0, $p1, etc. to $1, $2, etc.
            param_values = []
            for i in range(len(params)):
                param_name = f"p{i}"
                if param_name in params:
                    param_values.append(params[param_name])
                    sql = sql.replace(f"${param_name}", f"${i + 1}")
            
            # Execute with parameters
            result = self.connection.execute(sql, param_values)
        else:
            # Execute without parameters
            result = self.connection.execute(sql)
        
        # Fetch all results
        rows = result.fetchall()
        columns = [desc[0] for desc in result.description] if result.description else []
        
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
    
    def close(self):
        """Close the executor."""
        self.executor.shutdown(wait=True)