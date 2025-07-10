"""Notebook-friendly utilities for DuckQL."""

import asyncio
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from functools import wraps

from .core import DuckQL

if TYPE_CHECKING:
    import pandas as pd

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class NotebookDuckQL:
    """Notebook-friendly wrapper for DuckQL with synchronous methods."""
    
    def __init__(self, connection):
        self.duckql = DuckQL(connection)
        self._loop = None
    
    def _ensure_loop(self):
        """Ensure we have an event loop for async operations."""
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
    
    def query(self, graphql_query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query synchronously."""
        self._ensure_loop()
        
        async def _query():
            schema = self.duckql.get_schema()
            result = await schema.execute(graphql_query, variable_values=variables)
            
            if result.errors:
                raise Exception(f"GraphQL errors: {result.errors}")
            
            return result.data
        
        if self._loop.is_running():
            # In Jupyter with nest_asyncio
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(_query())
        else:
            # Regular Python
            return self._loop.run_until_complete(_query())
    
    def query_df(self, graphql_query: str, 
                 variables: Optional[Dict[str, Any]] = None,
                 table_name: Optional[str] = None) -> "pd.DataFrame":
        """Execute a GraphQL query and return results as a pandas DataFrame."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for query_df. Install with: pip install pandas")
        data = self.query(graphql_query, variables)
        
        if table_name:
            # Extract specific table from results
            if table_name in data:
                return pd.DataFrame(data[table_name])
            else:
                raise KeyError(f"Table '{table_name}' not found in query results")
        
        # Try to find the first list result
        for key, value in data.items():
            if isinstance(value, list):
                return pd.DataFrame(value)
        
        # Return the whole result as a single-row DataFrame
        return pd.DataFrame([data])
    
    def tables(self) -> List[str]:
        """Get list of available tables."""
        return self.duckql.introspector.get_tables()
    
    def columns(self, table_name: str) -> List[str]:
        """Get list of columns for a table."""
        info = self.duckql.introspector.get_table_info(table_name)
        return [col.name for col in info.columns]
    
    def schema_info(self, table_name: str) -> "pd.DataFrame":
        """Get schema information for a table as a DataFrame."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for schema_info. Install with: pip install pandas")
        info = self.duckql.introspector.get_table_info(table_name)
        
        data = []
        for col in info.columns:
            data.append({
                'column': col.name,
                'type': col.data_type,
                'nullable': col.is_nullable,
                'primary_key': col.is_primary_key,
                'default': col.default_value
            })
        
        return pd.DataFrame(data)
    
    def computed_field(self, table_name: str, field_name: Optional[str] = None):
        """Decorator for adding computed fields."""
        return self.duckql.computed_field(table_name, field_name)
    
    def resolver(self, name: str):
        """Decorator for adding custom resolvers."""
        # Wrap to make it work synchronously
        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                # Already async, use as is
                return self.duckql.resolver(name)(func)
            else:
                # Make it async
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    return func(*args, **kwargs)
                
                return self.duckql.resolver(name)(async_wrapper)
        
        return decorator
    
    def sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> "pd.DataFrame":
        """Execute raw SQL and return as DataFrame."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for sql. Install with: pip install pandas")
        self._ensure_loop()
        
        async def _sql():
            result = await self.duckql.executor.execute_query(query, params)
            return pd.DataFrame(result.rows)
        
        if self._loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(_sql())
        else:
            return self._loop.run_until_complete(_sql())
    
    def __repr__(self):
        tables = self.tables()
        return f"NotebookDuckQL with {len(tables)} tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}"


# Convenience function for notebooks
def connect(database_path: str = ":memory:") -> NotebookDuckQL:
    """Create a notebook-friendly DuckQL connection."""
    import duckdb
    conn = duckdb.connect(database_path)
    return NotebookDuckQL(conn)