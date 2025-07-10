"""Database introspection for DuckDB schema discovery."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import duckdb


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    default_value: Optional[str] = None


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    columns: List[ColumnInfo]
    primary_keys: List[str]
    indexes: List[str]


class DuckDBIntrospector:
    """Introspects DuckDB database schema."""
    
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.connection = connection
    
    def get_tables(self) -> List[str]:
        """Get all table names in the database."""
        result = self.connection.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
                AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
        return [row[0] for row in result]
    
    def get_views(self) -> List[str]:
        """Get all view names in the database."""
        result = self.connection.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main' 
                AND table_type = 'VIEW'
            ORDER BY table_name
            """
        ).fetchall()
        return [row[0] for row in result]
    
    def get_table_info(self, table_name: str) -> TableInfo:
        """Get detailed information about a table."""
        columns = self._get_columns(table_name)
        primary_keys = self._get_primary_keys(table_name)
        indexes = self._get_indexes(table_name)
        
        # Mark primary key columns
        for col in columns:
            if col.name in primary_keys:
                col.is_primary_key = True
        
        return TableInfo(
            name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            indexes=indexes
        )
    
    def _get_columns(self, table_name: str) -> List[ColumnInfo]:
        """Get column information for a table."""
        result = self.connection.execute(
            """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'main' 
                AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name]
        ).fetchall()
        
        columns = []
        for row in result:
            columns.append(ColumnInfo(
                name=row[0],
                data_type=row[1],
                is_nullable=row[2] == 'YES',
                default_value=row[3]
            ))
        
        return columns
    
    def _get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        try:
            # DuckDB doesn't have a standard way to query constraints
            # Try to infer from PRAGMA or table info
            result = self.connection.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            
            pk_columns = []
            for row in result:
                # Column 5 is pk flag in PRAGMA table_info
                if len(row) > 5 and row[5] > 0:
                    pk_columns.append(row[1])  # Column 1 is name
            
            return pk_columns
        except:
            # Fallback: look for common patterns
            columns = self._get_columns(table_name)
            for col in columns:
                if col.name in ['id', f'{table_name}_id', 'event_id', 'experiment_id']:
                    return [col.name]
            return []
    
    def _get_indexes(self, table_name: str) -> List[str]:
        """Get index names for a table."""
        try:
            # Query for indexes on the table
            result = self.connection.execute(
                """
                SELECT DISTINCT index_name
                FROM duckdb_indexes()
                WHERE table_name = ?
                """,
                [table_name]
            ).fetchall()
            return [row[0] for row in result]
        except:
            # Fallback if duckdb_indexes() is not available
            return []
    
    def get_schema(self) -> Dict[str, TableInfo]:
        """Get complete schema information for all tables."""
        schema = {}
        tables = self.get_tables()
        
        for table_name in tables:
            schema[table_name] = self.get_table_info(table_name)
        
        return schema