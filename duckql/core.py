"""Core DuckQL implementation."""

from typing import Dict, Any, Optional, List, Callable, Type
import duckdb
import strawberry
from strawberry.fastapi import GraphQLRouter
from fastapi import FastAPI
import uvicorn
import logging
import uuid

from .schema import DuckDBIntrospector, TypeBuilder, AggregateTypeBuilder
from .execution import GraphQLToSQLTranslator, QueryExecutor
from .exceptions import DuckQLError, SchemaError, QueryError
from .metrics import MetricsCollector, create_metrics_extension

logger = logging.getLogger(__name__)


class DuckQL:
    """Main DuckQL class for creating GraphQL APIs from DuckDB databases."""
    
    def __init__(self, 
                 connection: duckdb.DuckDBPyConnection,
                 max_workers: int = 4,
                 max_retries: int = 3,
                 retry_delay: float = 0.1,
                 retry_backoff: float = 2.0,
                 log_queries: bool = False,
                 log_slow_queries: bool = True,
                 slow_query_ms: int = 1000,
                 max_query_depth: Optional[int] = None,
                 enable_metrics: bool = True,
                 metrics_history_size: int = 10000):
        """
        Initialize DuckQL with a DuckDB connection.
        
        Args:
            connection: DuckDB database connection
            max_workers: Maximum number of worker threads for query execution
            max_retries: Maximum number of retry attempts for failed queries
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for exponential backoff
            log_queries: Whether to log all SQL queries at DEBUG level
            log_slow_queries: Whether to log slow queries at WARNING level
            slow_query_ms: Threshold in milliseconds for slow query logging
            max_query_depth: Maximum allowed query depth (None for unlimited)
            enable_metrics: Whether to enable metrics collection
            metrics_history_size: Maximum number of queries to keep in metrics history
        """
        self.connection = connection
        self.introspector = DuckDBIntrospector(connection)
        self.type_builder = TypeBuilder()
        self.aggregate_builder = AggregateTypeBuilder(self.type_builder)
        
        # Set up metrics collection
        self.metrics_collector = None
        if enable_metrics:
            self.metrics_collector = MetricsCollector(
                max_history=metrics_history_size,
                enable_detailed_logging=log_queries
            )
        
        self.executor = QueryExecutor(
            connection,
            max_workers=max_workers,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            log_queries=log_queries,
            log_slow_queries=log_slow_queries,
            slow_query_ms=slow_query_ms,
            metrics_collector=self.metrics_collector
        )
        self.translator = GraphQLToSQLTranslator()
        self.max_query_depth = max_query_depth
        
        # Storage for custom resolvers
        self._custom_resolvers: Dict[str, Callable] = {}
        self._computed_fields: Dict[str, Dict[str, Callable]] = {}
        
        # Build schema
        self._schema = None
        self._query_type = None
        self._build_schema()
    
    def _build_schema(self) -> None:
        """Build GraphQL schema from database."""
        # Get all tables
        tables = self.introspector.get_tables()
        
        # Build types for each table
        graphql_types = {}
        for table_name in tables:
            table_info = self.introspector.get_table_info(table_name)
            graphql_type = self.type_builder.build_type(table_info)
            graphql_types[table_name] = graphql_type
        
        # Build Query type with resolvers
        query_fields = {}
        
        for table_name, graphql_type in graphql_types.items():
            # Single item query
            single_field_name = self._to_camel_case(table_name[:-1] if table_name.endswith('s') else table_name)
            query_fields[single_field_name] = self._create_single_resolver(
                table_name, graphql_type
            )
            
            # List query (use the original table name in camelCase)
            list_field_name = self._to_camel_case(table_name)
            query_fields[list_field_name] = self._create_list_resolver(
                table_name, graphql_type
            )
            
            # Aggregate query
            aggregate_field_name = self._to_camel_case(table_name) + "Aggregate"
            table_info = self.introspector.get_table_info(table_name)
            query_fields[aggregate_field_name] = self.aggregate_builder.create_aggregate_resolver(
                table_name, table_info, self.executor, self.translator
            )
        
        # Add custom resolvers
        query_fields.update(self._custom_resolvers)
        
        # Create Query type
        Query = type("Query", (), query_fields)
        self._query_type = strawberry.type(Query)
        
        # Create schema with extensions
        extensions = []
        if self.max_query_depth is not None:
            from .validation import create_depth_limit_extension
            extensions.append(create_depth_limit_extension(self.max_query_depth))
        
        if self.metrics_collector:
            extensions.append(create_metrics_extension(self.metrics_collector))
        
        self._schema = strawberry.Schema(
            query=self._query_type,
            extensions=extensions
        )
    
    def _create_single_resolver(self, table_name: str, graphql_type: Type) -> Any:
        """Create a resolver for fetching a single item."""
        filter_type = self.type_builder.get_filter_type(table_name)
        
        async def resolver(
            root: Any,
            info: Any,
            where: Optional[filter_type] = None
        ) -> Optional[graphql_type]:
            correlation_id = str(uuid.uuid4())
            
            try:
                # Get requested fields
                selections = self._get_selections(info, table_name)
                
                # Convert where to dict
                where_dict = None
                if where:
                    where_dict = self._input_to_dict(where)
                
                # Translate to SQL
                sql, params = self.translator.translate_query(
                    table_name=table_name,
                    selections=selections,
                    where=where_dict,
                    limit=1
                )
                
                # Execute query with context
                context = {
                    "correlation_id": correlation_id,
                    "table": table_name,
                    "operation": "single",
                    "graphql_query": info.operation.loc.source.body if hasattr(info, 'operation') else None
                }
                
                result = await self.executor.execute_query(sql, params, context)
                
                if result.rows:
                    # Convert to graphql type instance
                    return self._dict_to_type(result.rows[0], graphql_type)
                
                return None
                
            except DuckQLError:
                # Re-raise our errors as-is
                raise
            except Exception as e:
                # Wrap other errors
                logger.error(f"[{correlation_id}] Error in single resolver for {table_name}: {str(e)}")
                raise QueryError(
                    f"Failed to fetch {table_name} record",
                    table_name=table_name,
                    operation="single",
                    correlation_id=correlation_id,
                    context={"original_error": str(e)}
                )
        
        # Create field with proper type annotations
        return strawberry.field(
            resolver=resolver,
            description=f"Fetch a single {table_name} record"
        )
    
    def _create_list_resolver(self, table_name: str, graphql_type: Type) -> Any:
        """Create a resolver for fetching a list of items."""
        filter_type = self.type_builder.get_filter_type(table_name)
        order_by_type = self.type_builder.get_order_by_type(table_name)
        
        async def resolver(
            root: Any,
            info: Any,
            where: Optional[filter_type] = None,
            order_by: Optional[order_by_type] = None,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
        ) -> List[graphql_type]:
            correlation_id = str(uuid.uuid4())
            
            try:
                # Get requested fields
                selections = self._get_selections(info, table_name)
                
                # Convert inputs to dicts
                where_dict = self._input_to_dict(where) if where else None
                order_by_dict = self._input_to_dict(order_by) if order_by else None
                
                # Translate to SQL
                sql, params = self.translator.translate_query(
                    table_name=table_name,
                    selections=selections,
                    where=where_dict,
                    order_by=order_by_dict,
                    limit=limit,
                    offset=offset
                )
                
                # Execute query with context
                context = {
                    "correlation_id": correlation_id,
                    "table": table_name,
                    "operation": "list",
                    "graphql_query": info.operation.loc.source.body if hasattr(info, 'operation') else None
                }
                
                result = await self.executor.execute_query(sql, params, context)
                
                # Convert to graphql type instances
                return [
                    self._dict_to_type(row, graphql_type) 
                    for row in result.rows
                ]
                
            except DuckQLError:
                # Re-raise our errors as-is
                raise
            except Exception as e:
                # Wrap other errors
                logger.error(f"[{correlation_id}] Error in list resolver for {table_name}: {str(e)}")
                raise QueryError(
                    f"Failed to fetch {table_name} records",
                    table_name=table_name,
                    operation="list",
                    correlation_id=correlation_id,
                    context={"original_error": str(e)}
                )
        
        # Create field with proper type annotations
        return strawberry.field(
            resolver=resolver,
            description=f"Fetch a list of {table_name} records"
        )
    
    def computed_field(self, table_name: str, field_name: Optional[str] = None) -> Callable:
        """Decorator for adding computed fields to a type."""
        def decorator(func: Callable) -> Callable:
            nonlocal field_name
            if field_name is None:
                field_name = func.__name__
            
            # Store for runtime resolution
            if table_name not in self._computed_fields:
                self._computed_fields[table_name] = {}
            
            self._computed_fields[table_name][field_name] = func
            
            # Get return type from function annotation
            import inspect
            sig = inspect.signature(func)
            return_type = sig.return_annotation
            if return_type == inspect.Signature.empty:
                return_type = str  # Default to string if no annotation
            
            # Register with type builder
            self.type_builder.register_computed_field(
                table_name, 
                field_name, 
                return_type, 
                func
            )
            
            # Clear existing types to force rebuild
            self.type_builder._types = {}
            self._query_type = None
            self._schema = None
            
            # Rebuild schema to include new field
            self._build_schema()
            
            return func
        
        return decorator
    
    def resolver(self, name: str) -> Callable:
        """Decorator for adding custom resolvers to Query type."""
        def decorator(func: Callable) -> Callable:
            # Store the resolver
            self._custom_resolvers[name] = strawberry.field(resolver=func)
            
            # Rebuild schema to include new resolver
            self._build_schema()
            
            return func
        
        return decorator
    
    def serve(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        path: str = "/graphql",
        debug: bool = True
    ) -> None:
        """Start the GraphQL server."""
        app = FastAPI(title="DuckQL GraphQL API")
        
        # Add GraphQL route
        graphql_app = GraphQLRouter(self._schema, path=path)
        app.include_router(graphql_app, prefix="")
        
        # Add health check
        @app.get("/health")
        async def health():
            return {"status": "healthy"}
        
        logger.info(f"🦆 DuckQL server starting at http://{host}:{port}{path}")
        logger.info(f"📊 GraphQL playground available at http://{host}:{port}{path}")
        
        # Run server
        uvicorn.run(app, host=host, port=port, log_level="info" if debug else "warning")
    
    def get_schema(self) -> strawberry.Schema:
        """Get the generated GraphQL schema."""
        return self._schema
    
    def _get_selections(self, info: Any, table_name: Optional[str] = None) -> List[str]:
        """Extract selected fields from GraphQL info, excluding computed fields."""
        selections = []
        has_computed_fields = False
        
        # Navigate through the selection set
        # Note: field_nodes is deprecated but still the working approach in this context
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            field = info.field_nodes[0]
        
        # Get computed field names for this table
        computed_field_names = set()
        if table_name:
            type_name = self.type_builder._to_pascal_case(table_name)
            if type_name in self.type_builder._computed_fields:
                computed_field_names = set(self.type_builder._computed_fields[type_name].keys())
        
        if hasattr(field, 'selection_set') and field.selection_set:
            for selection in field.selection_set.selections:
                if hasattr(selection, 'name'):
                    field_name = selection.name.value
                    
                    # Check if this is a computed field
                    if field_name in computed_field_names:
                        has_computed_fields = True
                        continue
                    
                    # Map GraphQL field name back to database column name
                    # Handle Python keywords that were converted (e.g. from_ -> from)
                    if field_name.endswith('_') and field_name[:-1] in ['from', 'class', 'import', 'return', 'def', 'for', 'while', 'if', 'else', 'elif', 'try', 'except', 'finally', 'with', 'as', 'yield', 'lambda', 'pass', 'break', 'continue']:
                        field_name = field_name[:-1]
                    selections.append(field_name)
        
        # If we have computed fields, we need to select all columns
        # because computed fields might need access to any column
        if has_computed_fields or not selections:
            selections = ["*"]
        
        return selections
    
    def _input_to_dict(self, input_obj: Any) -> Dict[str, Any]:
        """Convert strawberry input object to dict."""
        if input_obj is None:
            return {}
        
        result = {}
        for key, value in vars(input_obj).items():
            if value is not None:
                # Handle enums
                from enum import Enum
                if isinstance(value, Enum):
                    result[key] = value.value
                elif hasattr(value, '__dict__') and not isinstance(value, (str, int, float, bool)):
                    result[key] = self._input_to_dict(value)
                elif isinstance(value, list):
                    result[key] = [
                        self._input_to_dict(item) if hasattr(item, '__dict__') and not isinstance(item, (str, int, float, bool, Enum)) else item
                        for item in value
                    ]
                else:
                    result[key] = value
        
        return result
    
    def _dict_to_type(self, data: Dict[str, Any], graphql_type: Type) -> Any:
        """Convert dict to GraphQL type instance."""
        # Map database column names to GraphQL field names
        # Handle Python keywords that need underscore suffix
        import keyword
        
        mapped_data = {}
        for key, value in data.items():
            if keyword.iskeyword(key):
                mapped_key = f"{key}_"
            else:
                mapped_key = key
            mapped_data[mapped_key] = value
        
        # Create instance with mapped data
        instance = graphql_type(**mapped_data)
        
        return instance
    
    @staticmethod
    def _to_camel_case(snake_str: str) -> str:
        """Convert snake_case to camelCase."""
        components = snake_str.split('_')
        return components[0] + ''.join(x.capitalize() for x in components[1:])
    
    def get_stats(self) -> Dict[str, Any]:
        """Get query execution statistics."""
        stats = self.executor.get_stats()
        
        # Add metrics if available
        if self.metrics_collector:
            stats['metrics'] = self.metrics_collector.get_stats()
        
        return stats
    
    def reset_stats(self) -> None:
        """Reset query execution statistics."""
        self.executor.reset_stats()
        
        if self.metrics_collector:
            self.metrics_collector.reset_stats()
    
    def get_metrics_report(self, format: str = 'console') -> str:
        """Get a formatted metrics report.
        
        Args:
            format: Report format ('console', 'json', 'prometheus')
            
        Returns:
            Formatted metrics report string
        """
        if not self.metrics_collector:
            return "Metrics collection is disabled"
        
        from .metrics import ConsoleReporter, JSONReporter, PrometheusReporter
        
        reporters = {
            'console': ConsoleReporter,
            'json': JSONReporter,
            'prometheus': PrometheusReporter
        }
        
        reporter_class = reporters.get(format, ConsoleReporter)
        reporter = reporter_class(self.metrics_collector)
        return reporter.report()
    
    def __del__(self):
        """Cleanup resources."""
        if hasattr(self, 'executor'):
            self.executor.close()