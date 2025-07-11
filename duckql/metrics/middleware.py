"""Strawberry GraphQL middleware for metrics collection."""

from typing import Any, Callable, Optional
from strawberry.extensions import SchemaExtension
import uuid

from .collector import MetricsCollector


class MetricsMiddleware(SchemaExtension):
    """Strawberry extension for collecting GraphQL query metrics."""
    
    def __init__(self, metrics_collector: MetricsCollector, **kwargs):
        """
        Initialize metrics middleware.
        
        Args:
            metrics_collector: The metrics collector instance to use
        """
        super().__init__(**kwargs)
        self.metrics = metrics_collector
        self._current_metrics = None
        self._query_id = None
    
    async def resolve(self, _next, root, info, **kwargs):
        """Hook into field resolution to track metrics."""
        # Only track root-level queries
        if info.path and len(info.path) == 1:
            # Start tracking on the first root field resolution
            if self._query_id is None:
                self._query_id = str(uuid.uuid4())
                
                # Extract operation info
                operation_type = 'query'
                table_name = None
                
                # Try to get field name as table name
                field_name = info.field_name
                if field_name:
                    if 'Aggregate' in field_name:
                        operation_type = 'aggregate'
                        table_name = field_name.replace('Aggregate', '')
                    elif field_name.endswith('s'):
                        operation_type = 'list'
                        table_name = field_name
                    else:
                        operation_type = 'single'
                        table_name = field_name
                
                # Start metrics
                self._current_metrics = self.metrics.start_query(
                    query_id=self._query_id,
                    operation_type=operation_type,
                    table_name=table_name,
                    graphql_query=info.context.get('query') if hasattr(info, 'context') and info.context else None
                )
            
            try:
                # Continue with resolution - handle both sync and async
                import asyncio
                result = _next(root, info, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
                
                # Complete metrics on successful resolution
                if self._current_metrics:
                    # Count rows if it's a list
                    row_count = None
                    if isinstance(result, list):
                        row_count = len(result)
                    elif result is not None:
                        row_count = 1
                    
                    self.metrics.complete_query(self._current_metrics, row_count=row_count)
                    self._current_metrics = None
                    self._query_id = None
                
                return result
                
            except Exception as e:
                # Complete metrics with error
                if self._current_metrics:
                    self.metrics.complete_query(self._current_metrics, error=str(e))
                    self._current_metrics = None
                    self._query_id = None
                raise
        else:
            # For non-root fields, just pass through
            import asyncio
            result = _next(root, info, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result


def create_metrics_extension(metrics_collector: MetricsCollector) -> type:
    """
    Create a metrics extension class with the given collector.
    
    Args:
        metrics_collector: The metrics collector to use
        
    Returns:
        MetricsMiddleware class configured with the collector
    """
    class ConfiguredMetricsMiddleware(MetricsMiddleware):
        def __init__(self, **kwargs):
            # Pass collector and any other kwargs to parent
            super().__init__(metrics_collector, **kwargs)
    
    return ConfiguredMetricsMiddleware