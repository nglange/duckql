"""Metrics collection for DuckQL queries."""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import statistics


@dataclass
class QueryMetrics:
    """Metrics for a single query execution."""
    
    query_id: str
    operation_type: str  # single, list, aggregate
    table_name: Optional[str]
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    sql_query: Optional[str] = None
    graphql_query: Optional[str] = None
    row_count: Optional[int] = None
    error: Optional[str] = None
    retries: int = 0
    cache_hit: bool = False
    context: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, row_count: Optional[int] = None, error: Optional[str] = None):
        """Mark query as complete and calculate duration."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.row_count = row_count
        self.error = error


class MetricsCollector:
    """Collects and aggregates metrics for DuckQL queries."""
    
    def __init__(self, 
                 max_history: int = 10000,
                 enable_detailed_logging: bool = False):
        """
        Initialize metrics collector.
        
        Args:
            max_history: Maximum number of queries to keep in history
            enable_detailed_logging: Whether to store SQL/GraphQL queries
        """
        self.max_history = max_history
        self.enable_detailed_logging = enable_detailed_logging
        self._queries: List[QueryMetrics] = []
        self._lock = threading.Lock()
        
        # Aggregate counters
        self._total_queries = 0
        self._total_errors = 0
        self._total_retries = 0
        self._cache_hits = 0
        
        # Per-table counters
        self._table_queries: Dict[str, int] = {}
        self._table_errors: Dict[str, int] = {}
        
        # Per-operation counters
        self._operation_counts: Dict[str, int] = {
            'single': 0,
            'list': 0,
            'aggregate': 0,
            'custom': 0
        }
    
    def start_query(self, 
                    query_id: str,
                    operation_type: str,
                    table_name: Optional[str] = None,
                    sql_query: Optional[str] = None,
                    graphql_query: Optional[str] = None,
                    context: Optional[Dict[str, Any]] = None) -> QueryMetrics:
        """Start tracking a new query."""
        metrics = QueryMetrics(
            query_id=query_id,
            operation_type=operation_type,
            table_name=table_name,
            start_time=time.time(),
            sql_query=sql_query if self.enable_detailed_logging else None,
            graphql_query=graphql_query if self.enable_detailed_logging else None,
            context=context or {}
        )
        
        with self._lock:
            self._queries.append(metrics)
            self._total_queries += 1
            
            # Update operation count
            if operation_type in self._operation_counts:
                self._operation_counts[operation_type] += 1
            else:
                self._operation_counts['custom'] += 1
            
            # Update table count
            if table_name:
                self._table_queries[table_name] = self._table_queries.get(table_name, 0) + 1
            
            # Trim history if needed
            if len(self._queries) > self.max_history:
                self._queries = self._queries[-self.max_history:]
        
        return metrics
    
    def complete_query(self, 
                       metrics: QueryMetrics,
                       row_count: Optional[int] = None,
                       error: Optional[str] = None):
        """Complete tracking for a query."""
        metrics.complete(row_count=row_count, error=error)
        
        with self._lock:
            if error:
                self._total_errors += 1
                if metrics.table_name:
                    self._table_errors[metrics.table_name] = \
                        self._table_errors.get(metrics.table_name, 0) + 1
            
            if metrics.retries > 0:
                self._total_retries += metrics.retries
            
            if metrics.cache_hit:
                self._cache_hits += 1
    
    def record_retry(self, metrics: QueryMetrics):
        """Record a retry for a query."""
        with self._lock:
            metrics.retries += 1
    
    def record_cache_hit(self, metrics: QueryMetrics):
        """Record a cache hit for a query."""
        with self._lock:
            metrics.cache_hit = True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics."""
        with self._lock:
            completed_queries = [q for q in self._queries if q.duration_ms is not None]
            error_queries = [q for q in completed_queries if q.error is not None]
            successful_queries = [q for q in completed_queries if q.error is None]
            
            # Calculate duration statistics
            durations = [q.duration_ms for q in successful_queries]
            duration_stats = {}
            if durations:
                duration_stats = {
                    'min': min(durations),
                    'max': max(durations),
                    'mean': statistics.mean(durations),
                    'median': statistics.median(durations),
                    'p95': statistics.quantiles(durations, n=20)[18] if len(durations) > 1 else durations[0],
                    'p99': statistics.quantiles(durations, n=100)[98] if len(durations) > 1 else durations[0]
                }
            
            # Calculate row count statistics
            row_counts = [q.row_count for q in successful_queries if q.row_count is not None]
            row_stats = {}
            if row_counts:
                row_stats = {
                    'min': min(row_counts),
                    'max': max(row_counts),
                    'mean': statistics.mean(row_counts),
                    'total': sum(row_counts)
                }
            
            return {
                'summary': {
                    'total_queries': self._total_queries,
                    'total_errors': self._total_errors,
                    'error_rate': self._total_errors / self._total_queries if self._total_queries > 0 else 0,
                    'total_retries': self._total_retries,
                    'cache_hits': self._cache_hits,
                    'cache_hit_rate': self._cache_hits / self._total_queries if self._total_queries > 0 else 0
                },
                'operations': dict(self._operation_counts),
                'tables': {
                    'queries': dict(self._table_queries),
                    'errors': dict(self._table_errors)
                },
                'durations_ms': duration_stats,
                'row_counts': row_stats,
                'recent_errors': [
                    {
                        'query_id': q.query_id,
                        'table': q.table_name,
                        'error': q.error,
                        'duration_ms': q.duration_ms,
                        'timestamp': datetime.fromtimestamp(q.start_time).isoformat()
                    }
                    for q in error_queries[-10:]  # Last 10 errors
                ],
                'slow_queries': [
                    {
                        'query_id': q.query_id,
                        'table': q.table_name,
                        'operation': q.operation_type,
                        'duration_ms': q.duration_ms,
                        'row_count': q.row_count,
                        'timestamp': datetime.fromtimestamp(q.start_time).isoformat()
                    }
                    for q in sorted(successful_queries, key=lambda x: x.duration_ms, reverse=True)[:10]
                ]
            }
    
    def get_query_history(self, 
                          limit: int = 100,
                          table_name: Optional[str] = None,
                          operation_type: Optional[str] = None,
                          include_errors: bool = True) -> List[Dict[str, Any]]:
        """Get recent query history with optional filters."""
        with self._lock:
            queries = self._queries[-limit:]
            
            # Apply filters
            if table_name:
                queries = [q for q in queries if q.table_name == table_name]
            
            if operation_type:
                queries = [q for q in queries if q.operation_type == operation_type]
            
            if not include_errors:
                queries = [q for q in queries if q.error is None]
            
            return [
                {
                    'query_id': q.query_id,
                    'operation': q.operation_type,
                    'table': q.table_name,
                    'duration_ms': q.duration_ms,
                    'row_count': q.row_count,
                    'error': q.error,
                    'retries': q.retries,
                    'cache_hit': q.cache_hit,
                    'timestamp': datetime.fromtimestamp(q.start_time).isoformat(),
                    'sql': q.sql_query,
                    'graphql': q.graphql_query
                }
                for q in queries
            ]
    
    def reset_stats(self):
        """Reset all statistics."""
        with self._lock:
            self._queries.clear()
            self._total_queries = 0
            self._total_errors = 0
            self._total_retries = 0
            self._cache_hits = 0
            self._table_queries.clear()
            self._table_errors.clear()
            self._operation_counts = {
                'single': 0,
                'list': 0,
                'aggregate': 0,
                'custom': 0
            }