"""Reporters for exporting metrics in various formats."""

import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime

from .collector import MetricsCollector


class MetricsReporter(ABC):
    """Base class for metrics reporters."""
    
    def __init__(self, collector: MetricsCollector):
        """Initialize reporter with a metrics collector."""
        self.collector = collector
    
    @abstractmethod
    def report(self) -> str:
        """Generate a metrics report."""
        pass


class ConsoleReporter(MetricsReporter):
    """Reporter that formats metrics for console output."""
    
    def report(self, include_details: bool = True) -> str:
        """Generate a human-readable metrics report."""
        stats = self.collector.get_stats()
        
        lines = [
            "=== DuckQL Metrics Report ===",
            f"Generated at: {datetime.now().isoformat()}",
            "",
            "📊 Summary:",
            f"  Total Queries: {stats['summary']['total_queries']}",
            f"  Total Errors: {stats['summary']['total_errors']} ({stats['summary']['error_rate']:.1%} error rate)",
            f"  Total Retries: {stats['summary']['total_retries']}",
            f"  Cache Hits: {stats['summary']['cache_hits']} ({stats['summary']['cache_hit_rate']:.1%} hit rate)",
            "",
            "⚡ Performance:",
        ]
        
        if stats['durations_ms']:
            d = stats['durations_ms']
            lines.extend([
                f"  Min: {d['min']:.2f}ms",
                f"  Mean: {d['mean']:.2f}ms",
                f"  Median: {d['median']:.2f}ms",
                f"  P95: {d['p95']:.2f}ms",
                f"  P99: {d['p99']:.2f}ms",
                f"  Max: {d['max']:.2f}ms",
            ])
        else:
            lines.append("  No completed queries")
        
        lines.extend([
            "",
            "📈 Operations:",
        ])
        
        for op, count in stats['operations'].items():
            lines.append(f"  {op}: {count}")
        
        if include_details:
            lines.extend([
                "",
                "🔍 Table Activity:",
            ])
            
            for table, count in sorted(stats['tables']['queries'].items(), 
                                      key=lambda x: x[1], reverse=True)[:10]:
                errors = stats['tables']['errors'].get(table, 0)
                lines.append(f"  {table}: {count} queries, {errors} errors")
            
            if stats['slow_queries']:
                lines.extend([
                    "",
                    "🐌 Slowest Queries:",
                ])
                
                for q in stats['slow_queries'][:5]:
                    lines.append(
                        f"  {q['operation']} on {q['table']}: "
                        f"{q['duration_ms']:.2f}ms ({q['row_count']} rows)"
                    )
            
            if stats['recent_errors']:
                lines.extend([
                    "",
                    "❌ Recent Errors:",
                ])
                
                for e in stats['recent_errors'][:5]:
                    lines.append(
                        f"  {e['table']}: {e['error'][:50]}..."
                        if len(e['error']) > 50 else f"  {e['table']}: {e['error']}"
                    )
        
        return "\n".join(lines)


class JSONReporter(MetricsReporter):
    """Reporter that exports metrics as JSON."""
    
    def report(self, pretty: bool = True) -> str:
        """Generate a JSON metrics report."""
        stats = self.collector.get_stats()
        
        # Add metadata
        report = {
            'timestamp': datetime.now().isoformat(),
            'version': '1.0',
            'metrics': stats
        }
        
        if pretty:
            return json.dumps(report, indent=2)
        else:
            return json.dumps(report)


class PrometheusReporter(MetricsReporter):
    """Reporter that exports metrics in Prometheus format."""
    
    def report(self) -> str:
        """Generate metrics in Prometheus exposition format."""
        stats = self.collector.get_stats()
        lines = []
        
        # Add headers
        lines.extend([
            "# HELP duckql_queries_total Total number of queries executed",
            "# TYPE duckql_queries_total counter",
            f"duckql_queries_total {stats['summary']['total_queries']}",
            "",
            "# HELP duckql_errors_total Total number of query errors",
            "# TYPE duckql_errors_total counter", 
            f"duckql_errors_total {stats['summary']['total_errors']}",
            "",
            "# HELP duckql_retries_total Total number of query retries",
            "# TYPE duckql_retries_total counter",
            f"duckql_retries_total {stats['summary']['total_retries']}",
            "",
            "# HELP duckql_cache_hits_total Total number of cache hits",
            "# TYPE duckql_cache_hits_total counter",
            f"duckql_cache_hits_total {stats['summary']['cache_hits']}",
            "",
        ])
        
        # Operation metrics
        lines.extend([
            "# HELP duckql_queries_by_operation Queries by operation type",
            "# TYPE duckql_queries_by_operation counter",
        ])
        for op, count in stats['operations'].items():
            lines.append(f'duckql_queries_by_operation{{operation="{op}"}} {count}')
        lines.append("")
        
        # Table metrics
        if stats['tables']['queries']:
            lines.extend([
                "# HELP duckql_queries_by_table Queries by table",
                "# TYPE duckql_queries_by_table counter",
            ])
            for table, count in stats['tables']['queries'].items():
                lines.append(f'duckql_queries_by_table{{table="{table}"}} {count}')
            lines.append("")
        
        # Duration metrics
        if stats['durations_ms']:
            d = stats['durations_ms']
            lines.extend([
                "# HELP duckql_query_duration_milliseconds Query duration statistics",
                "# TYPE duckql_query_duration_milliseconds summary",
                f'duckql_query_duration_milliseconds{{quantile="0"}} {d["min"]}',
                f'duckql_query_duration_milliseconds{{quantile="0.5"}} {d["median"]}',
                f'duckql_query_duration_milliseconds{{quantile="0.95"}} {d["p95"]}',
                f'duckql_query_duration_milliseconds{{quantile="0.99"}} {d["p99"]}',
                f'duckql_query_duration_milliseconds{{quantile="1"}} {d["max"]}',
                f'duckql_query_duration_milliseconds_sum {d["mean"] * stats["summary"]["total_queries"]}',
                f'duckql_query_duration_milliseconds_count {stats["summary"]["total_queries"]}',
                "",
            ])
        
        # Row count metrics
        if stats['row_counts']:
            r = stats['row_counts']
            lines.extend([
                "# HELP duckql_rows_returned Total rows returned by queries",
                "# TYPE duckql_rows_returned summary",
                f"duckql_rows_returned_sum {r['total']}",
                f"duckql_rows_returned_count {stats['summary']['total_queries']}",
                "",
            ])
        
        return "\n".join(lines)


class MetricsServer:
    """Simple HTTP server for exposing metrics."""
    
    def __init__(self, collector: MetricsCollector, port: int = 9090):
        """
        Initialize metrics server.
        
        Args:
            collector: The metrics collector to expose
            port: Port to serve metrics on
        """
        self.collector = collector
        self.port = port
        self.prometheus_reporter = PrometheusReporter(collector)
        self.json_reporter = JSONReporter(collector)
    
    def create_app(self):
        """Create FastAPI app for metrics endpoints."""
        from fastapi import FastAPI, Response
        
        app = FastAPI(title="DuckQL Metrics")
        
        @app.get("/metrics")
        async def prometheus_metrics():
            """Prometheus-compatible metrics endpoint."""
            return Response(
                content=self.prometheus_reporter.report(),
                media_type="text/plain; version=0.0.4"
            )
        
        @app.get("/metrics/json")
        async def json_metrics():
            """JSON metrics endpoint."""
            return Response(
                content=self.json_reporter.report(),
                media_type="application/json"
            )
        
        @app.get("/health")
        async def health():
            """Health check endpoint."""
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}
        
        return app
    
    def run(self):
        """Run the metrics server."""
        import uvicorn
        app = self.create_app()
        uvicorn.run(app, host="0.0.0.0", port=self.port, log_level="warning")