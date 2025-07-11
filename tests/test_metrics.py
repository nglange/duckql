"""Tests for metrics collection and reporting."""

import pytest
import asyncio
import time
import json
import duckdb

from duckql import DuckQL
from duckql.metrics import (
    MetricsCollector, 
    QueryMetrics,
    ConsoleReporter,
    JSONReporter,
    PrometheusReporter
)


class TestMetricsCollector:
    """Test the metrics collector."""
    
    def test_start_and_complete_query(self):
        """Test basic query tracking."""
        collector = MetricsCollector()
        
        # Start a query
        metrics = collector.start_query(
            query_id="test-1",
            operation_type="list",
            table_name="users",
            sql_query="SELECT * FROM users"
        )
        
        assert metrics.query_id == "test-1"
        assert metrics.operation_type == "list"
        assert metrics.table_name == "users"
        assert metrics.start_time is not None
        assert metrics.end_time is None
        
        # Complete the query
        time.sleep(0.01)  # Small delay
        collector.complete_query(metrics, row_count=10)
        
        assert metrics.end_time is not None
        assert metrics.duration_ms > 0
        assert metrics.row_count == 10
        assert metrics.error is None
    
    def test_query_with_error(self):
        """Test tracking queries that fail."""
        collector = MetricsCollector()
        
        metrics = collector.start_query(
            query_id="test-2",
            operation_type="single",
            table_name="posts"
        )
        
        # Complete with error
        collector.complete_query(metrics, error="Table not found")
        
        assert metrics.error == "Table not found"
        assert metrics.row_count is None
        
        # Check stats
        stats = collector.get_stats()
        assert stats['summary']['total_errors'] == 1
        assert stats['tables']['errors']['posts'] == 1
    
    def test_retry_tracking(self):
        """Test retry counting."""
        collector = MetricsCollector()
        
        metrics = collector.start_query(
            query_id="test-3",
            operation_type="aggregate",
            table_name="orders"
        )
        
        # Record retries
        collector.record_retry(metrics)
        collector.record_retry(metrics)
        
        assert metrics.retries == 2
        
        collector.complete_query(metrics, row_count=100)
        
        stats = collector.get_stats()
        assert stats['summary']['total_retries'] == 2
    
    def test_aggregated_stats(self):
        """Test aggregated statistics."""
        collector = MetricsCollector()
        
        # Run several queries
        for i in range(5):
            m = collector.start_query(
                query_id=f"test-{i}",
                operation_type="list" if i % 2 == 0 else "single",
                table_name="users" if i < 3 else "posts"
            )
            time.sleep(0.01 * (i + 1))  # Variable delays
            collector.complete_query(m, row_count=i * 10)
        
        # Add one error
        m = collector.start_query(
            query_id="test-error",
            operation_type="aggregate",
            table_name="invalid"
        )
        collector.complete_query(m, error="Invalid table")
        
        stats = collector.get_stats()
        
        # Check summary
        assert stats['summary']['total_queries'] == 6
        assert stats['summary']['total_errors'] == 1
        assert stats['summary']['error_rate'] == 1/6
        
        # Check operations
        assert stats['operations']['list'] == 3
        assert stats['operations']['single'] == 2
        assert stats['operations']['aggregate'] == 1
        
        # Check tables
        assert stats['tables']['queries']['users'] == 3
        assert stats['tables']['queries']['posts'] == 2
        assert stats['tables']['queries']['invalid'] == 1
        
        # Check duration stats
        assert 'durations_ms' in stats
        assert stats['durations_ms']['min'] > 0
        assert stats['durations_ms']['max'] > stats['durations_ms']['min']
        assert stats['durations_ms']['mean'] > 0
    
    def test_query_history(self):
        """Test query history retrieval."""
        collector = MetricsCollector()
        
        # Add queries
        for i in range(10):
            m = collector.start_query(
                query_id=f"hist-{i}",
                operation_type="list",
                table_name=f"table_{i % 3}"
            )
            if i == 5:
                collector.complete_query(m, error="Test error")
            else:
                collector.complete_query(m, row_count=i)
        
        # Get all history
        history = collector.get_query_history(limit=20)
        assert len(history) == 10
        
        # Filter by table
        table_0_history = collector.get_query_history(table_name="table_0")
        assert len(table_0_history) == 4  # 0, 3, 6, 9
        
        # Exclude errors
        success_history = collector.get_query_history(include_errors=False)
        assert len(success_history) == 9
    
    def test_reset_stats(self):
        """Test resetting statistics."""
        collector = MetricsCollector()
        
        # Add some queries
        for i in range(3):
            m = collector.start_query(
                query_id=f"reset-{i}",
                operation_type="list",
                table_name="users"
            )
            collector.complete_query(m, row_count=10)
        
        # Verify stats exist
        stats = collector.get_stats()
        assert stats['summary']['total_queries'] == 3
        
        # Reset
        collector.reset_stats()
        
        # Verify reset
        stats = collector.get_stats()
        assert stats['summary']['total_queries'] == 0
        assert len(collector._queries) == 0


class TestReporters:
    """Test metrics reporters."""
    
    @pytest.fixture
    def populated_collector(self):
        """Create a collector with test data."""
        collector = MetricsCollector()
        
        # Add various queries
        queries = [
            ("list", "users", 50, 100, None),
            ("single", "posts", 10, 1, None),
            ("aggregate", "orders", 200, 500, None),
            ("list", "products", 150, 200, None),
            ("single", "users", 5, 1, "User not found"),
        ]
        
        for i, (op, table, duration, rows, error) in enumerate(queries):
            m = collector.start_query(
                query_id=f"report-{i}",
                operation_type=op,
                table_name=table
            )
            # Simulate duration
            m.start_time -= duration / 1000
            collector.complete_query(m, row_count=rows if not error else None, error=error)
        
        return collector
    
    def test_console_reporter(self, populated_collector):
        """Test console report generation."""
        reporter = ConsoleReporter(populated_collector)
        report = reporter.report()
        
        # Check key sections
        assert "DuckQL Metrics Report" in report
        assert "Summary:" in report
        assert "Performance:" in report
        assert "Operations:" in report
        assert "Table Activity:" in report
        assert "Slowest Queries:" in report
        assert "Recent Errors:" in report
        
        # Check some values
        assert "Total Queries: 5" in report
        assert "Total Errors: 1" in report
    
    def test_json_reporter(self, populated_collector):
        """Test JSON report generation."""
        reporter = JSONReporter(populated_collector)
        report_str = reporter.report()
        
        # Parse JSON
        report = json.loads(report_str)
        
        assert 'timestamp' in report
        assert 'version' in report
        assert 'metrics' in report
        
        metrics = report['metrics']
        assert metrics['summary']['total_queries'] == 5
        assert metrics['summary']['total_errors'] == 1
    
    def test_prometheus_reporter(self, populated_collector):
        """Test Prometheus format generation."""
        reporter = PrometheusReporter(populated_collector)
        report = reporter.report()
        
        # Check Prometheus format
        lines = report.split('\n')
        
        # Check headers
        assert any("# HELP" in line for line in lines)
        assert any("# TYPE" in line for line in lines)
        
        # Check metrics
        assert any("duckql_queries_total 5" in line for line in lines)
        assert any("duckql_errors_total 1" in line for line in lines)
        assert any('duckql_queries_by_operation{operation="list"} 2' in line for line in lines)


class TestDuckQLMetricsIntegration:
    """Test metrics integration with DuckQL."""
    
    @pytest.fixture
    def db_with_data(self):
        """Create a test database."""
        conn = duckdb.connect(":memory:")
        
        conn.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                value DECIMAL(10, 2)
            )
        """)
        
        conn.execute("""
            INSERT INTO test_table VALUES
            (1, 'Item 1', 100.50),
            (2, 'Item 2', 200.75),
            (3, 'Item 3', 300.25)
        """)
        
        return conn
    
    @pytest.mark.asyncio
    async def test_metrics_collection_enabled(self, db_with_data):
        """Test that metrics are collected when enabled."""
        server = DuckQL(db_with_data, enable_metrics=True)
        schema = server.get_schema()
        
        # Run a query
        result = await schema.execute("""
            query {
                testTable {
                    id
                    name
                    value
                }
            }
        """)
        
        assert not result.errors
        
        # Check metrics were collected
        stats = server.get_stats()
        assert 'metrics' in stats
        assert stats['metrics']['summary']['total_queries'] > 0
    
    @pytest.mark.asyncio
    async def test_metrics_collection_disabled(self, db_with_data):
        """Test that metrics can be disabled."""
        server = DuckQL(db_with_data, enable_metrics=False)
        schema = server.get_schema()
        
        # Run a query
        await schema.execute("""
            query {
                testTable {
                    id
                }
            }
        """)
        
        # Check no metrics
        stats = server.get_stats()
        assert 'metrics' not in stats
    
    @pytest.mark.asyncio
    async def test_metrics_report_formats(self, db_with_data):
        """Test different report formats."""
        server = DuckQL(db_with_data, enable_metrics=True)
        schema = server.get_schema()
        
        # Run some queries
        for i in range(3):
            await schema.execute("""
                query {
                    testTable(limit: 1) {
                        id
                    }
                }
            """)
        
        # Test different formats
        console_report = server.get_metrics_report(format='console')
        assert "DuckQL Metrics Report" in console_report
        
        json_report = server.get_metrics_report(format='json')
        data = json.loads(json_report)
        assert data['metrics']['summary']['total_queries'] >= 3
        
        prometheus_report = server.get_metrics_report(format='prometheus')
        assert "duckql_queries_total" in prometheus_report
    
    @pytest.mark.asyncio
    async def test_slow_query_tracking(self, db_with_data):
        """Test that slow queries are tracked."""
        server = DuckQL(
            db_with_data, 
            enable_metrics=True,
            slow_query_ms=0.1  # Very low threshold in ms
        )
        schema = server.get_schema()
        
        # Run a query that will be "slow"
        await schema.execute("""
            query {
                testTable {
                    id
                    name
                    value
                }
            }
        """)
        
        # Check slow queries in stats
        stats = server.get_stats()
        slow_queries = stats['metrics']['slow_queries']
        assert len(slow_queries) > 0
        assert slow_queries[0]['duration_ms'] > 0.1