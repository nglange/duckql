"""CLI interface for DuckQL."""

import click
import duckdb
from pathlib import Path
from typing import Optional

from .core import DuckQL


@click.group()
def cli():
    """DuckQL - GraphQL interface for DuckDB databases."""
    pass


@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--host', default='0.0.0.0', help='Host to bind to')
@click.option('--port', default=8000, type=int, help='Port to bind to')
@click.option('--path', default='/graphql', help='GraphQL endpoint path')
@click.option('--debug/--no-debug', default=True, help='Enable debug mode')
@click.option('--log-queries/--no-log-queries', default=False, help='Log all SQL queries')
@click.option('--log-slow-queries/--no-log-slow-queries', default=True, help='Log slow queries')
@click.option('--slow-query-ms', default=1000, type=int, help='Slow query threshold in milliseconds')
@click.option('--max-depth', default=None, type=int, help='Maximum query depth allowed')
@click.option('--enable-metrics/--disable-metrics', default=True, help='Enable metrics collection')
@click.option('--metrics-port', default=9090, type=int, help='Port for metrics endpoint')
@click.option('--verbose', '-v', is_flag=True, help='Verbose error output')
def serve(database: str, host: str, port: int, path: str, debug: bool, 
          log_queries: bool, log_slow_queries: bool, slow_query_ms: int, 
          max_depth: Optional[int], enable_metrics: bool, metrics_port: int, verbose: bool):
    """Start GraphQL server for a DuckDB database."""
    # Set up logging
    import logging
    if debug or log_queries:
        logging.basicConfig(
            level=logging.DEBUG if log_queries else logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Import here to respect logging configuration
    from .exceptions import DuckQLError, ConnectionError as DuckQLConnectionError
    
    # Connect to database
    click.echo(f"🦆 Connecting to database: {database}")
    
    try:
        if database == ':memory:':
            conn = duckdb.connect(':memory:')
        else:
            conn = duckdb.connect(database)
        
        # Create DuckQL instance with logging options
        server = DuckQL(
            conn,
            log_queries=log_queries,
            log_slow_queries=log_slow_queries,
            slow_query_ms=slow_query_ms,
            max_query_depth=max_depth,
            enable_metrics=enable_metrics
        )
        
        # Get table count for info
        tables = server.introspector.get_tables()
        click.echo(f"📊 Found {len(tables)} tables: {', '.join(tables)}")
        
        if log_queries:
            click.echo(f"🔍 Query logging enabled (slow query threshold: {slow_query_ms}ms)")
        
        if max_depth:
            click.echo(f"🛡️  Query depth limit: {max_depth}")
        
        if enable_metrics:
            click.echo(f"📊 Metrics endpoint: http://{host}:{metrics_port}/metrics")
            # Start metrics server in background thread
            from .metrics import MetricsServer
            import threading
            if server.metrics_collector:
                metrics_server = MetricsServer(server.metrics_collector, port=metrics_port)
                metrics_thread = threading.Thread(target=metrics_server.run, daemon=True)
                metrics_thread.start()
        
        # Start server
        click.echo(f"🚀 Starting GraphQL server at http://{host}:{port}{path}")
        if debug:
            click.echo(f"🎮 GraphQL Playground available at http://{host}:{port}{path}")
        
        server.serve(host=host, port=port, path=path, debug=debug)
        
    except DuckQLConnectionError as e:
        click.echo(f"\n❌ Connection Error: {e.message}", err=True)
        if e.suggestions:
            click.echo("\n💡 Suggestions:", err=True)
            for suggestion in e.suggestions:
                click.echo(f"   • {suggestion}", err=True)
        if verbose:
            click.echo(f"\n🔍 Correlation ID: {e.correlation_id}", err=True)
        raise click.Abort()
        
    except DuckQLError as e:
        click.echo(f"\n❌ {e.error_code}: {e.message}", err=True)
        if e.context:
            click.echo(f"📍 Context: {e.context}", err=True)
        if e.suggestions:
            click.echo("\n💡 Suggestions:", err=True)
            for suggestion in e.suggestions:
                click.echo(f"   • {suggestion}", err=True)
        if verbose:
            click.echo(f"\n🔍 Correlation ID: {e.correlation_id}", err=True)
        raise click.Abort()
        
    except Exception as e:
        click.echo(f"\n❌ Unexpected Error: {e}", err=True)
        if verbose:
            import traceback
            click.echo("\n🔍 Stack trace:", err=True)
            click.echo(traceback.format_exc(), err=True)
        raise click.Abort()


@cli.command()
@click.argument('database', type=click.Path(exists=True))
def schema(database: str):
    """Show GraphQL schema for a DuckDB database."""
    try:
        # Connect to database
        conn = duckdb.connect(database)
        
        # Create DuckQL instance
        server = DuckQL(conn)
        
        # Print schema
        schema_str = str(server.get_schema())
        click.echo(schema_str)
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('database', type=click.Path(exists=True))
@click.option('--format', type=click.Choice(['console', 'json', 'prometheus']), default='console', help='Metrics output format')
def metrics(database: str, format: str):
    """Show query metrics for a running DuckQL instance."""
    try:
        # Connect to database
        conn = duckdb.connect(database)
        
        # Create DuckQL instance
        server = DuckQL(conn, enable_metrics=True)
        
        # Get and display metrics
        report = server.get_metrics_report(format=format)
        click.echo(report)
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument('database', type=click.Path(exists=True))
def tables(database: str):
    """List all tables in a DuckDB database."""
    try:
        # Connect to database
        conn = duckdb.connect(database)
        
        # Create introspector
        from .schema import DuckDBIntrospector
        introspector = DuckDBIntrospector(conn)
        
        # Get tables and views
        tables = introspector.get_tables()
        views = introspector.get_views()
        
        click.echo("📊 Tables:")
        for table in tables:
            table_info = introspector.get_table_info(table)
            click.echo(f"  - {table} ({len(table_info.columns)} columns)")
        
        if views:
            click.echo("\n📈 Views:")
            for view in views:
                click.echo(f"  - {view}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        raise click.Abort()


if __name__ == '__main__':
    cli()