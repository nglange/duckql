"""CLI interface for DuckQL."""

import click
import duckdb
from pathlib import Path

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
def serve(database: str, host: str, port: int, path: str, debug: bool):
    """Start GraphQL server for a DuckDB database."""
    # Connect to database
    click.echo(f"🦆 Connecting to database: {database}")
    
    try:
        if database == ':memory:':
            conn = duckdb.connect(':memory:')
        else:
            conn = duckdb.connect(database)
        
        # Create DuckQL instance
        server = DuckQL(conn)
        
        # Get table count for info
        tables = server.introspector.get_tables()
        click.echo(f"📊 Found {len(tables)} tables: {', '.join(tables)}")
        
        # Start server
        server.serve(host=host, port=port, path=path, debug=debug)
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
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