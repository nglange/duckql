# DuckQL API Reference

## Core Classes

### DuckQL

The main class for creating a GraphQL API from a DuckDB database.

```python
class DuckQL(
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
    metrics_history_size: int = 10000
)
```

#### Parameters

- **connection**: DuckDB database connection
- **max_workers**: Maximum number of worker threads for query execution (default: 4)
- **max_retries**: Maximum number of retry attempts for failed queries (default: 3)
- **retry_delay**: Initial delay between retries in seconds (default: 0.1)
- **retry_backoff**: Multiplier for exponential backoff (default: 2.0)
- **log_queries**: Whether to log all SQL queries at DEBUG level (default: False)
- **log_slow_queries**: Whether to log slow queries at WARNING level (default: True)
- **slow_query_ms**: Threshold in milliseconds for slow query logging (default: 1000)
- **max_query_depth**: Maximum allowed query depth, None for unlimited (default: None)
- **enable_metrics**: Whether to enable metrics collection (default: True)
- **metrics_history_size**: Maximum number of queries to keep in metrics history (default: 10000)

#### Methods

##### serve()

Start the GraphQL server.

```python
def serve(
    host: str = "0.0.0.0",
    port: int = 8000,
    path: str = "/graphql",
    debug: bool = True
) -> None
```

##### get_schema()

Get the generated GraphQL schema.

```python
def get_schema() -> strawberry.Schema
```

##### computed_field()

Decorator for adding computed fields to a table.

```python
@server.computed_field(table_name: str, field_name: Optional[str] = None)
def field_function(obj: Dict[str, Any]) -> Any:
    # Compute and return the field value
    pass
```

##### resolver()

Decorator for adding custom resolvers to the Query type.

```python
@server.resolver(name: str)
async def resolver_function(root: Any, info: Any, **kwargs) -> Any:
    # Execute custom query and return results
    pass
```

##### get_stats()

Get query execution statistics.

```python
def get_stats() -> Dict[str, Any]
```

Returns:
- `query_count`: Total number of queries executed
- `total_query_time_ms`: Total time spent executing queries
- `average_query_time_ms`: Average query execution time
- `connection_pool_size`: Number of connections in the pool
- `max_retries`: Configured maximum retry attempts
- `slow_query_threshold_ms`: Configured slow query threshold
- `metrics`: Detailed metrics (if enabled) containing:
  - `summary`: Overall statistics (total queries, errors, retries, cache hits)
  - `operations`: Breakdown by operation type (single, list, aggregate)
  - `tables`: Query and error counts by table
  - `durations_ms`: Performance percentiles (min, max, mean, median, p95, p99)
  - `row_counts`: Row count statistics
  - `slow_queries`: List of slowest queries
  - `recent_errors`: Recent query errors

##### reset_stats()

Reset query execution statistics.

```python
def reset_stats() -> None
```

##### get_metrics_report()

Get a formatted metrics report.

```python
def get_metrics_report(format: str = 'console') -> str
```

**Parameters:**
- `format`: Report format - 'console', 'json', or 'prometheus' (default: 'console')

**Returns:** Formatted metrics report as a string

## Exception Classes

### DuckQLError

Base exception for all DuckQL errors.

```python
class DuckQLError(Exception):
    message: str
    error_code: str
    context: Dict[str, Any]
    suggestions: List[str]
    correlation_id: str
```

### SchemaError

Raised when there are issues with database schema or table/column access.

```python
class SchemaError(DuckQLError):
    table_name: Optional[str]
    column_name: Optional[str]
```

### QueryError

Raised when SQL query execution fails.

```python
class QueryError(DuckQLError):
    query: Optional[str]
    table_name: Optional[str]
    operation: Optional[str]
```

### ConnectionError

Raised when database connection fails.

```python
class ConnectionError(DuckQLError):
    database_path: Optional[str]
```

### ValidationError

Raised when GraphQL validation fails.

```python
class ValidationError(DuckQLError):
    field_name: Optional[str]
    expected_type: Optional[str]
    actual_value: Optional[Any]
```

### FilterError

Raised when WHERE clause or filter conditions are invalid.

```python
class FilterError(QueryError):
    filter_field: Optional[str]
    filter_operation: Optional[str]
```

## Query Result Types

### QueryResult

Result of a database query.

```python
@dataclass
class QueryResult:
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
```

## GraphQL Schema Generation

### Supported Types

DuckQL automatically maps DuckDB types to GraphQL types:

| DuckDB Type | GraphQL Type | Notes |
|-------------|--------------|-------|
| BOOLEAN | Boolean | |
| TINYINT, SMALLINT, INTEGER, BIGINT | Int | All integer types |
| UINTEGER, UBIGINT | Int | Unsigned integers |
| REAL, DOUBLE, DECIMAL | Float | All floating point types |
| VARCHAR, CHAR, TEXT | String | All text types |
| DATE | String | ISO 8601 format (YYYY-MM-DD) |
| TIME | String | ISO 8601 format (HH:MM:SS) |
| TIMESTAMP | String | ISO 8601 format |
| UUID | String | |
| JSON, JSONB | JSON | Preserved as objects |
| BLOB, BYTEA | String | Base64 encoded |
| ARRAY | List | Nested arrays supported |
| ENUM | String | |
| INTERVAL | String | |

### Generated Query Fields

For each table, DuckQL generates three query fields:

1. **Single item query**: `tableName(where: TableNameFilter): TableName`
2. **List query**: `tableNames(where: TableNameFilter, orderBy: TableNameOrderBy, limit: Int, offset: Int): [TableName!]!`
3. **Aggregate query**: `tableNamesAggregate(where: TableNameFilter, groupBy: [String!], having: TableNameFilter): [TableNameAggregate!]!`

### Filter Operations

Available filter operations for WHERE clauses:

- `field_eq`: Equal to
- `field_ne`: Not equal to
- `field_gt`: Greater than
- `field_gte`: Greater than or equal to
- `field_lt`: Less than
- `field_lte`: Less than or equal to
- `field_in`: In list
- `field_not_in`: Not in list
- `field_like`: SQL LIKE pattern
- `field_ilike`: Case-insensitive LIKE
- `field_is_null`: Is null (boolean)

Logical operators:
- `_and`: AND condition
- `_or`: OR condition  
- `_not`: NOT condition

### Aggregate Functions

Available aggregate functions:

- `sum`: Sum of values
- `avg`: Average value
- `min`: Minimum value
- `max`: Maximum value
- `count`: Count of non-null values
- `_count`: Count of all rows (including nulls)

## Examples

### Basic Usage

```python
import duckdb
from duckql import DuckQL

# Connect to database
conn = duckdb.connect("analytics.db")

# Create GraphQL API
server = DuckQL(conn)

# Start server
server.serve(port=8000)
```

### With Configuration

```python
server = DuckQL(
    conn,
    max_workers=8,
    max_retries=5,
    retry_delay=0.2,
    log_queries=True,
    slow_query_ms=500,
    max_query_depth=7,
    enable_metrics=True
)
```

### Adding Computed Fields

```python
@server.computed_field("products", "margin_percentage")
def margin_percentage(obj) -> float:
    cost = obj.get('cost', 0)
    price = obj.get('price', 0)
    if price > 0:
        return ((price - cost) / price) * 100
    return 0.0
```

### Adding Custom Resolvers

```python
import strawberry
from typing import List

@strawberry.type
class SalesMetric:
    period: str
    revenue: float
    orders: int

@server.resolver("salesByPeriod")
async def sales_by_period(root, info, period: str = "day") -> List[SalesMetric]:
    sql = f"""
        SELECT 
            DATE_TRUNC('{period}', order_date) as period,
            SUM(total_amount) as revenue,
            COUNT(*) as orders
        FROM orders
        GROUP BY period
        ORDER BY period DESC
        LIMIT 10
    """
    
    result = await server.executor.execute_query(sql)
    
    return [
        SalesMetric(
            period=str(row['period']),
            revenue=float(row['revenue']),
            orders=row['orders']
        )
        for row in result.rows
    ]
```

### Error Handling

```python
from duckql.exceptions import DuckQLError, SchemaError, QueryError

try:
    result = await server.executor.execute_query(sql)
except SchemaError as e:
    print(f"Schema error: {e.message}")
    print(f"Table: {e.table_name}, Column: {e.column_name}")
    print(f"Suggestions: {', '.join(e.suggestions)}")
except QueryError as e:
    print(f"Query error: {e.message}")
    print(f"SQL: {e.context.get('sql')}")
except DuckQLError as e:
    print(f"Error [{e.error_code}]: {e.message}")
    print(f"Correlation ID: {e.correlation_id}")
```

### Performance Monitoring

```python
# Get statistics
stats = server.get_stats()
print(f"Queries executed: {stats['query_count']}")
print(f"Average query time: {stats['average_query_time_ms']}ms")

# Reset statistics
server.reset_stats()

# Execute some queries...

# Check new statistics
new_stats = server.get_stats()
```

### Metrics and Monitoring

```python
# Enable metrics
server = DuckQL(conn, enable_metrics=True)

# Get console-friendly report
print(server.get_metrics_report('console'))

# Get JSON metrics
import json
metrics_json = server.get_metrics_report('json')
metrics_data = json.loads(metrics_json)

# Get Prometheus format
prometheus_metrics = server.get_metrics_report('prometheus')

# Access raw metrics
stats = server.get_stats()
if 'metrics' in stats:
    summary = stats['metrics']['summary']
    print(f"Error rate: {summary['error_rate']:.1%}")
    print(f"Cache hit rate: {summary['cache_hit_rate']:.1%}")
    
    # Check performance percentiles
    durations = stats['metrics']['durations_ms']
    print(f"P95 latency: {durations['p95']:.2f}ms")
```

## CLI Reference

### serve command

```bash
duckql serve [OPTIONS] DATABASE
```

**Options:**
- `--host TEXT`: Host to bind to (default: 0.0.0.0)
- `--port INTEGER`: Port to bind to (default: 8000)
- `--path TEXT`: GraphQL endpoint path (default: /graphql)
- `--debug/--no-debug`: Enable debug mode (default: True)
- `--log-queries/--no-log-queries`: Log all SQL queries (default: False)
- `--log-slow-queries/--no-log-slow-queries`: Log slow queries (default: True)
- `--slow-query-ms INTEGER`: Slow query threshold in milliseconds (default: 1000)
- `--max-depth INTEGER`: Maximum query depth allowed
- `--enable-metrics/--disable-metrics`: Enable metrics collection (default: True)
- `--metrics-port INTEGER`: Port for metrics endpoint (default: 9090)
- `-v, --verbose`: Verbose error output

### metrics command

```bash
duckql metrics [OPTIONS] DATABASE
```

**Options:**
- `--format [console|json|prometheus]`: Output format (default: console)

### schema command

```bash
duckql schema DATABASE
```

Show the generated GraphQL schema.

### tables command

```bash
duckql tables DATABASE
```

List all tables and views in the database.