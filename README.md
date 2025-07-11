# DuckQL

🦆 Transform any DuckDB database into a GraphQL API with zero configuration.

DuckQL automatically generates a fully-featured GraphQL API from your DuckDB database schema, enabling web developers to query analytical data without writing SQL or backend code.

## Features

- 🚀 **Zero Configuration**: Point at any DuckDB database and get a GraphQL API instantly
- 🔍 **Rich Filtering**: Complex WHERE clauses with operators (eq, ne, gt, gte, lt, lte, in, like)
- 📊 **Aggregations**: GROUP BY queries with sum, avg, min, max, count functions
- 🔄 **Real-time Schema**: Automatically discovers tables, columns, and relationships
- 🎯 **Type Safety**: Automatic type mapping from DuckDB to GraphQL with proper nullability
- ⚡ **Async Execution**: Non-blocking query execution with connection pooling
- 🔁 **Automatic Retries**: Built-in retry logic for transient errors
- 🧩 **Extensible**: Add computed fields and custom resolvers for complex analytics
- 📝 **Rich Error Messages**: Detailed errors with suggestions and correlation IDs
- 📊 **Query Logging**: Configurable query logging and performance tracking
- 🌐 **Web-Ready**: Built-in GraphQL playground for exploring your API

## Installation

```bash
pip install duckql
```

## Quick Start

### CLI Usage

```bash
# Start GraphQL server for your DuckDB database
duckql serve analytics.db

# With query logging and custom port
duckql serve analytics.db --port 3000 --log-queries

# With slow query detection (logs queries over 500ms)
duckql serve analytics.db --log-slow-queries --slow-query-ms 500

# View available tables
duckql tables analytics.db

# Export GraphQL schema
duckql schema analytics.db > schema.graphql
```

Visit http://localhost:8000/graphql to explore your API using GraphQL playground.

### Python API

```python
from duckql import DuckQL
import duckdb

# Connect to your database
conn = duckdb.connect("analytics.db")

# Create GraphQL API with configuration
server = DuckQL(
    conn,
    max_workers=8,           # Thread pool size for query execution
    max_retries=3,           # Retry attempts for transient errors
    retry_delay=0.1,         # Initial retry delay in seconds
    log_queries=True,        # Log all SQL queries
    log_slow_queries=True,   # Log queries exceeding threshold
    slow_query_ms=1000       # Slow query threshold (1 second)
)

# Start server
server.serve(port=8000)
```

## Example: E-Commerce Analytics

The included example shows DuckQL in action with an e-commerce analytics database:

```python
# Create database with e-commerce schema
conn = create_ecommerce_analytics_db()  # Creates sales, customers, inventory tables

# Initialize DuckQL
server = DuckQL(conn)

# Add computed field
@server.computed_field("inventory", "stock_status")
def stock_status(obj) -> str:
    current = obj.get("current_stock", 0)
    reorder = obj.get("reorder_point", 0)
    if current == 0:
        return "OUT_OF_STOCK"
    elif current < reorder:
        return "LOW_STOCK"
    else:
        return "IN_STOCK"

# Start GraphQL server
server.serve(port=8000)
```

Now query your data:

```graphql
# Sales dashboard
query {
  sales_dashboard(days: 7) {
    total_orders
    total_revenue
    avg_order_value
    unique_customers
  }
}

# Low stock alert
query {
  inventory(where: { current_stock_lt: reorder_point }) {
    product_name
    current_stock
    reorder_point
    stock_status
  }
}

# Sales by region (aggregation)
query {
  salesAggregate(
    groupBy: ["region", "sales_channel"]
    where: { order_date_gte: "2024-01-01" }
  ) {
    region
    sales_channel
    revenue { sum, avg }
    quantity { sum }
    _count
  }
}
```

## Query Examples

### Basic Queries

```graphql
# List products with pagination
query {
  products(limit: 10, offset: 0) {
    product_id
    name
    price
    category
  }
}
```

### Filtering

```graphql
# Find orders with multiple conditions
query {
  orders(
    where: {
      status_eq: "completed"
      total_amount_gte: 1000
      order_date_gte: "2024-01-01"
    }
    orderBy: { order_date: DESC }
  ) {
    order_id
    customer_id
    total_amount
    order_date
  }
}
```

### Complex Filters with AND/OR

```graphql
query {
  products(
    where: {
      _or: [
        { category_eq: "Electronics", price_lt: 500 }
        { category_eq: "Books", price_lt: 50 }
      ]
    }
  ) {
    name
    category
    price
  }
}
```

### Aggregations

```graphql
# Revenue by product category
query {
  salesAggregate(groupBy: ["category"]) {
    category
    revenue {
      sum
      avg
      min
      max
    }
    quantity {
      sum
    }
    _count  # Number of sales
  }
}

# Time-series aggregation
query {
  metricsAggregate(
    groupBy: ["DATE_TRUNC('day', timestamp)"]
    where: { metric_name_eq: "page_views" }
  ) {
    timestamp_bucket
    value {
      sum
      avg
    }
  }
}
```

## Advanced Features

### Computed Fields

Add derived fields to any table:

```python
@server.computed_field("customers", "customer_lifetime_days")
def customer_lifetime_days(obj) -> int:
    first_purchase = obj.get("first_purchase_date")
    last_purchase = obj.get("last_purchase_date")
    if first_purchase and last_purchase:
        return (last_purchase - first_purchase).days
    return 0
```

### Custom Resolvers

Create complex analytical queries:

```python
import strawberry
from typing import List, Optional

# Define the result type
@strawberry.type
class TopProduct:
    name: str
    category: str
    units_sold: int
    total_revenue: float

# Add custom resolver
@server.resolver("topSellingProducts") 
async def top_selling_products(root, info, limit: int = 10, days: int = 30) -> List[TopProduct]:
    sql = f"""
        SELECT 
            p.name,
            p.category,
            SUM(s.quantity) as units_sold,
            SUM(s.revenue) as total_revenue
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        WHERE s.order_date >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY p.name, p.category
        ORDER BY total_revenue DESC
        LIMIT {limit}
    """
    result = await server.executor.execute_query(sql)
    
    return [
        TopProduct(
            name=row['name'],
            category=row['category'],
            units_sold=row['units_sold'],
            total_revenue=row['total_revenue']
        )
        for row in result.rows
    ]
```

Now query it:

```graphql
query {
  topSellingProducts(days: 7, limit: 5) {
    name
    category
    unitsSold
    totalRevenue
  }
}
```

## Type Mappings

DuckQL automatically maps DuckDB types to GraphQL:

| DuckDB Type | GraphQL Type | Notes |
|-------------|--------------|-------|
| INTEGER/BIGINT | Int | All integer types |
| DECIMAL/DOUBLE | Float | All floating point types |
| VARCHAR/TEXT | String | All text types |
| BOOLEAN | Boolean | |
| DATE | String | ISO 8601 format |
| TIMESTAMP | String | ISO 8601 format |
| JSON/JSONB | JSON | Preserved as objects |
| UUID | String | |
| ARRAY | List | Nested arrays supported |
| BLOB | String | Base64 encoded |

## Comparison Operators

Available in WHERE clauses:

- `field_eq`: Equal to
- `field_ne`: Not equal to  
- `field_gt`: Greater than
- `field_gte`: Greater than or equal
- `field_lt`: Less than
- `field_lte`: Less than or equal
- `field_in`: In list
- `field_not_in`: Not in list
- `field_like`: SQL LIKE pattern
- `field_ilike`: Case-insensitive LIKE

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim
RUN pip install duckql
COPY analytics.db /data/
CMD ["duckql", "serve", "/data/analytics.db", "--host", "0.0.0.0"]
```

### Configuration

```python
server = DuckQL(
    connection=conn,
    max_workers=8,             # Thread pool size
    max_retries=3,             # Retry attempts for transient errors
    retry_delay=0.1,           # Initial retry delay
    retry_backoff=2.0,         # Exponential backoff multiplier
    log_queries=False,         # Disable query logging in production
    log_slow_queries=True,     # Log only slow queries
    slow_query_ms=2000,        # 2 second threshold
    enable_metrics=True,       # Enable metrics collection
    metrics_history_size=10000 # Keep last 10k queries in memory
)

server.serve(
    host="0.0.0.0",
    port=8000,
    debug=False  # Disable GraphQL playground in production
)
```

### Retry Configuration

DuckQL includes automatic retry logic for transient errors:

```python
from duckql import DuckQL

server = DuckQL(
    connection=conn,
    max_retries=3,        # Number of retry attempts (default: 3)
    retry_delay=0.1,      # Initial delay between retries in seconds (default: 0.1)
    retry_backoff=2.0,    # Exponential backoff multiplier (default: 2.0)
)
```

The retry logic handles:
- Connection timeouts
- Temporary database locks
- Network issues
- Connection pool exhaustion

Non-retryable errors (like SQL syntax errors) fail immediately without retries.

### Metrics and Monitoring

DuckQL includes built-in metrics collection for monitoring query performance:

```python
from duckql import DuckQL

# Enable metrics collection
server = DuckQL(
    connection=conn,
    enable_metrics=True,      # Enable metrics (default: True)
    metrics_history_size=10000  # Keep last 10k queries (default: 10000)
)

# Get metrics report
print(server.get_metrics_report(format='console'))  # Human-readable
print(server.get_metrics_report(format='json'))     # JSON format
print(server.get_metrics_report(format='prometheus'))  # Prometheus format

# Get raw statistics
stats = server.get_stats()
print(f"Total queries: {stats['metrics']['summary']['total_queries']}")
print(f"Error rate: {stats['metrics']['summary']['error_rate']:.1%}")
print(f"P95 latency: {stats['metrics']['durations_ms']['p95']:.2f}ms")

# Reset statistics
server.reset_stats()
```

#### Metrics Server

Expose metrics via HTTP endpoints:

```bash
# Start DuckQL with metrics endpoint
duckql serve database.db --enable-metrics --metrics-port 9090

# Access metrics
curl http://localhost:9090/metrics          # Prometheus format
curl http://localhost:9090/metrics/json     # JSON format
```

#### Available Metrics

- **Query counts**: Total, by operation type, by table
- **Error tracking**: Total errors, error rate, errors by table
- **Performance**: Min/max/mean/median/P95/P99 query duration
- **Throughput**: Queries per second, rows returned
- **Slow queries**: Queries exceeding threshold with details
- **Cache statistics**: Hit rate for connection pooling

## Error Handling

DuckQL provides rich error messages with helpful context:

```python
try:
    result = await server.executor.execute_query(sql)
except DuckQLError as e:
    print(f"Error: {e.message}")
    print(f"Error Code: {e.error_code}")
    print(f"Context: {e.context}")
    print(f"Suggestions: {e.suggestions}")
    print(f"Correlation ID: {e.correlation_id}")
```

Error types include:
- `SchemaError`: Table or column not found
- `QueryError`: SQL execution errors
- `ConnectionError`: Database connection issues
- `ValidationError`: GraphQL validation errors
- `FilterError`: Invalid filter conditions

## Performance Monitoring

Track query performance and statistics:

```python
# Get query statistics
stats = server.get_stats()
print(f"Total queries: {stats['query_count']}")
print(f"Average query time: {stats['average_query_time_ms']}ms")
print(f"Total query time: {stats['total_query_time_ms']}ms")

# Reset statistics
server.reset_stats()
```

## Roadmap

- [x] Query retry logic with exponential backoff
- [x] Enhanced error messages with suggestions
- [x] Query logging and performance tracking
- [x] Computed fields
- [x] Custom resolvers
- [ ] Query depth limiting
- [ ] Relationship traversal (foreign key navigation)
- [ ] Mutations for INSERT/UPDATE/DELETE
- [ ] Subscriptions for real-time updates
- [ ] Authentication and authorization
- [ ] DataLoader pattern for N+1 prevention
- [ ] Schema customization options
- [ ] Query result caching

## Development

```bash
# Clone repository
git clone https://github.com/yourusername/duckql.git
cd duckql

# Install with Poetry
poetry install

# Run tests
poetry run pytest

# Run example
poetry run python example.py
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.