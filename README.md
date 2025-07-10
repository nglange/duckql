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
- 🧩 **Extensible**: Add computed fields and custom resolvers for complex analytics
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

# Create GraphQL API
server = DuckQL(conn)

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
@server.resolver("top_selling_products") 
async def top_selling_products(root, info, limit: int = 10, days: int = 30) -> list:
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
    return result.rows
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
    max_workers=8,  # Thread pool size
    enable_playground=False,  # Disable GraphiQL in production
)

server.serve(
    host="0.0.0.0",
    port=8000,
    debug=False
)
```

## Roadmap

- [ ] Relationship traversal (foreign key navigation)
- [ ] Mutations for INSERT/UPDATE/DELETE
- [ ] Subscriptions for real-time updates
- [ ] Authentication and authorization
- [ ] Query depth limiting
- [ ] DataLoader pattern for N+1 prevention
- [ ] Schema customization options

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