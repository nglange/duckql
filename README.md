# DuckQL

GraphQL interface for DuckDB databases. Automatically generates a GraphQL API from any DuckDB database schema.

## Installation

```bash
pip install duckql
```

## Quick Start

### CLI Usage

```bash
# Serve GraphQL API for a DuckDB database
duckql serve my_database.db

# View tables in database
duckql tables my_database.db

# Show generated GraphQL schema
duckql schema my_database.db
```

### Python API

```python
from duckql import DuckQL
import duckdb

# Connect to your database
db = duckdb.connect("analytics.db")

# Create DuckQL server
server = DuckQL(db)

# Start GraphQL server
server.serve()
```

## Features

- **Automatic Schema Generation**: Discovers tables and generates GraphQL types
- **Rich Filtering**: WHERE, ORDER BY, LIMIT, OFFSET with comparison operators
- **Type Safety**: Proper type mapping from DuckDB to GraphQL
- **Async Execution**: Non-blocking queries using thread pool
- **Computed Fields**: Add derived fields to any type
- **Custom Resolvers**: Extend with complex analytical queries

## GraphQL Query Examples

### Basic Query

```graphql
query {
  events(
    where: { timestamp: { gte: "2024-01-01" } }
    orderBy: { timestamp: DESC }
    limit: 100
  ) {
    id
    timestamp
    event_type
    properties  # JSON field
  }
}
```

### Complex Filtering

```graphql
query {
  conversations(
    where: {
      _and: [
        { status: { eq: "completed" } }
        { final_convergence_score: { gte: 0.8 } }
        { experiment_id: { eq: "exp_123" } }
      ]
    }
  ) {
    conversation_id
    total_turns
    final_convergence_score
  }
}
```

## Advanced Usage

### Computed Fields

```python
# Add computed fields to types
@server.computed_field("events", "day_of_week")
def day_of_week(obj) -> str:
    return obj["timestamp"].strftime("%A")
```

### Custom Resolvers

```python
# Add custom analytical queries
@server.resolver("convergence_summary")
async def convergence_summary(root, info, experiment_id: str) -> dict:
    sql = """
        SELECT 
            AVG(final_convergence_score) as avg_score,
            COUNT(*) as total_conversations
        FROM conversations 
        WHERE experiment_id = $1
    """
    result = await server.executor.execute_query(sql, {"p0": experiment_id})
    return result.rows[0]
```

## Type Mappings

| DuckDB Type | GraphQL Type |
|-------------|--------------|
| INTEGER | Int |
| DOUBLE | Float |
| VARCHAR | String |
| BOOLEAN | Boolean |
| TIMESTAMP | DateTime |
| JSON | JSON |
| DATE | Date |
| UUID | String |
| Arrays | List[T] |

## License

MIT