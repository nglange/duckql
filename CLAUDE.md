# CLAUDE.md - AI Assistant Guide for DuckQL

## Project Overview

DuckQL is a Python library that provides a GraphQL interface for DuckDB databases. It automatically generates a GraphQL API from any DuckDB database schema, making analytical data accessible without writing SQL.

**Key Value**: Researchers and data analysts can query DuckDB using GraphQL instead of SQL, with automatic schema discovery and optimized query translation. Perfect for Jupyter notebooks.

## Current Status (as of last update)

### ✅ Completed
- Core functionality: schema introspection, type generation, query translation
- Async query execution with thread pool
- CLI interface (`duckql serve database.db`)
- Comprehensive test suite (39/48 unit tests passing)
- Integration tests for full query flow
- Notebook-friendly utilities (`NotebookDuckQL`)
- Example Jupyter notebook
- Support for complex schemas (Pidgin project with 80+ column tables)
- JSON and array field support
- Computed fields and custom resolvers

### 🚧 In Progress / Known Issues
- Some unit tests fail due to minor assertion mismatches (not actual bugs)
- Filter types with self-referential fields need better handling
- Column names that are Python keywords or start with numbers need sanitization

### 📋 TODO (Priority Order)
1. **Add aggregation support** - GROUP BY, COUNT, SUM, AVG, etc.
2. **Improve error messages** - User-friendly errors with context
3. **Add query depth limiting** - Prevent deeply nested queries
4. **Performance optimizations** - Query result caching, connection pooling
5. **Better type handling** - STRUCT, MAP, nested arrays
6. **Authentication/authorization** - Optional security layer
7. **Metrics and monitoring** - Query performance tracking

## Architecture

```
duckql/
├── core.py              # Main DuckQL class, GraphQL schema building
├── schema/              
│   ├── introspection.py # Database schema discovery
│   └── types.py         # GraphQL type generation
├── execution/           
│   ├── translator.py    # GraphQL → SQL translation
│   └── executor.py      # Async query execution
├── notebook.py          # Jupyter notebook utilities
└── cli.py              # CLI interface
```

### Key Design Decisions

1. **Strawberry over Ariadne**: Better for dynamic type generation
2. **SQLGlot for SQL**: Safe, dialect-aware SQL generation (no string concat!)
3. **Code-first approach**: Types generated from introspection
4. **Async by default**: All queries run in ThreadPoolExecutor
5. **Read-only focus**: Built for analytics, not CRUD

## Development Guidelines

### Testing
```bash
# Run all tests
poetry run pytest -v

# Run specific test categories
poetry run pytest tests/test_integration.py -v  # Integration tests
poetry run pytest tests/test_introspection.py -v  # Schema discovery
poetry run pytest tests/test_translator.py -v  # SQL translation
```

### Common Tasks

#### Adding a new DuckDB type mapping
Edit `duckql/schema/types.py`:
```python
DUCKDB_TYPE_MAP = {
    'NEW_TYPE': python_type,  # Add here
    ...
}
```

#### Adding a new filter operator
Edit `duckql/schema/types.py` in `_build_filter_type()`:
```python
if base_type in (int, float, Decimal):
    annotations[f"{field_name}_new_op"] = Optional[base_type]  # Add here
```

Then update `duckql/execution/translator.py` in `_build_field_condition()`.

#### Debugging GraphQL schema issues
```python
# Check generated schema
duckql = DuckQL(conn)
schema = duckql.get_schema()
print(schema)  # Shows GraphQL SDL

# Check specific table mapping
introspector = DuckDBIntrospector(conn)
table_info = introspector.get_table_info('table_name')
print(table_info.columns)
```

## Important Context

### Pidgin Project
DuckQL was built to support the Pidgin project, which has complex analytical schemas:
- Tables with 80+ columns (turn_metrics)
- Extensive JSON fields for flexible data
- UUID primary keys
- Composite primary keys
- Large datasets for performance testing

### Python 3.13 Compatibility
- DuckDB 1.0+ required (0.9.x doesn't compile on Python 3.13)
- Strawberry GraphQL 0.255+ required (earlier versions have dataclass issues)
- Some dependencies may need updates for Python 3.13

### Reserved Words and Special Characters
- Column names that are Python keywords get `_` suffix
- Columns starting with numbers get `field_` prefix  
- Special characters in column names are replaced with `_`
- SQL injection attempts in data are safely parameterized

### Notebook Usage Pattern
Most users will use DuckQL from Jupyter notebooks:
```python
from duckql import connect

# Simple connection
db = connect("analytics.db")

# Query to DataFrame
df = db.query_df("""
    query {
        sales(where: { amount: { gt: 100 } }) {
            date
            product
            amount
        }
    }
""")
```

## Debugging Tips

### Common Errors

1. **"Could not resolve the type of '_and'"**
   - Issue with self-referential filter types
   - Check `_build_filter_type()` in types.py

2. **"Parser Error: unterminated quoted string"**
   - SQL escaping issue in test data
   - Use proper quote doubling in SQL strings

3. **"cannot import name '_create_fn' from 'dataclasses'"**
   - Strawberry version incompatibility
   - Update to 0.255+ for Python 3.13

4. **Column name errors**
   - Reserved words or invalid identifiers
   - Check `_to_field_name()` sanitization

### Performance Issues
- Large result sets: Add pagination (limit/offset)
- Slow queries: Check generated SQL with `translator.translate_query()`
- Memory usage: Results are fully loaded, consider streaming

## Future Enhancements

### High Priority
1. **Aggregation Queries**: 
   ```graphql
   query {
     salesAggregate(groupBy: ["product"]) {
       product
       amount { sum, avg, max }
       _count
     }
   }
   ```

2. **Subscription Support**: Real-time updates using DuckDB's LISTEN/NOTIFY

3. **Schema Caching**: Avoid introspection on every startup

### Nice to Have
- GraphQL schema SDL export
- Automatic relationship detection
- Query cost analysis
- Result streaming for large datasets
- DuckDB extension integration (spatial, full-text search)

## References

- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [Strawberry GraphQL Docs](https://strawberry.rocks/)
- [SQLGlot Documentation](https://sqlglot.com/)
- [Pidgin Project](https://github.com/erichare/pidgin) - The motivating use case

## Quick Wins

If you're looking to make immediate improvements:

1. **Fix the failing tests** - Most are just assertion updates needed
2. **Add `__str__` methods** - Better error messages and debugging
3. **Add logging** - Use Python's logging module for debug info
4. **Write more examples** - Especially for computed fields and custom resolvers
5. **Optimize imports** - Make pandas truly optional throughout

Remember: This is an analytical tool. Every decision should support fast, safe queries on analytical data. Don't try to make it a general-purpose GraphQL server!