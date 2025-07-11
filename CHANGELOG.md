# Changelog

All notable changes to DuckQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Query retry logic with exponential backoff for transient errors
  - Configurable retry attempts, delay, and backoff multiplier
  - Automatic retry for connection errors, timeouts, and temporary locks
  - Non-retryable errors (like SQL syntax errors) fail immediately
  
- Enhanced error handling with custom exception hierarchy
  - `SchemaError` for table/column not found errors
  - `QueryError` for SQL execution errors
  - `ConnectionError` for database connection issues
  - `ValidationError` for GraphQL validation errors
  - `FilterError` for invalid WHERE conditions
  - Rich error messages with context, suggestions, and correlation IDs
  
- Query logging and performance monitoring
  - Optional logging of all SQL queries at DEBUG level
  - Slow query detection with configurable threshold
  - Query execution statistics (count, total time, average time)
  - Correlation IDs for request tracking across logs
  
- Computed fields support
  - Add derived fields to any table
  - Access to all row data in computed field functions
  - Automatic GraphQL schema updates
  - Type-safe field definitions
  
- Custom resolver support
  - Add complex analytical queries to the Query type
  - Full async/await support
  - Type-safe resolver definitions with Strawberry types
  - Direct access to query executor

- CLI enhancements
  - `--log-queries` flag to enable query logging
  - `--log-slow-queries` and `--slow-query-ms` for slow query detection
  - `--max-depth` flag for query depth limiting
  - `--enable-metrics/--disable-metrics` flags for metrics control
  - `--metrics-port` flag to specify metrics endpoint port
  - `metrics` command to display metrics in various formats
  - `--verbose` flag for detailed error output
  - Better error formatting with emojis and suggestions

- Query depth limiting
  - Prevent deeply nested queries that could cause performance issues
  - Configurable maximum query depth
  - Introspection queries exempt by default
  - Clear error messages with actual vs allowed depth

- Comprehensive metrics and monitoring
  - Real-time query performance tracking
  - Operation and table-level statistics
  - Query duration percentiles (P50, P95, P99)
  - Error rate tracking and slow query detection
  - Multiple export formats (Console, JSON, Prometheus)
  - Built-in metrics HTTP server
  - Configurable history retention

### Changed
- Minimum Python version raised to 3.9 (from 3.8) for pytest-cov compatibility
- Connection pooling now properly copies schema for in-memory databases
- Error messages are now more user-friendly with actionable suggestions

### Fixed
- Fixed segmentation faults by implementing proper connection pooling
- Fixed computed fields not appearing in GraphQL schema
- Fixed custom resolvers requiring proper type definitions
- Fixed correlation ID duplicate parameter issue in error handling

## [0.1.0] - 2024-01-15

### Added
- Initial release of DuckQL
- Zero-configuration GraphQL API generation from DuckDB databases
- Automatic schema introspection and type mapping
- Rich filtering with multiple operators (eq, ne, gt, gte, lt, lte, in, like)
- Aggregation queries with GROUP BY support
- Order by and pagination (limit/offset)
- Complex filters with AND/OR/NOT operators
- JSON and array type support
- Connection pooling for concurrent query execution
- Built-in GraphQL playground
- CLI interface for easy startup
- Support for views and complex data types