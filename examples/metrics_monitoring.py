"""
Metrics and Monitoring Example

This example demonstrates how to use DuckQL's built-in metrics collection
and monitoring capabilities.
"""

import asyncio
import duckdb
import time
from duckql import DuckQL
from duckql.metrics import MetricsServer


def create_sample_database():
    """Create a sample database with some test data."""
    conn = duckdb.connect(":memory:")
    
    # Create tables with different sizes for performance testing
    conn.execute("""
        CREATE TABLE small_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value DECIMAL(10, 2)
        )
    """)
    
    conn.execute("""
        CREATE TABLE medium_table (
            id INTEGER PRIMARY KEY,
            category VARCHAR,
            amount DECIMAL(12, 2),
            created_at TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE large_table (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price DECIMAL(10, 2),
            order_date DATE
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO small_table 
        SELECT i, 'Item ' || i, RANDOM() * 1000
        FROM generate_series(1, 100) AS s(i)
    """)
    
    conn.execute("""
        INSERT INTO medium_table 
        SELECT 
            i, 
            CASE (i % 5)
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Clothing'
                WHEN 2 THEN 'Food'
                WHEN 3 THEN 'Books'
                ELSE 'Other'
            END,
            RANDOM() * 10000,
            CURRENT_TIMESTAMP - INTERVAL (RANDOM() * 365) DAY
        FROM generate_series(1, 1000) AS s(i)
    """)
    
    conn.execute("""
        INSERT INTO large_table 
        SELECT 
            i,
            (RANDOM() * 100)::INTEGER + 1,
            (RANDOM() * 50)::INTEGER + 1,
            (RANDOM() * 10)::INTEGER + 1,
            RANDOM() * 500,
            CURRENT_DATE - INTERVAL (RANDOM() * 365) DAY
        FROM generate_series(1, 10000) AS s(i)
    """)
    
    return conn


async def run_various_queries(schema):
    """Run various types of queries to generate metrics."""
    
    # 1. Simple queries
    print("Running simple queries...")
    for i in range(5):
        await schema.execute("""
            query {
                smallTable(limit: 10) {
                    id
                    name
                    value
                }
            }
        """)
    
    # 2. Filtered queries
    print("Running filtered queries...")
    for i in range(3):
        await schema.execute("""
            query {
                mediumTable(where: {category: {eq: "Electronics"}}) {
                    id
                    category
                    amount
                }
            }
        """)
    
    # 3. Aggregation queries
    print("Running aggregation queries...")
    await schema.execute("""
        query {
            largeTableAggregate(groupBy: ["user_id"]) {
                user_id
                price {
                    sum
                    avg
                    max
                }
                _count
            }
        }
    """)
    
    # 4. Slow query (large result set)
    print("Running slow query...")
    await schema.execute("""
        query {
            largeTable(limit: 5000) {
                id
                user_id
                product_id
                quantity
                price
            }
        }
    """)
    
    # 5. Error query (invalid field)
    print("Running error query...")
    try:
        await schema.execute("""
            query {
                smallTable {
                    id
                    invalid_field
                }
            }
        """)
    except:
        pass  # Expected to fail
    
    # 6. Complex nested aggregations
    print("Running complex aggregation...")
    await schema.execute("""
        query {
            mediumTableAggregate(
                where: {amount: {gt: 5000}}
                groupBy: ["category"]
            ) {
                category
                amount {
                    sum
                    avg
                    min
                    max
                }
                _count
            }
        }
    """)


async def main():
    """Demonstrate metrics collection and reporting."""
    print("🦆 DuckQL Metrics & Monitoring Demo\n")
    
    # Create database and server
    conn = create_sample_database()
    server = DuckQL(
        conn,
        enable_metrics=True,
        log_queries=True,
        log_slow_queries=True,
        slow_query_ms=100  # Low threshold for demo
    )
    
    schema = server.get_schema()
    
    # Start metrics server in background
    print("📊 Starting metrics server on http://localhost:9090/metrics\n")
    metrics_server = MetricsServer(server.metrics_collector, port=9090)
    import threading
    metrics_thread = threading.Thread(target=metrics_server.run, daemon=True)
    metrics_thread.start()
    
    # Give metrics server time to start
    time.sleep(1)
    
    # Run various queries
    print("🚀 Running test queries...\n")
    await run_various_queries(schema)
    
    # Wait a bit for all metrics to be collected
    time.sleep(0.5)
    
    # Display metrics in different formats
    print("\n" + "="*60 + "\n")
    
    # 1. Console report
    print("📈 Console Metrics Report:")
    print(server.get_metrics_report(format='console'))
    
    print("\n" + "="*60 + "\n")
    
    # 2. Show specific query history
    print("📜 Recent Query History:")
    history = server.metrics_collector.get_query_history(limit=5)
    for query in history:
        print(f"  - {query['operation']} on {query['table']}: "
              f"{query['duration_ms']:.2f}ms, {query['row_count']} rows")
    
    print("\n" + "="*60 + "\n")
    
    # 3. JSON format (partial)
    print("📄 JSON Metrics (partial):")
    import json
    json_report = json.loads(server.get_metrics_report(format='json'))
    summary = json_report['metrics']['summary']
    print(json.dumps(summary, indent=2))
    
    print("\n" + "="*60 + "\n")
    
    # 4. Prometheus format (sample)
    print("📊 Prometheus Metrics (sample):")
    prometheus_report = server.get_metrics_report(format='prometheus')
    # Show first few lines
    lines = prometheus_report.split('\n')[:10]
    for line in lines:
        print(line)
    print("...")
    
    print("\n✅ Demo complete!")
    print("\n💡 Tips:")
    print("   - Visit http://localhost:9090/metrics for Prometheus metrics")
    print("   - Visit http://localhost:9090/metrics/json for JSON metrics")
    print("   - Use server.reset_stats() to clear metrics")
    print("   - Configure slow_query_ms to adjust slow query threshold")
    print("   - Enable log_queries for detailed SQL logging")


if __name__ == "__main__":
    asyncio.run(main())