"""
Getting Started with DuckQL

This example shows the basics of using DuckQL to create a GraphQL API
from a DuckDB database.
"""

import duckdb
from duckql import DuckQL


def create_sample_database():
    """Create a simple database with employee data."""
    conn = duckdb.connect(":memory:")
    
    # Create employee table
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            department VARCHAR,
            salary DECIMAL(10, 2),
            hire_date DATE,
            is_active BOOLEAN
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO employees VALUES
        (1, 'Alice Johnson', 'Engineering', 95000, '2021-01-15', true),
        (2, 'Bob Smith', 'Sales', 75000, '2020-06-01', true),
        (3, 'Charlie Brown', 'Engineering', 105000, '2019-03-20', true),
        (4, 'Diana Martinez', 'HR', 65000, '2022-02-10', true),
        (5, 'Eve Wilson', 'Sales', 82000, '2021-08-05', false)
    """)
    
    return conn


def main():
    """Basic DuckQL usage example."""
    print("🦆 Getting Started with DuckQL\n")
    
    # Create database
    conn = create_sample_database()
    
    # Initialize DuckQL
    server = DuckQL(conn)
    
    print("✅ GraphQL API created!")
    print("\nAvailable queries:")
    print("  - employee(where: {...}): Get a single employee")
    print("  - employees(where: {...}, orderBy: {...}, limit: N): List employees")
    print("  - employeesAggregate(groupBy: [...], where: {...}): Aggregate data")
    
    print("\n📊 Example GraphQL queries you can run:\n")
    
    print("1. List all active employees:")
    print("""
query {
  employees(where: { is_active_eq: true }) {
    name
    department
    salary
  }
}
    """)
    
    print("2. Find high earners:")
    print("""
query {
  employees(
    where: { salary_gte: 80000 }
    orderBy: { salary: DESC }
  ) {
    name
    salary
    department
  }
}
    """)
    
    print("3. Get department statistics:")
    print("""
query {
  employeesAggregate(groupBy: ["department"]) {
    department
    salary {
      avg
      min
      max
    }
    _count
  }
}
    """)
    
    print("\n🚀 Starting GraphQL server...")
    print("   Visit http://localhost:8000/graphql to explore your API")
    print("   Press Ctrl+C to stop\n")
    
    # Start the server
    server.serve(port=8000)


if __name__ == "__main__":
    main()