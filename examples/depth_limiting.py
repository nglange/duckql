"""
Query Depth Limiting Example

This example demonstrates how to use query depth limiting to prevent
deeply nested queries that could cause performance issues.
"""

import asyncio
import duckdb
from duckql import DuckQL


def create_hierarchical_database():
    """Create a database with hierarchical data for testing depth limits."""
    conn = duckdb.connect(":memory:")
    
    # Create a hierarchical structure: Organizations -> Departments -> Teams -> Employees
    conn.execute("""
        CREATE TABLE organizations (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            founded_year INTEGER
        )
    """)
    
    conn.execute("""
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            org_id INTEGER,
            name VARCHAR,
            budget DECIMAL(12, 2)
        )
    """)
    
    conn.execute("""
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            dept_id INTEGER,
            name VARCHAR,
            lead_name VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            team_id INTEGER,
            name VARCHAR,
            title VARCHAR,
            salary DECIMAL(10, 2)
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO organizations VALUES
        (1, 'TechCorp', 2010),
        (2, 'DataInc', 2015)
    """)
    
    conn.execute("""
        INSERT INTO departments VALUES
        (1, 1, 'Engineering', 5000000),
        (2, 1, 'Marketing', 2000000),
        (3, 2, 'Research', 3000000),
        (4, 2, 'Sales', 2500000)
    """)
    
    conn.execute("""
        INSERT INTO teams VALUES
        (1, 1, 'Backend', 'Alice'),
        (2, 1, 'Frontend', 'Bob'),
        (3, 2, 'Content', 'Charlie'),
        (4, 3, 'ML Team', 'Diana')
    """)
    
    conn.execute("""
        INSERT INTO employees VALUES
        (1, 1, 'John Doe', 'Senior Engineer', 120000),
        (2, 1, 'Jane Smith', 'Engineer', 100000),
        (3, 2, 'Mike Johnson', 'UI Designer', 95000),
        (4, 3, 'Sarah Williams', 'Content Writer', 75000),
        (5, 4, 'Tom Brown', 'Data Scientist', 130000)
    """)
    
    return conn


async def main():
    """Demonstrate query depth limiting."""
    print("🦆 DuckQL Query Depth Limiting Demo\n")
    
    # Create database
    conn = create_hierarchical_database()
    
    # Example 1: No depth limit
    print("1️⃣ Creating server WITHOUT depth limit...")
    server_no_limit = DuckQL(conn)
    schema_no_limit = server_no_limit.get_schema()
    
    # This deeply nested query would work
    deep_query = """
    query {
        organizations {
            name
            departments {
                name
                teams {
                    name
                    employees {
                        name
                        title
                        salary
                    }
                }
            }
        }
    }
    """
    
    print("   Executing deeply nested query (4 levels deep)...")
    result = await schema_no_limit.execute(deep_query)
    if result.errors:
        print(f"   ❌ Error: {result.errors[0].message}")
    else:
        print(f"   ✅ Success! Retrieved data from {len(result.data['organizations'])} organizations")
    
    # Example 2: With depth limit
    print("\n2️⃣ Creating server WITH depth limit of 3...")
    server_with_limit = DuckQL(conn, max_query_depth=3)
    schema_with_limit = server_with_limit.get_schema()
    
    # Shallow query - should work
    shallow_query = """
    query {
        organizations {
            name
            departments {
                name
                budget
            }
        }
    }
    """
    
    print("   Executing shallow query (2 levels deep)...")
    result = await schema_with_limit.execute(shallow_query)
    if result.errors:
        print(f"   ❌ Error: {result.errors[0].message}")
    else:
        print(f"   ✅ Success! Query within depth limit")
        for org in result.data['organizations']:
            print(f"      - {org['name']}: {len(org['departments'])} departments")
    
    # Deep query - should fail
    print("\n   Executing deep query (4 levels deep)...")
    result = await schema_with_limit.execute(deep_query)
    if result.errors:
        print(f"   ❌ Expected error: {result.errors[0].message}")
    else:
        print(f"   ✅ Unexpected success")
    
    # Example 3: Introspection queries are allowed
    print("\n3️⃣ Testing introspection query (always allowed)...")
    server_strict = DuckQL(conn, max_query_depth=1)  # Very strict limit
    schema_strict = server_strict.get_schema()
    
    introspection_query = """
    query {
        __schema {
            types {
                name
                fields {
                    name
                    type {
                        name
                        kind
                    }
                }
            }
        }
    }
    """
    
    result = await schema_strict.execute(introspection_query)
    if result.errors:
        print(f"   ❌ Error: {result.errors[0].message}")
    else:
        print(f"   ✅ Introspection allowed despite strict depth limit")
        print(f"      Found {len(result.data['__schema']['types'])} types")
    
    # Example 4: Aggregation queries
    print("\n4️⃣ Testing aggregation queries with depth limit...")
    agg_query = """
    query {
        departmentsAggregate(groupBy: ["org_id"]) {
            org_id
            budget {
                sum
                avg
            }
            _count
        }
    }
    """
    
    result = await schema_with_limit.execute(agg_query)
    if result.errors:
        print(f"   ❌ Error: {result.errors[0].message}")
    else:
        print(f"   ✅ Aggregation queries work within depth limit")
        for agg in result.data['departmentsAggregate']:
            print(f"      - Org {agg['org_id']}: {agg['_count']} departments, "
                  f"total budget ${agg['budget']['sum']:,.2f}")
    
    print("\n✅ Demo complete!")
    print("\n💡 Best Practices:")
    print("   - Set a reasonable depth limit (3-7 is common)")
    print("   - Consider your schema complexity when choosing limits")
    print("   - Introspection queries are exempt by default")
    print("   - Use depth limiting to prevent malicious queries")
    print("   - Monitor rejected queries to adjust limits if needed")


if __name__ == "__main__":
    asyncio.run(main())