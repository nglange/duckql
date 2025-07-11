"""
Advanced DuckQL Features Example

This example demonstrates:
- Computed fields
- Custom resolvers
- Error handling
- Query logging
- Performance monitoring
"""

import asyncio
import duckdb
import strawberry
from typing import List, Optional
from datetime import datetime, timedelta

from duckql import DuckQL
from duckql.exceptions import DuckQLError


def create_analytics_database():
    """Create a sample analytics database."""
    conn = duckdb.connect(":memory:")
    
    # Create tables
    conn.execute("""
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            username VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP,
            last_login TIMESTAMP,
            total_spent DECIMAL(10, 2),
            order_count INTEGER
        )
    """)
    
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            price DECIMAL(10, 2),
            stock_quantity INTEGER,
            reorder_point INTEGER,
            created_at TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            order_date TIMESTAMP,
            total_amount DECIMAL(10, 2),
            status VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE order_items (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            line_total DECIMAL(10, 2)
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO users VALUES
        (1, 'alice', 'alice@example.com', '2023-01-01', '2024-01-15', 2500.00, 15),
        (2, 'bob', 'bob@example.com', '2023-02-01', '2024-01-14', 1800.00, 8),
        (3, 'charlie', 'charlie@example.com', '2023-03-01', '2024-01-13', 3200.00, 22),
        (4, 'diana', 'diana@example.com', '2023-04-01', '2024-01-12', 950.00, 5),
        (5, 'eve', 'eve@example.com', '2023-05-01', '2024-01-11', 4100.00, 28)
    """)
    
    conn.execute("""
        INSERT INTO products VALUES
        (1, 'Laptop', 'Electronics', 999.99, 15, 10, '2023-01-01'),
        (2, 'Mouse', 'Electronics', 29.99, 150, 50, '2023-01-01'),
        (3, 'Keyboard', 'Electronics', 79.99, 8, 20, '2023-01-01'),
        (4, 'Monitor', 'Electronics', 299.99, 25, 15, '2023-01-01'),
        (5, 'Desk Chair', 'Furniture', 199.99, 30, 10, '2023-01-01'),
        (6, 'Standing Desk', 'Furniture', 499.99, 5, 5, '2023-01-01'),
        (7, 'Notebook', 'Office Supplies', 4.99, 500, 100, '2023-01-01'),
        (8, 'Pen Pack', 'Office Supplies', 9.99, 200, 50, '2023-01-01')
    """)
    
    # Create some orders
    base_date = datetime.now() - timedelta(days=30)
    for i in range(50):
        order_date = base_date + timedelta(days=i % 30)
        user_id = (i % 5) + 1
        order_id = i + 1
        status = 'completed' if i < 45 else 'pending'
        
        conn.execute(f"""
            INSERT INTO orders VALUES
            ({order_id}, {user_id}, TIMESTAMP '{order_date.strftime('%Y-%m-%d %H:%M:%S')}', 0, '{status}')
        """)
        
        # Add 1-3 items per order
        total = 0
        for j in range((i % 3) + 1):
            product_id = ((i + j) % 8) + 1
            quantity = (i % 3) + 1
            
            # Get product price
            price = conn.execute(f"SELECT price FROM products WHERE product_id = {product_id}").fetchone()[0]
            line_total = price * quantity
            total += line_total
            
            conn.execute(f"""
                INSERT INTO order_items VALUES
                ({order_id}, {product_id}, {quantity}, {price}, {line_total})
            """)
        
        # Update order total
        conn.execute(f"UPDATE orders SET total_amount = {total} WHERE order_id = {order_id}")
    
    return conn


async def main():
    """Demonstrate advanced DuckQL features."""
    print("🦆 DuckQL Advanced Features Demo\n")
    
    # Create database
    conn = create_analytics_database()
    
    # Initialize DuckQL with all features enabled
    server = DuckQL(
        conn,
        max_workers=4,
        max_retries=3,
        retry_delay=0.1,
        log_queries=True,        # Enable query logging
        log_slow_queries=True,   # Log slow queries
        slow_query_ms=100        # 100ms threshold for demo
    )
    
    # 1. Add Computed Fields
    print("1️⃣ Adding computed fields...")
    
    @server.computed_field("users", "account_age_days")
    def account_age_days(obj) -> int:
        """Calculate how long the user has been registered."""
        created = obj.get('created_at')
        if created:
            if isinstance(created, str):
                created = datetime.fromisoformat(created)
            return (datetime.now() - created).days
        return 0
    
    @server.computed_field("users", "average_order_value")
    def average_order_value(obj) -> Optional[float]:
        """Calculate average order value."""
        total_spent = obj.get('total_spent', 0)
        order_count = obj.get('order_count', 0)
        if order_count > 0:
            return round(total_spent / order_count, 2)
        return None
    
    @server.computed_field("products", "stock_status")
    def stock_status(obj) -> str:
        """Determine stock status based on quantity and reorder point."""
        stock = obj.get('stock_quantity', 0)
        reorder = obj.get('reorder_point', 0)
        
        if stock == 0:
            return "OUT_OF_STOCK"
        elif stock < reorder:
            return "LOW_STOCK"
        elif stock < reorder * 2:
            return "NORMAL"
        else:
            return "WELL_STOCKED"
    
    # 2. Add Custom Resolvers
    print("2️⃣ Adding custom resolvers...")
    
    @strawberry.type
    class DashboardStats:
        total_revenue: float
        total_orders: int
        unique_customers: int
        average_order_value: float
        pending_orders: int
        low_stock_products: int
    
    @server.resolver("dashboardStats")
    async def dashboard_stats(root, info) -> DashboardStats:
        """Get dashboard statistics."""
        sql = """
            WITH stats AS (
                SELECT 
                    SUM(total_amount) as total_revenue,
                    COUNT(*) as total_orders,
                    COUNT(DISTINCT user_id) as unique_customers,
                    AVG(total_amount) as average_order_value,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders
                FROM orders
                WHERE order_date >= CURRENT_TIMESTAMP - INTERVAL '30 days'
            ),
            inventory AS (
                SELECT COUNT(*) as low_stock_products
                FROM products
                WHERE stock_quantity < reorder_point
            )
            SELECT * FROM stats, inventory
        """
        
        result = await server.executor.execute_query(sql)
        row = result.rows[0]
        
        return DashboardStats(
            total_revenue=float(row['total_revenue'] or 0),
            total_orders=row['total_orders'],
            unique_customers=row['unique_customers'],
            average_order_value=float(row['average_order_value'] or 0),
            pending_orders=row['pending_orders'],
            low_stock_products=row['low_stock_products']
        )
    
    @strawberry.type
    class ProductSales:
        product_name: str
        category: str
        units_sold: int
        revenue: float
        
    @server.resolver("topSellingProducts")
    async def top_selling_products(root, info, limit: int = 5) -> List[ProductSales]:
        """Get top selling products."""
        sql = f"""
            SELECT 
                p.name as product_name,
                p.category,
                SUM(oi.quantity) as units_sold,
                SUM(oi.line_total) as revenue
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status = 'completed'
            GROUP BY p.name, p.category
            ORDER BY revenue DESC
            LIMIT {limit}
        """
        
        result = await server.executor.execute_query(sql)
        
        return [
            ProductSales(
                product_name=row['product_name'],
                category=row['category'],
                units_sold=row['units_sold'],
                revenue=float(row['revenue'])
            )
            for row in result.rows
        ]
    
    # 3. Execute Example Queries
    print("\n3️⃣ Executing example queries...")
    schema = server.get_schema()
    
    # Query 1: Users with computed fields
    print("\n📊 Query 1: Users with computed fields")
    query1 = """
    query {
        users(limit: 3, orderBy: { total_spent: DESC }) {
            username
            total_spent
            order_count
            account_age_days
            average_order_value
        }
    }
    """
    
    result1 = await schema.execute(query1)
    if not result1.errors:
        for user in result1.data['users']:
            print(f"  - {user['username']}: ${user['total_spent']} spent, "
                  f"avg order ${user['average_order_value']}, "
                  f"account age {user['account_age_days']} days")
    
    # Query 2: Low stock products
    print("\n📦 Query 2: Low stock products")
    # Note: We need to manually check stock < reorder_point since we can't compare two fields directly
    query2 = """
    query {
        products {
            name
            stock_quantity
            reorder_point
            stock_status
        }
    }
    """
    
    result2 = await schema.execute(query2)
    if not result2.errors:
        products = result2.data['products']
        low_stock = [p for p in products if p['stock_status'] in ['OUT_OF_STOCK', 'LOW_STOCK']]
        print(f"  Found {len(low_stock)} low stock products:")
        for product in low_stock:
            print(f"  - {product['name']}: {product['stock_quantity']} units "
                  f"(reorder at {product['reorder_point']}) - {product['stock_status']}")
    
    # Query 3: Dashboard stats
    print("\n📈 Query 3: Dashboard statistics")
    query3 = """
    query {
        dashboardStats {
            totalRevenue
            totalOrders
            uniqueCustomers
            averageOrderValue
            pendingOrders
            lowStockProducts
        }
    }
    """
    
    result3 = await schema.execute(query3)
    if not result3.errors:
        stats = result3.data['dashboardStats']
        print(f"  Revenue: ${stats['totalRevenue']:,.2f}")
        print(f"  Orders: {stats['totalOrders']} ({stats['pendingOrders']} pending)")
        print(f"  Customers: {stats['uniqueCustomers']}")
        print(f"  AOV: ${stats['averageOrderValue']:.2f}")
        print(f"  Low Stock Products: {stats['lowStockProducts']}")
    
    # Query 4: Top selling products
    print("\n🏆 Query 4: Top selling products")
    query4 = """
    query {
        topSellingProducts(limit: 3) {
            productName
            category
            unitsSold
            revenue
        }
    }
    """
    
    result4 = await schema.execute(query4)
    if not result4.errors:
        for i, product in enumerate(result4.data['topSellingProducts'], 1):
            print(f"  {i}. {product['productName']} ({product['category']}): "
                  f"{product['unitsSold']} units, ${product['revenue']:,.2f}")
    
    # 4. Demonstrate Error Handling
    print("\n4️⃣ Demonstrating error handling...")
    
    # Try to query non-existent table
    error_query = """
    query {
        nonExistentTable {
            id
        }
    }
    """
    
    error_result = await schema.execute(error_query)
    if error_result.errors:
        print(f"  ❌ Expected error: {error_result.errors[0].message}")
    
    # Try invalid filter
    try:
        await server.executor.execute_query("SELECT * FROM users WHERE invalid_column = 1")
    except DuckQLError as e:
        print(f"\n  ❌ DuckQL Error:")
        print(f"     Message: {e.message}")
        print(f"     Code: {e.error_code}")
        print(f"     Suggestions: {e.suggestions}")
    
    # 5. Show Performance Statistics
    print("\n5️⃣ Performance statistics:")
    stats = server.get_stats()
    print(f"  Total queries executed: {stats['query_count']}")
    print(f"  Average query time: {stats['average_query_time_ms']:.2f}ms")
    print(f"  Total query time: {stats['total_query_time_ms']:.2f}ms")
    
    print("\n✅ Demo complete!")


if __name__ == "__main__":
    asyncio.run(main())