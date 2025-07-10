"""Example usage of DuckQL with common analytics scenarios."""

import duckdb
from duckql import DuckQL
from datetime import datetime, timedelta

# Create an e-commerce analytics database
def create_ecommerce_analytics_db():
    conn = duckdb.connect(":memory:")
    
    # Sales data table
    conn.execute("""
        CREATE TABLE sales (
            sale_id INTEGER PRIMARY KEY,
            order_date DATE NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            discount_percent DECIMAL(5, 2) DEFAULT 0,
            revenue DECIMAL(10, 2),
            region VARCHAR NOT NULL,
            sales_channel VARCHAR NOT NULL
        )
    """)
    
    # Customer segments table
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name VARCHAR NOT NULL,
            segment VARCHAR NOT NULL,
            lifetime_value DECIMAL(12, 2),
            first_purchase_date DATE,
            last_purchase_date DATE,
            total_orders INTEGER DEFAULT 0
        )
    """)
    
    # Product inventory table
    conn.execute("""
        CREATE TABLE inventory (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR NOT NULL,
            category VARCHAR NOT NULL,
            current_stock INTEGER NOT NULL,
            reorder_point INTEGER NOT NULL,
            unit_cost DECIMAL(10, 2) NOT NULL,
            supplier VARCHAR,
            last_restock_date DATE
        )
    """)
    
    # Web analytics table
    conn.execute("""
        CREATE TABLE page_views (
            view_id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            user_id VARCHAR,
            page_url VARCHAR NOT NULL,
            referrer VARCHAR,
            device_type VARCHAR,
            session_duration_seconds INTEGER,
            converted BOOLEAN DEFAULT false,
            conversion_value DECIMAL(10, 2)
        )
    """)
    
    # Insert sample data
    base_date = datetime.now().date()
    
    # Customers
    conn.execute("""
        INSERT INTO customers VALUES 
        (1, 'Acme Corp', 'Enterprise', 125000.00, '2023-01-15', '2024-01-10', 45),
        (2, 'TechStart Inc', 'SMB', 35000.00, '2023-06-20', '2024-01-08', 12),
        (3, 'Global Retail', 'Enterprise', 250000.00, '2022-11-01', '2024-01-11', 89),
        (4, 'Local Shop', 'SMB', 15000.00, '2023-09-10', '2023-12-20', 8),
        (5, 'MegaMart', 'Enterprise', 180000.00, '2023-02-28', '2024-01-09', 67)
    """)
    
    # Products/Inventory
    conn.execute("""
        INSERT INTO inventory VALUES 
        (1, 'Laptop Pro 15"', 'Electronics', 45, 20, 800.00, 'TechSupplier Co', '2024-01-01'),
        (2, 'Wireless Mouse', 'Electronics', 120, 50, 15.00, 'TechSupplier Co', '2024-01-05'),
        (3, 'Office Chair', 'Furniture', 30, 10, 120.00, 'FurnitureMax', '2023-12-28'),
        (4, 'Standing Desk', 'Furniture', 15, 5, 300.00, 'FurnitureMax', '2023-12-20'),
        (5, 'Monitor 27"', 'Electronics', 60, 25, 250.00, 'TechSupplier Co', '2024-01-03')
    """)
    
    # Generate sales data for the last 30 days
    sales_data = []
    sale_id = 1
    for days_ago in range(30, 0, -1):
        date = base_date - timedelta(days=days_ago)
        # Generate 5-20 sales per day
        daily_sales = 5 + (days_ago % 16)
        for _ in range(daily_sales):
            customer_id = 1 + (sale_id % 5)
            product_id = 1 + (sale_id % 5)
            quantity = 1 + (sale_id % 3)
            discount = 0 if sale_id % 4 else 10
            region = ['North America', 'Europe', 'Asia', 'South America'][sale_id % 4]
            channel = ['Online', 'Retail', 'Partner'][sale_id % 3]
            
            # Get product info for realistic pricing
            product_names = ['Laptop Pro 15"', 'Wireless Mouse', 'Office Chair', 'Standing Desk', 'Monitor 27"']
            categories = ['Electronics', 'Electronics', 'Furniture', 'Furniture', 'Electronics']
            prices = [1299.99, 29.99, 249.99, 599.99, 399.99]
            
            product_idx = (product_id - 1) % 5
            price = prices[product_idx]
            revenue = quantity * price * (1 - discount/100)
            
            sales_data.append(f"""
                ({sale_id}, '{date}', {customer_id}, {product_id}, 
                 '{product_names[product_idx]}', '{categories[product_idx]}',
                 {quantity}, {price}, {discount}, {revenue:.2f},
                 '{region}', '{channel}')
            """)
            sale_id += 1
    
    conn.execute(f"""
        INSERT INTO sales (sale_id, order_date, customer_id, product_id, 
                         product_name, category, quantity, unit_price, 
                         discount_percent, revenue, region, sales_channel) 
        VALUES {','.join(sales_data)}
    """)
    
    # Page views data
    page_views_data = []
    view_id = 1
    for hours_ago in range(48, 0, -1):
        timestamp = datetime.now() - timedelta(hours=hours_ago)
        # Generate 10-50 views per hour
        hourly_views = 10 + (hours_ago * 7) % 41
        for _ in range(hourly_views):
            user_id = f"user_{view_id % 100:03d}" if view_id % 3 else None
            pages = ['/home', '/products', '/products/laptop', '/products/chair', '/checkout', '/about']
            page = pages[view_id % len(pages)]
            referrers = ['google.com', 'facebook.com', 'direct', 'email', None]
            referrer = referrers[view_id % len(referrers)]
            devices = ['desktop', 'mobile', 'tablet']
            device = devices[view_id % len(devices)]
            duration = 30 + (view_id * 17) % 300
            converted = view_id % 20 == 0
            conversion_value = prices[view_id % 5] if converted else None
            
            page_views_data.append(f"""
                ({view_id}, '{timestamp}', {f"'{user_id}'" if user_id else 'NULL'}, 
                 '{page}', {f"'{referrer}'" if referrer else 'NULL'}, '{device}',
                 {duration}, {converted}, {conversion_value if conversion_value else 'NULL'})
            """)
            view_id += 1
    
    conn.execute(f"""
        INSERT INTO page_views (view_id, timestamp, user_id, page_url, 
                              referrer, device_type, session_duration_seconds,
                              converted, conversion_value)
        VALUES {','.join(page_views_data)}
    """)
    
    return conn


def main():
    # Create sample database
    print("🦆 Creating e-commerce analytics database...")
    conn = create_ecommerce_analytics_db()
    
    # Create DuckQL server
    print("🚀 Initializing DuckQL server...")
    server = DuckQL(conn)
    
    # Add computed fields
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
    
    @server.computed_field("customers", "avg_order_value")
    def avg_order_value(obj) -> float:
        ltv = obj.get("lifetime_value", 0)
        orders = obj.get("total_orders", 1)
        return round(ltv / orders, 2) if orders > 0 else 0.0
    
    # Add custom resolvers for analytics
    @server.resolver("sales_dashboard")
    async def sales_dashboard(root, info, days: int = 7) -> dict:
        sql = f"""
            SELECT 
                COUNT(*) as total_orders,
                SUM(revenue) as total_revenue,
                AVG(revenue) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(quantity) as units_sold
            FROM sales
            WHERE order_date >= CURRENT_DATE - INTERVAL '{days} days'
        """
        result = await server.executor.execute_query(sql)
        return result.rows[0] if result.rows else None
    
    @server.resolver("top_products")
    async def top_products(root, info, limit: int = 10) -> list:
        sql = f"""
            SELECT 
                product_name,
                category,
                SUM(quantity) as units_sold,
                SUM(revenue) as total_revenue,
                COUNT(*) as order_count
            FROM sales
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY product_name, category
            ORDER BY total_revenue DESC
            LIMIT {limit}
        """
        result = await server.executor.execute_query(sql)
        return result.rows
    
    print("\n📊 Available GraphQL queries:")
    print("  - sales: Query sales transactions")
    print("  - customers: Query customer segments") 
    print("  - inventory: Query product inventory with stock status")
    print("  - pageViews: Query web analytics data")
    print("  - sales_dashboard(days): Get sales KPIs for last N days")
    print("  - top_products(limit): Get best-selling products")
    
    print("\n🎯 Example GraphQL queries to try:")
    print("""
1. Sales dashboard for last week:
   query {
     sales_dashboard(days: 7) {
       total_orders
       total_revenue
       avg_order_value
       unique_customers
       units_sold
     }
   }

2. Low stock alert:
   query {
     inventory(where: { current_stock_lt: reorder_point }) {
       product_name
       category
       current_stock
       reorder_point
       stock_status
       supplier
     }
   }

3. Top customers by revenue:
   query {
     customers(orderBy: { lifetime_value: DESC }, limit: 5) {
       customer_name
       segment
       lifetime_value
       total_orders
       avg_order_value
     }
   }

4. Sales by region (using aggregation):
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

5. Conversion funnel analysis:
   query {
     pageViews(
       where: { converted_eq: true }
       orderBy: { timestamp: DESC }
       limit: 20
     ) {
       timestamp
       page_url
       device_type
       conversion_value
       session_duration_seconds
     }
   }
    """)
    
    # Start server
    print("\n🚀 Starting GraphQL server at http://localhost:8000/graphql")
    print("   Open GraphQL playground to explore the API interactively!")
    server.serve(port=8000)


if __name__ == "__main__":
    main()