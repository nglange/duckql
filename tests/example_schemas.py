"""Example database schemas for testing and demonstrations."""

# E-commerce schema - common use case for analytics
ECOMMERCE_SCHEMA = """
-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    email VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR,
    customer_segment VARCHAR,
    lifetime_value DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT true
);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    sku VARCHAR NOT NULL UNIQUE,
    name VARCHAR NOT NULL,
    category VARCHAR,
    subcategory VARCHAR,
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    weight DECIMAL(8, 3),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    status VARCHAR NOT NULL,
    shipping_address JSON,
    total_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    shipping_amount DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    currency VARCHAR DEFAULT 'USD',
    payment_method VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_percent DECIMAL(5, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    total_amount DECIMAL(10, 2) NOT NULL
);

-- Reviews table
CREATE TABLE IF NOT EXISTS reviews (
    review_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR,
    comment TEXT,
    is_verified_purchase BOOLEAN DEFAULT false,
    helpful_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_customer ON reviews(customer_id);
"""

# Analytics/Metrics schema - for time series and dashboard use cases
ANALYTICS_SCHEMA = """
-- Events table for general event tracking
CREATE TABLE IF NOT EXISTS events (
    event_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    event_type VARCHAR NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR,
    session_id VARCHAR,
    properties JSON,
    -- Date for partitioning  
    event_date DATE
);

-- Page views for web analytics
CREATE TABLE IF NOT EXISTS page_views (
    view_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    user_id VARCHAR,
    page_url VARCHAR NOT NULL,
    referrer_url VARCHAR,
    timestamp TIMESTAMP NOT NULL,
    time_on_page_seconds INTEGER,
    bounce BOOLEAN DEFAULT false,
    device_type VARCHAR,
    browser VARCHAR,
    country VARCHAR,
    -- Derived columns for queries
    page_path VARCHAR,
    view_date DATE
);

-- Metrics table for time series data
CREATE TABLE IF NOT EXISTS metrics (
    metric_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    dimensions JSON,
    tags VARCHAR[],
    -- Common dimensions extracted for indexing
    environment VARCHAR,
    region VARCHAR,
    service VARCHAR
);

-- User analytics summary (materialized view pattern)
CREATE TABLE IF NOT EXISTS user_metrics_daily (
    user_id VARCHAR NOT NULL,
    date DATE NOT NULL,
    sessions_count INTEGER DEFAULT 0,
    page_views_count INTEGER DEFAULT 0,
    total_time_seconds INTEGER DEFAULT 0,
    bounce_rate DECIMAL(5, 4),
    pages_per_session DECIMAL(8, 2),
    events JSON,
    PRIMARY KEY (user_id, date)
);

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_events_type_date ON events(event_type, event_date);
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_page_views_session ON page_views(session_id);
CREATE INDEX IF NOT EXISTS idx_page_views_date ON page_views(view_date);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON metrics(metric_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_user_metrics_date ON user_metrics_daily(date);
"""

# Financial/Business metrics schema
FINANCIAL_SCHEMA = """
-- Revenue tracking
CREATE TABLE IF NOT EXISTS revenue (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    product_line VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    channel VARCHAR NOT NULL,
    revenue_amount DECIMAL(12, 2) NOT NULL,
    units_sold INTEGER NOT NULL,
    returns_amount DECIMAL(12, 2) DEFAULT 0,
    returns_count INTEGER DEFAULT 0,
    currency VARCHAR DEFAULT 'USD'
);

-- Expenses tracking
CREATE TABLE IF NOT EXISTS expenses (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    category VARCHAR NOT NULL,
    subcategory VARCHAR,
    vendor VARCHAR,
    amount DECIMAL(12, 2) NOT NULL,
    currency VARCHAR DEFAULT 'USD',
    department VARCHAR,
    is_recurring BOOLEAN DEFAULT false,
    notes TEXT
);

-- KPI metrics
CREATE TABLE IF NOT EXISTS kpi_metrics (
    id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DECIMAL(16, 4) NOT NULL,
    target_value DECIMAL(16, 4),
    previous_value DECIMAL(16, 4),
    year_over_year_change DECIMAL(8, 4),
    department VARCHAR,
    is_key_metric BOOLEAN DEFAULT false
);

-- Budget vs Actual
CREATE TABLE IF NOT EXISTS budget_tracking (
    id INTEGER PRIMARY KEY,
    fiscal_year INTEGER NOT NULL,
    fiscal_month INTEGER NOT NULL,
    department VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    budget_amount DECIMAL(12, 2) NOT NULL,
    actual_amount DECIMAL(12, 2) DEFAULT 0,
    variance_amount DECIMAL(12, 2),
    variance_percent DECIMAL(8, 4)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_revenue_date ON revenue(date);
CREATE INDEX IF NOT EXISTS idx_revenue_product_region ON revenue(product_line, region);
CREATE INDEX IF NOT EXISTS idx_expenses_date_category ON expenses(date, category);
CREATE INDEX IF NOT EXISTS idx_kpi_date_metric ON kpi_metrics(date, metric_name);
"""


def get_ecommerce_test_data():
    """Get test data for e-commerce schema."""
    return [
        # Customers
        """
        INSERT INTO customers (customer_id, email, name, country, customer_segment, lifetime_value) VALUES
        (1, 'john.doe@example.com', 'John Doe', 'USA', 'Premium', 5280.50),
        (2, 'jane.smith@example.com', 'Jane Smith', 'UK', 'Regular', 1250.00),
        (3, 'bob.wilson@example.com', 'Bob Wilson', 'Canada', 'Premium', 8900.75),
        (4, 'alice.johnson@example.com', 'Alice Johnson', 'USA', 'New', 150.00),
        (5, 'charlie.brown@example.com', 'Charlie Brown', 'Australia', 'Regular', 2100.50)
        """,
        
        # Products
        """
        INSERT INTO products (product_id, sku, name, category, subcategory, price, cost) VALUES
        (1, 'LAPTOP-001', 'Pro Laptop 15"', 'Electronics', 'Computers', 1299.99, 850.00),
        (2, 'PHONE-001', 'Smartphone X', 'Electronics', 'Mobile', 899.99, 450.00),
        (3, 'CHAIR-001', 'Ergonomic Office Chair', 'Furniture', 'Office', 399.99, 200.00),
        (4, 'BOOK-001', 'Python Programming', 'Books', 'Technology', 49.99, 20.00),
        (5, 'HDPHONE-001', 'Wireless Headphones', 'Electronics', 'Audio', 199.99, 80.00)
        """,
        
        # Orders
        """
        INSERT INTO orders (order_id, customer_id, order_date, status, total_amount, tax_amount, shipping_amount) VALUES
        (1, 1, '2024-01-15', 'completed', 1399.99, 100.00, 0.00),
        (2, 2, '2024-01-16', 'completed', 949.98, 50.00, 9.99),
        (3, 1, '2024-01-17', 'processing', 199.99, 15.00, 5.00),
        (4, 3, '2024-01-18', 'completed', 1699.98, 120.00, 0.00),
        (5, 4, '2024-01-19', 'cancelled', 49.99, 3.50, 5.00)
        """,
        
        # Order items
        """
        INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, total_amount) VALUES
        (1, 1, 1, 1, 1299.99, 1299.99),
        (2, 2, 2, 1, 899.99, 899.99),
        (3, 2, 4, 1, 49.99, 49.99),
        (4, 3, 5, 1, 199.99, 199.99),
        (5, 4, 1, 1, 1299.99, 1299.99),
        (6, 4, 3, 1, 399.99, 399.99),
        (7, 5, 4, 1, 49.99, 49.99)
        """,
        
        # Reviews
        """
        INSERT INTO reviews (review_id, product_id, customer_id, rating, title, comment, is_verified_purchase) VALUES
        (1, 1, 1, 5, 'Excellent laptop!', 'Fast and reliable. Great for development work.', true),
        (2, 2, 2, 4, 'Good phone', 'Battery life could be better but overall satisfied.', true),
        (3, 1, 3, 5, 'Best laptop ever', 'Perfect for my needs. Highly recommend!', true),
        (4, 5, 1, 4, 'Great sound quality', 'Comfortable and great audio. Worth the price.', true)
        """
    ]


def get_analytics_test_data():
    """Get test data for analytics schema."""
    return [
        # Events
        """
        INSERT INTO events (event_type, event_timestamp, user_id, session_id, properties, event_date) VALUES
        ('page_view', '2024-01-15 10:00:00', 'user_001', 'session_001', '{"page": "/home"}', '2024-01-15'),
        ('button_click', '2024-01-15 10:00:30', 'user_001', 'session_001', '{"button": "signup", "page": "/home"}', '2024-01-15'),
        ('page_view', '2024-01-15 10:01:00', 'user_001', 'session_001', '{"page": "/signup"}', '2024-01-15'),
        ('form_submit', '2024-01-15 10:02:00', 'user_001', 'session_001', '{"form": "registration"}', '2024-01-15'),
        ('page_view', '2024-01-15 11:00:00', 'user_002', 'session_002', '{"page": "/products"}', '2024-01-15')
        """,
        
        # Page views
        """
        INSERT INTO page_views (session_id, user_id, page_url, referrer_url, timestamp, time_on_page_seconds, device_type, browser, country, page_path, view_date) VALUES
        ('session_001', 'user_001', 'https://example.com/home', 'https://google.com', '2024-01-15 10:00:00', 30, 'desktop', 'Chrome', 'USA', '/home', '2024-01-15'),
        ('session_001', 'user_001', 'https://example.com/signup', 'https://example.com/home', '2024-01-15 10:01:00', 120, 'desktop', 'Chrome', 'USA', '/signup', '2024-01-15'),
        ('session_002', 'user_002', 'https://example.com/products', NULL, '2024-01-15 11:00:00', 180, 'mobile', 'Safari', 'UK', '/products', '2024-01-15'),
        ('session_002', 'user_002', 'https://example.com/products/laptop', 'https://example.com/products', '2024-01-15 11:03:00', 60, 'mobile', 'Safari', 'UK', '/products/laptop', '2024-01-15')
        """,
        
        # Metrics
        """
        INSERT INTO metrics (metric_name, metric_value, timestamp, environment, region, service, dimensions) VALUES
        ('api_latency_ms', 125.5, '2024-01-15 10:00:00', 'production', 'us-east', 'web-api', '{"endpoint": "/api/users", "method": "GET"}'),
        ('api_latency_ms', 89.2, '2024-01-15 10:01:00', 'production', 'us-east', 'web-api', '{"endpoint": "/api/products", "method": "GET"}'),
        ('cpu_usage_percent', 45.2, '2024-01-15 10:00:00', 'production', 'us-east', 'web-api', '{"instance": "web-01"}'),
        ('memory_usage_percent', 62.8, '2024-01-15 10:00:00', 'production', 'us-east', 'web-api', '{"instance": "web-01"}'),
        ('conversion_rate', 0.0234, '2024-01-15 23:59:59', 'production', 'global', 'analytics', '{"funnel": "signup", "cohort": "2024-01"}')
        """,
        
        # User metrics daily
        """
        INSERT INTO user_metrics_daily (user_id, date, sessions_count, page_views_count, total_time_seconds, bounce_rate, pages_per_session) VALUES
        ('user_001', '2024-01-15', 1, 2, 150, 0.0, 2.0),
        ('user_002', '2024-01-15', 1, 2, 240, 0.0, 2.0),
        ('user_001', '2024-01-14', 3, 8, 1200, 0.333, 2.67),
        ('user_003', '2024-01-15', 2, 5, 600, 0.5, 2.5)
        """
    ]


def get_financial_test_data():
    """Get test data for financial schema."""
    return [
        # Revenue
        """
        INSERT INTO revenue (id, date, product_line, region, channel, revenue_amount, units_sold, returns_amount, returns_count) VALUES
        (1, '2024-01-01', 'Software', 'North America', 'Direct', 125000.00, 50, 2500.00, 2),
        (2, '2024-01-01', 'Software', 'Europe', 'Partner', 89000.00, 35, 0.00, 0),
        (3, '2024-01-01', 'Hardware', 'North America', 'Direct', 234000.00, 120, 5600.00, 3),
        (4, '2024-01-01', 'Services', 'Asia', 'Direct', 67000.00, 25, 0.00, 0),
        (5, '2024-01-02', 'Software', 'North America', 'Direct', 143000.00, 58, 1200.00, 1)
        """,
        
        # Expenses
        """
        INSERT INTO expenses (id, date, category, subcategory, vendor, amount, department, is_recurring) VALUES
        (1, '2024-01-01', 'Salaries', 'Engineering', NULL, 250000.00, 'Engineering', true),
        (2, '2024-01-01', 'Marketing', 'Advertising', 'Google Ads', 15000.00, 'Marketing', false),
        (3, '2024-01-01', 'Infrastructure', 'Cloud', 'AWS', 23000.00, 'Engineering', true),
        (4, '2024-01-01', 'Office', 'Rent', 'WeWork', 18000.00, 'Operations', true),
        (5, '2024-01-02', 'Travel', 'Sales', 'Various', 5600.00, 'Sales', false)
        """,
        
        # KPI metrics
        """
        INSERT INTO kpi_metrics (id, date, metric_name, metric_value, target_value, previous_value, year_over_year_change, department, is_key_metric) VALUES
        (1, '2024-01-01', 'Monthly Recurring Revenue', 2850000.00, 3000000.00, 2750000.00, 0.15, 'Finance', true),
        (2, '2024-01-01', 'Customer Acquisition Cost', 125.50, 100.00, 142.30, -0.12, 'Marketing', true),
        (3, '2024-01-01', 'Gross Margin', 0.72, 0.75, 0.70, 0.03, 'Finance', true),
        (4, '2024-01-01', 'Customer Churn Rate', 0.052, 0.050, 0.061, -0.15, 'Customer Success', true),
        (5, '2024-01-01', 'Employee Satisfaction', 4.2, 4.5, 4.0, 0.05, 'HR', false)
        """,
        
        # Budget tracking
        """
        INSERT INTO budget_tracking (id, fiscal_year, fiscal_month, department, category, budget_amount, actual_amount, variance_amount, variance_percent) VALUES
        (1, 2024, 1, 'Engineering', 'Salaries', 260000.00, 250000.00, -10000.00, -3.85),
        (2, 2024, 1, 'Engineering', 'Infrastructure', 25000.00, 23000.00, -2000.00, -8.00),
        (3, 2024, 1, 'Marketing', 'Advertising', 20000.00, 15000.00, -5000.00, -25.00),
        (4, 2024, 1, 'Sales', 'Travel', 10000.00, 5600.00, -4400.00, -44.00),
        (5, 2024, 1, 'Operations', 'Office', 20000.00, 18000.00, -2000.00, -10.00)
        """
    ]


def get_all_schemas():
    """Get all schema creation SQL statements."""
    return [ECOMMERCE_SCHEMA, ANALYTICS_SCHEMA, FINANCIAL_SCHEMA]