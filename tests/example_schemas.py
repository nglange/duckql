"""Example database schemas for testing."""

# E-commerce schema
ECOMMERCE_SCHEMA = """
CREATE TABLE orders (
    order_id VARCHAR PRIMARY KEY,
    customer_email VARCHAR,
    items JSON,
    total_amount DECIMAL(10, 2),
    status VARCHAR,
    created_at TIMESTAMP,
    metadata JSON
);
"""

def get_ecommerce_test_data():
    """Get test data for e-commerce schema."""
    return [
        """
        INSERT INTO orders VALUES
        ('ORD-001', 'alice@example.com', '[{"product": "Widget", "quantity": 2}]', 
         59.98, 'completed', TIMESTAMP '2024-01-15 10:00:00', '{"source": "web"}')
        """
    ]

# Analytics schema
ANALYTICS_SCHEMA = """
CREATE TABLE events (
    event_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id INTEGER,
    event_type VARCHAR,
    properties JSON,
    occurred_at TIMESTAMP
);
"""

def get_analytics_test_data():
    """Get test data for analytics schema."""
    return [
        """
        INSERT INTO events VALUES
        ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1, 'page_view', 
         '{"page": "/home", "duration": 30}', TIMESTAMP '2024-01-01 10:00:00')
        """
    ]

# Financial schema
FINANCIAL_SCHEMA = """
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    amount DECIMAL(10, 2),
    transaction_type VARCHAR,
    created_at TIMESTAMP
);
"""

def get_financial_test_data():
    """Get test data for financial schema."""
    return [
        """
        INSERT INTO transactions VALUES
        (1, 1001, 100.00, 'deposit', TIMESTAMP '2024-01-01 09:00:00')
        """
    ]