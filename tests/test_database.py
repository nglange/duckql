"""Test database creation with comprehensive edge cases."""

import duckdb
from datetime import datetime, date, timedelta
import json
import uuid


def create_test_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with various edge cases and data types."""
    conn = duckdb.connect(":memory:")
    
    # Table 1: Simple types
    conn.execute("""
        CREATE TABLE simple_types (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            age SMALLINT,
            balance DECIMAL(10, 2),
            is_active BOOLEAN,
            created_date DATE,
            updated_at TIMESTAMP,
            notes TEXT
        )
    """)
    
    # Table 2: Edge case types
    conn.execute("""
        CREATE TABLE edge_types (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            big_number BIGINT,
            huge_number HUGEINT,
            tiny_number TINYINT,
            real_number REAL,
            double_number DOUBLE,
            binary_data BLOB,
            time_only TIME,
            interval_data INTERVAL,
            -- Special values
            null_field VARCHAR,
            empty_string VARCHAR,
            zero_int INTEGER,
            false_bool BOOLEAN
        )
    """)
    
    # Table 3: JSON and complex types
    conn.execute("""
        CREATE TABLE json_types (
            id INTEGER PRIMARY KEY,
            config JSON,
            metadata JSON,
            nested_json JSON,
            empty_json JSON,
            null_json JSON,
            array_json JSON,
            -- Arrays
            int_array INTEGER[],
            string_array VARCHAR[],
            -- Mixed content
            mixed_data JSON
        )
    """)
    
    # Table 4: Special characters and SQL injection attempts
    conn.execute("""
        CREATE TABLE special_chars (
            id INTEGER PRIMARY KEY,
            normal_text VARCHAR,
            text_with_quotes VARCHAR,
            text_with_semicolon VARCHAR,
            text_with_newlines VARCHAR,
            text_with_unicode VARCHAR,
            text_with_injection VARCHAR,
            sql_keyword_name VARCHAR  -- Column named after SQL keyword
        )
    """)
    
    # Table 5: Relationships and foreign keys
    conn.execute("""
        CREATE TABLE parent_table (
            parent_id INTEGER PRIMARY KEY,
            parent_name VARCHAR NOT NULL
        )
    """)
    
    conn.execute("""
        CREATE TABLE child_table (
            child_id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            child_name VARCHAR,
            FOREIGN KEY (parent_id) REFERENCES parent_table(parent_id)
        )
    """)
    
    # Table 6: Large dataset for performance testing
    conn.execute("""
        CREATE TABLE large_dataset (
            id INTEGER PRIMARY KEY,
            category VARCHAR,
            value DOUBLE,
            timestamp TIMESTAMP,
            data JSON
        )
    """)
    
    # Table 7: Reserved words and naming edge cases
    conn.execute("""
        CREATE TABLE "order" (
            "select" INTEGER PRIMARY KEY,
            "from" VARCHAR,
            "where" BOOLEAN,
            "user" VARCHAR,
            "table" VARCHAR,
            -- Mixed case
            "CamelCase" VARCHAR,
            "snake_case" VARCHAR,
            "kebab-case-invalid" VARCHAR,
            -- Numbers in names
            "column123" INTEGER,
            "123start" INTEGER  -- Invalid in some systems
        )
    """)
    
    # Table 8: All numeric types
    conn.execute("""
        CREATE TABLE numeric_precision (
            id INTEGER PRIMARY KEY,
            -- Signed integers
            tinyint_col TINYINT,           -- -128 to 127
            smallint_col SMALLINT,         -- -32768 to 32767
            integer_col INTEGER,           -- -2147483648 to 2147483647
            bigint_col BIGINT,            -- -9223372036854775808 to 9223372036854775807
            hugeint_col HUGEINT,          -- 128-bit signed
            -- Unsigned integers
            utinyint_col UTINYINT,         -- 0 to 255
            usmallint_col USMALLINT,       -- 0 to 65535
            uinteger_col UINTEGER,         -- 0 to 4294967295
            ubigint_col UBIGINT,          -- 0 to 18446744073709551615
            -- Floating point
            real_col REAL,                -- 32-bit IEEE 754
            double_col DOUBLE,            -- 64-bit IEEE 754
            -- Fixed precision
            decimal_col DECIMAL(18, 6),
            numeric_col NUMERIC(10, 2)
        )
    """)
    
    # Table 9: Empty table (no data)
    conn.execute("""
        CREATE TABLE empty_table (
            id INTEGER PRIMARY KEY,
            data VARCHAR
        )
    """)
    
    # Insert test data
    
    # Simple types data
    conn.execute("""
        INSERT INTO simple_types VALUES
        (1, 'Alice', 25, 1000.50, true, '2024-01-01', '2024-01-01 10:00:00', 'First user'),
        (2, 'Bob', 30, 2500.00, true, '2024-01-02', '2024-01-02 11:30:00', 'Second user'),
        (3, 'Charlie', NULL, 0.00, false, '2024-01-03', '2024-01-03 09:15:00', NULL),
        (4, '', 0, -100.50, true, '2024-01-04', '2024-01-04 14:45:00', ''),
        (5, NULL, -1, NULL, NULL, NULL, NULL, 'All nulls except id and notes')
    """)
    
    # Edge types data
    conn.execute("""
        INSERT INTO edge_types (
            big_number, huge_number, tiny_number, real_number, double_number,
            binary_data, time_only, interval_data,
            null_field, empty_string, zero_int, false_bool
        ) VALUES
        (9223372036854775807, 170141183460469231731687303715884105727, 127, 3.14159, 2.718281828,
         'binary'::BLOB, '13:45:30', INTERVAL '1 year 2 months 3 days 4 hours 5 minutes',
         NULL, '', 0, false),
        (-9223372036854775808, -170141183460469231731687303715884105727, -128, -0.0, 1.7976931348623157e+308,
         '\x00\x01\x02\x03'::BLOB, '00:00:00', INTERVAL '-1 day',
         NULL, '', 0, false)
    """)
    
    # JSON types data
    conn.execute("""
        INSERT INTO json_types VALUES
        (1, '{"key": "value"}', '{"meta": true}', '{"nested": {"deep": {"value": 42}}}', 
         '{}', NULL, '[1, 2, 3]', ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'],
         '{"string": "text", "number": 123, "bool": true, "null": null, "array": [1, 2], "object": {}}'),
        (2, '{"special": "quote\"s and \\backslash"}', '{"unicode": "émojis 🦆"}', 
         '{"sql": "SELECT * FROM users; DROP TABLE users;--"}',
         '{}', NULL, '[]', ARRAY[]::INTEGER[], ARRAY[]::VARCHAR[],
         '{"deeply": {"nested": {"structure": {"with": {"many": {"levels": {"value": "deep"}}}}}}}')
    """)
    
    # Special characters data
    conn.execute("""
        INSERT INTO special_chars VALUES
        (1, 'Normal text', 'Text with "quotes"', 'Text with; semicolon', 
         E'Text with\nnewlines\nand\ttabs', 'Unicode: 你好世界 🌍', 
         'Robert''); DROP TABLE students;--', 'SELECT'),
        (2, 'Simple', 'O''Reilly', 'Semi;colon;separated', 
         E'Line 1\nLine 2\nLine 3', '∑∏∫≈≠∞', 
         '1 OR 1=1', 'FROM'),
        (3, '<script>alert("XSS")</script>', '{"json": "with \\"quotes\\""}', 
         'UNION SELECT * FROM passwords', E'\r\n\r\n', '🦆🦆🦆', 
         '\'; DELETE FROM users WHERE ''t''=''t', 'WHERE')
    """)
    
    # Parent-child data
    conn.execute("""
        INSERT INTO parent_table VALUES
        (1, 'Parent One'),
        (2, 'Parent Two'),
        (3, 'Parent Three')
    """)
    
    conn.execute("""
        INSERT INTO child_table VALUES
        (1, 1, 'Child 1-1'),
        (2, 1, 'Child 1-2'),
        (3, 2, 'Child 2-1'),
        (4, NULL, 'Orphan Child')
    """)
    
    # Large dataset (1000 rows)
    conn.execute("""
        INSERT INTO large_dataset
        SELECT 
            row_number() OVER () as id,
            CASE (row_number() OVER ()) % 5
                WHEN 0 THEN 'category_a'
                WHEN 1 THEN 'category_b'
                WHEN 2 THEN 'category_c'
                WHEN 3 THEN 'category_d'
                ELSE 'category_e'
            END as category,
            random() * 1000 as value,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER ()) MINUTE as timestamp,
            json_object('index', row_number() OVER (), 'random', random()) as data
        FROM generate_series(1, 1000)
    """)
    
    # Reserved words table
    conn.execute("""
        INSERT INTO "order" VALUES
        (1, 'value1', true, 'user1', 'table1', 'CamelValue', 'snake_value', 'kebab-value', 123, 456),
        (2, 'value2', false, 'user2', 'table2', 'AnotherCamel', 'another_snake', 'another-kebab', 789, 999)
    """)
    
    # Numeric precision data - test boundary values
    conn.execute("""
        INSERT INTO numeric_precision VALUES
        (1, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727,
         255, 65535, 4294967295, 18446744073709551615,
         3.4028235e+38, 1.7976931348623157e+308,
         999999999999.999999, 99999999.99),
        (2, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105727,
         0, 0, 0, 0,
         -3.4028235e+38, -1.7976931348623157e+308,
         -999999999999.999999, -99999999.99),
        (3, 0, 0, 0, 0, 0, 128, 32768, 2147483648, 9223372036854775808,
         0.0, -0.0, 0.000001, 0.01)
    """)
    
    # Create some views for testing
    conn.execute("""
        CREATE VIEW active_users AS
        SELECT id, name, age, balance
        FROM simple_types
        WHERE is_active = true
    """)
    
    conn.execute("""
        CREATE VIEW parent_child_view AS
        SELECT 
            p.parent_id,
            p.parent_name,
            c.child_id,
            c.child_name
        FROM parent_table p
        LEFT JOIN child_table c ON p.parent_id = c.parent_id
    """)
    
    # Create a table with computed/generated columns if supported
    try:
        conn.execute("""
            CREATE TABLE computed_columns (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR,
                last_name VARCHAR,
                full_name VARCHAR GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.execute("""
            INSERT INTO computed_columns (id, first_name, last_name) VALUES
            (1, 'John', 'Doe'),
            (2, 'Jane', 'Smith')
        """)
    except:
        # Generated columns might not be supported
        pass
    
    return conn


def create_pidgin_test_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with the full Pidgin schema."""
    conn = duckdb.connect(":memory:")
    
    # Use the actual Pidgin schema
    from pidgin_schema import get_all_schemas
    
    # Create all tables
    for schema_sql in get_all_schemas():
        conn.execute(schema_sql)
    
    # Insert realistic test data
    
    # Experiments
    conn.execute("""
        INSERT INTO experiments VALUES
        ('exp_test_001', 'Edge Case Testing', '2024-01-01 10:00:00'::TIMESTAMP, 
         '2024-01-01 10:05:00'::TIMESTAMP, '2024-01-01 12:00:00'::TIMESTAMP, 'completed',
         '{"models": ["gpt-4", "claude-3"], "temperature": 0.7, "max_turns": 50}'::JSON,
         100, 95, 5,
         '{"researcher": "Dr. Test", "purpose": "system validation"}'::JSON),
        ('exp_null_test', 'Null Value Experiment', '2024-01-02 10:00:00'::TIMESTAMP,
         NULL, NULL, 'created',
         NULL, 0, 0, 0, NULL)
    """)
    
    # Conversations with various edge cases
    conn.execute("""
        INSERT INTO conversations VALUES
        ('conv_normal_001', 'exp_test_001', '2024-01-01 10:10:00'::TIMESTAMP,
         '2024-01-01 10:10:30'::TIMESTAMP, '2024-01-01 10:15:00'::TIMESTAMP, 'completed',
         'gpt-4', 'openai', 0.7, 'Alice',
         'claude-3-opus', 'anthropic', 0.7, 'Bob',
         'Hello, let''s have a conversation!', 50, 'agent_a',
         45, 0.92, 'High convergence achieved', 270000,
         NULL, NULL, NULL, false),
        ('conv_error_001', 'exp_test_001', '2024-01-01 10:20:00'::TIMESTAMP,
         '2024-01-01 10:20:30'::TIMESTAMP, NULL, 'failed',
         'gpt-4', 'openai', 0.9, NULL,
         'claude-3-opus', 'anthropic', 0.9, NULL,
         'Test error handling', 50, 'agent_b',
         5, NULL, NULL, 30000,
         'Context length exceeded', 'context_overflow', '2024-01-01 10:21:00'::TIMESTAMP, true)
    """)
    
    # Turn metrics with all ~80 columns
    conn.execute("""
        INSERT INTO turn_metrics VALUES
        ('conv_normal_001', 1, '2024-01-01 10:10:35'::TIMESTAMP,
         -- Convergence metrics
         0.45, 0.30, 0.25, 0.40, 0.35,
         0.30, 0.15, 0.20, 0.18, 0.19,
         -- Agent A message metrics (23 fields)
         150, 25, 20, 0.80, 5.2, 500,
         2, 1, 12.5, 1, 0, 3, 2, 1,
         4.5, 0.85, 0.75, 0.15, 0.05, 0.03, 0.65, true, 20,
         -- Agent A linguistic markers (7 fields)
         2, 1, 0, 3, 2, 0, 1,
         -- Agent B message metrics (23 fields)
         145, 24, 19, 0.79, 5.1, 480,
         2, 1, 12.0, 0, 1, 2, 1, 2,
         4.4, 0.84, 0.74, 0.14, 0.04, 0.02, 0.64, true, 19,
         -- Agent B linguistic markers (7 fields)
         1, 2, 0, 2, 1, 1, 2,
         -- JSON fields
         '{"hello": 2, "world": 1, "how": 1, "are": 1}'::JSON,
         '{"hello": 1, "hi": 1, "there": 1, "how": 1}'::JSON,
         '["hello", "how"]'::JSON,
         -- Timing
         '2024-01-01 10:10:30'::TIMESTAMP, '2024-01-01 10:10:35'::TIMESTAMP, 5000)
    """)
    
    # Messages
    conn.execute("""
        INSERT INTO messages VALUES
        ('conv_normal_001', 1, 'agent_a', 
         'Hello! How are you today? I''m excited to have this conversation.', 
         '2024-01-01 10:10:35'::TIMESTAMP, 15, 15),
        ('conv_normal_001', 1, 'agent_b',
         'Hi there! I''m doing well, thank you. I''m also looking forward to our chat.',
         '2024-01-01 10:10:35'::TIMESTAMP, 16, 16)
    """)
    
    # Events with JSON data
    conn.execute("""
        INSERT INTO events VALUES
        (gen_random_uuid(), '2024-01-01 10:00:00'::TIMESTAMP, 'experiment_started',
         NULL, 'exp_test_001', 
         '{"action": "start", "config": {"temperature": 0.7}}'::JSON,
         '2024-01-01'::DATE, 1),
        (gen_random_uuid(), '2024-01-01 10:10:30'::TIMESTAMP, 'conversation_started',
         'conv_normal_001', 'exp_test_001',
         '{"agents": ["gpt-4", "claude-3-opus"]}'::JSON,
         '2024-01-01'::DATE, 2)
    """)
    
    # Token usage
    conn.execute("""
        INSERT INTO token_usage VALUES
        (gen_random_uuid(), '2024-01-01 10:10:35'::TIMESTAMP, 'conv_normal_001',
         'openai', 'gpt-4',
         100, 15, 115,
         3000, 150000, 0.001, 0.001,
         0.30, 0.03, 0.33)
    """)
    
    # Context truncations
    conn.execute("""
        INSERT INTO context_truncations VALUES
        ('conv_error_001', 'exp_test_001', 'agent_a', 5, 3, '2024-01-01 10:21:00'::TIMESTAMP)
    """)
    
    return conn


if __name__ == "__main__":
    # Create test databases
    print("Creating comprehensive test database...")
    test_db = create_test_database()
    
    print("Creating Pidgin test database...")
    pidgin_db = create_pidgin_test_database()
    
    print("Test databases created successfully!")
    
    # Quick verification
    print("\nTest database tables:")
    tables = test_db.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    print("\nPidgin database tables:")
    tables = pidgin_db.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")