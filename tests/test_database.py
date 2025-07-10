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
            int_array INTEGER[],
            string_array VARCHAR[],
            complex_json JSON
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
            text_with_tabs VARCHAR,
            text_with_unicode VARCHAR,
            text_with_emojis VARCHAR,
            sql_injection_attempt VARCHAR,
            markdown_text TEXT,
            html_text TEXT
        )
    """)
    
    # Table 5: Parent-child relationship
    conn.execute("""
        CREATE TABLE parent_table (
            parent_id INTEGER PRIMARY KEY,
            parent_name VARCHAR NOT NULL,
            parent_value DECIMAL(10, 2)
        )
    """)
    
    conn.execute("""
        CREATE TABLE child_table (
            child_id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            child_name VARCHAR,
            child_order INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parent_table(parent_id)
        )
    """)
    
    # Table 6: Wide table (many columns)
    conn.execute("""
        CREATE TABLE wide_table (
            id INTEGER PRIMARY KEY,
            col_001 VARCHAR, col_002 VARCHAR, col_003 VARCHAR, col_004 VARCHAR, col_005 VARCHAR,
            col_006 INTEGER, col_007 INTEGER, col_008 INTEGER, col_009 INTEGER, col_010 INTEGER,
            col_011 DECIMAL(10,2), col_012 DECIMAL(10,2), col_013 DECIMAL(10,2), col_014 DECIMAL(10,2), col_015 DECIMAL(10,2),
            col_016 BOOLEAN, col_017 BOOLEAN, col_018 BOOLEAN, col_019 BOOLEAN, col_020 BOOLEAN,
            col_021 DATE, col_022 DATE, col_023 DATE, col_024 DATE, col_025 DATE,
            col_026 TIMESTAMP, col_027 TIMESTAMP, col_028 TIMESTAMP, col_029 TIMESTAMP, col_030 TIMESTAMP,
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
         '\\x00\\x01\\x02\\x03'::BLOB, '00:00:00', INTERVAL '-1 day',
         NULL, '', 0, false)
    """)
    
    # JSON types data
    conn.execute("""
        INSERT INTO json_types VALUES
        (1, '{"key": "value"}', '{"meta": true}', '{"nested": {"deep": {"value": 42}}}', 
         '{}', NULL, '[1, 2, 3]', ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'],
         '{"string": "text", "number": 123, "bool": true, "null": null, "array": [1, 2], "object": {}}'),
        (2, '{"special": "quote\\"s and \\\\backslash"}', '{"unicode": "émojis 🦆"}', 
         '{"sql": "SELECT * FROM users; DROP TABLE users;--"}',
         '{}', NULL, '[]', ARRAY[]::INTEGER[], ARRAY[]::VARCHAR[],
         '{"deeply": {"nested": {"structure": {"with": {"many": {"levels": {"value": "deep"}}}}}}}')
    """)
    
    # Special characters data
    conn.execute("""
        INSERT INTO special_chars VALUES
        (1, 'Normal text', 'Text with "quotes"', 'Text with; semicolon',
         E'Text with\\nnewlines', E'Text with\\ttabs', 'Unicode: 你好世界 🌍',
         'Emojis: 😀🎉🦆💻', 'Robert''); DROP TABLE students;--',
         '# Markdown\\n\\n- List item\\n- Another item\\n\\n**Bold** and *italic*',
         '<h1>HTML</h1><p>Paragraph with <a href="#">link</a></p>'),
        (2, 'Another normal', E'It''s a single quote', 'Semi;colon;delimited;data',
         E'Line 1\\nLine 2\\nLine 3', E'Col1\\tCol2\\tCol3', 'Ελληνικά Greek',
         '🏃‍♂️🏃‍♀️ Running people', '"; DELETE FROM users WHERE 1=1;--',
         '## Heading 2\\n\\nCode block:\\n```python\\nprint("hello")\\n```',
         '<script>alert("XSS")</script><img src=x onerror=alert(1)>')
    """)
    
    # Parent-child data
    conn.execute("""
        INSERT INTO parent_table VALUES
        (1, 'Parent One', 100.00),
        (2, 'Parent Two', 200.00),
        (3, 'Parent Three', 300.00)
    """)
    
    conn.execute("""
        INSERT INTO child_table VALUES
        (1, 1, 'Child 1-1', 1),
        (2, 1, 'Child 1-2', 2),
        (3, 2, 'Child 2-1', 1),
        (4, 3, 'Child 3-1', 1),
        (5, 3, 'Child 3-2', 2),
        (6, 3, 'Child 3-3', 3)
    """)
    
    # Wide table - using generate_series for bulk data
    conn.execute("""
        INSERT INTO wide_table 
        SELECT 
            row_number() OVER () as id,
            'value_' || (row_number() OVER () % 5) as col_001,
            'data_' || (row_number() OVER () % 3) as col_002,
            'text_' || (row_number() OVER () % 7) as col_003,
            'info_' || (row_number() OVER () % 2) as col_004,
            'desc_' || (row_number() OVER () % 4) as col_005,
            row_number() OVER () % 100 as col_006,
            row_number() OVER () % 200 as col_007,
            row_number() OVER () % 300 as col_008,
            row_number() OVER () % 400 as col_009,
            row_number() OVER () % 500 as col_010,
            (row_number() OVER () % 1000) * 1.5 as col_011,
            (row_number() OVER () % 2000) * 2.5 as col_012,
            (row_number() OVER () % 3000) * 3.5 as col_013,
            (row_number() OVER () % 4000) * 4.5 as col_014,
            (row_number() OVER () % 5000) * 5.5 as col_015,
            (row_number() OVER () % 2) = 0 as col_016,
            (row_number() OVER () % 3) = 0 as col_017,
            (row_number() OVER () % 4) = 0 as col_018,
            (row_number() OVER () % 5) = 0 as col_019,
            (row_number() OVER () % 6) = 0 as col_020,
            DATE '2024-01-01' + INTERVAL (row_number() OVER () % 30) DAY as col_021,
            DATE '2024-02-01' + INTERVAL (row_number() OVER () % 28) DAY as col_022,
            DATE '2024-03-01' + INTERVAL (row_number() OVER () % 31) DAY as col_023,
            DATE '2024-04-01' + INTERVAL (row_number() OVER () % 30) DAY as col_024,
            DATE '2024-05-01' + INTERVAL (row_number() OVER () % 31) DAY as col_025,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER ()) HOUR as col_026,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER ()) MINUTE as col_027,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER ()) SECOND as col_028,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER () * 2) HOUR as col_029,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER () * 3) MINUTE as col_030,
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


def create_ecommerce_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with e-commerce schema."""
    conn = duckdb.connect(":memory:")
    
    # Create schema
    from .example_schemas import ECOMMERCE_SCHEMA, get_ecommerce_test_data
    conn.execute(ECOMMERCE_SCHEMA)
    
    # Insert test data
    for sql in get_ecommerce_test_data():
        conn.execute(sql)
    
    return conn


def create_analytics_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with analytics schema."""
    conn = duckdb.connect(":memory:")
    
    # Create schema
    from .example_schemas import ANALYTICS_SCHEMA, get_analytics_test_data
    conn.execute(ANALYTICS_SCHEMA)
    
    # Insert test data
    for sql in get_analytics_test_data():
        conn.execute(sql)
    
    return conn


def create_financial_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with financial schema."""
    conn = duckdb.connect(":memory:")
    
    # Create schema
    from .example_schemas import FINANCIAL_SCHEMA, get_financial_test_data
    conn.execute(FINANCIAL_SCHEMA)
    
    # Insert test data
    for sql in get_financial_test_data():
        conn.execute(sql)
    
    return conn


def create_complex_analytics_database() -> duckdb.DuckDBPyConnection:
    """Create a test database with complex analytics schema for stress testing."""
    conn = duckdb.connect(":memory:")
    
    # Create a wide table similar to the problematic schema
    conn.execute("""
        CREATE TABLE wide_metrics (
            id INTEGER,
            timestamp TIMESTAMP,
            -- 50+ metric columns to test performance
            metric_001 DOUBLE, metric_002 DOUBLE, metric_003 DOUBLE, metric_004 DOUBLE, metric_005 DOUBLE,
            metric_006 DOUBLE, metric_007 DOUBLE, metric_008 DOUBLE, metric_009 DOUBLE, metric_010 DOUBLE,
            metric_011 DOUBLE, metric_012 DOUBLE, metric_013 DOUBLE, metric_014 DOUBLE, metric_015 DOUBLE,
            metric_016 DOUBLE, metric_017 DOUBLE, metric_018 DOUBLE, metric_019 DOUBLE, metric_020 DOUBLE,
            metric_021 DOUBLE, metric_022 DOUBLE, metric_023 DOUBLE, metric_024 DOUBLE, metric_025 DOUBLE,
            metric_026 DOUBLE, metric_027 DOUBLE, metric_028 DOUBLE, metric_029 DOUBLE, metric_030 DOUBLE,
            metric_031 DOUBLE, metric_032 DOUBLE, metric_033 DOUBLE, metric_034 DOUBLE, metric_035 DOUBLE,
            metric_036 DOUBLE, metric_037 DOUBLE, metric_038 DOUBLE, metric_039 DOUBLE, metric_040 DOUBLE,
            metric_041 DOUBLE, metric_042 DOUBLE, metric_043 DOUBLE, metric_044 DOUBLE, metric_045 DOUBLE,
            metric_046 DOUBLE, metric_047 DOUBLE, metric_048 DOUBLE, metric_049 DOUBLE, metric_050 DOUBLE,
            -- Dimension columns
            category VARCHAR,
            subcategory VARCHAR,
            region VARCHAR,
            -- JSON columns for flexibility
            properties JSON,
            metadata JSON,
            PRIMARY KEY (id, timestamp)
        )
    """)
    
    # Insert some test data
    conn.execute("""
        INSERT INTO wide_metrics
        SELECT
            row_number() OVER () as id,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (row_number() OVER ()) MINUTE as timestamp,
            -- Generate 50 metric values
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            random() * 100, random() * 100, random() * 100, random() * 100, random() * 100,
            -- Dimensions
            CASE (row_number() OVER ()) % 5
                WHEN 0 THEN 'Electronics'
                WHEN 1 THEN 'Clothing'
                WHEN 2 THEN 'Food'
                WHEN 3 THEN 'Books'
                ELSE 'Other'
            END as category,
            'Sub_' || ((row_number() OVER ()) % 10) as subcategory,
            CASE (row_number() OVER ()) % 4
                WHEN 0 THEN 'North America'
                WHEN 1 THEN 'Europe'
                WHEN 2 THEN 'Asia'
                ELSE 'Other'
            END as region,
            json_object('source', 'sensor_' || ((row_number() OVER ()) % 20)) as properties,
            json_object('version', '1.0', 'processed', true) as metadata
        FROM generate_series(1, 100)
    """)
    
    return conn