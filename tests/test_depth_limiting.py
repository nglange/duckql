"""Tests for query depth limiting."""

import pytest
import duckdb
from graphql import GraphQLError

from duckql import DuckQL


def create_nested_database():
    """Create a database with nested relationships for testing."""
    conn = duckdb.connect(":memory:")
    
    # Create tables with relationships
    conn.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR
        )
    """)
    
    conn.execute("""
        CREATE TABLE posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title VARCHAR,
            content TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER,
            user_id INTEGER,
            text TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE likes (
            id INTEGER PRIMARY KEY,
            comment_id INTEGER,
            user_id INTEGER
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO users VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com')
    """)
    
    conn.execute("""
        INSERT INTO posts VALUES
        (1, 1, 'First Post', 'Hello World'),
        (2, 1, 'Second Post', 'GraphQL is great')
    """)
    
    conn.execute("""
        INSERT INTO comments VALUES
        (1, 1, 2, 'Nice post!'),
        (2, 1, 1, 'Thanks!')
    """)
    
    conn.execute("""
        INSERT INTO likes VALUES
        (1, 1, 1),
        (2, 2, 2)
    """)
    
    return conn



class TestDuckQLDepthLimiting:
    """Test depth limiting integration with DuckQL."""
    
    @pytest.fixture
    def db_connection(self):
        """Create test database."""
        return create_nested_database()
    
    @pytest.mark.asyncio
    async def test_query_within_depth_limit(self, db_connection):
        """Test that queries within depth limit work fine."""
        server = DuckQL(db_connection, max_query_depth=3)
        schema = server.get_schema()
        
        # Depth 2 query - should work
        query = """
        query {
            users {
                name
                email
            }
        }
        """
        
        result = await schema.execute(query)
        assert not result.errors
        assert len(result.data['users']) == 2
    
    @pytest.mark.asyncio
    async def test_query_exceeds_depth_limit(self, db_connection):
        """Test that queries exceeding depth limit are rejected."""
        server = DuckQL(db_connection, max_query_depth=2)
        schema = server.get_schema()
        
        # Depth 3 query - should fail
        query = """
        query {
            users {
                name
                posts {
                    title
                }
            }
        }
        """
        
        result = await schema.execute(query)
        assert result.errors
        assert len(result.errors) == 1
        assert "exceeds maximum allowed depth" in result.errors[0].message
    
    @pytest.mark.asyncio
    async def test_no_depth_limit_by_default(self, db_connection):
        """Test that depth is unlimited by default."""
        server = DuckQL(db_connection)  # No max_query_depth
        schema = server.get_schema()
        
        # Very deep query - should work
        query = """
        query {
            users {
                name
                posts {
                    title
                    comments {
                        text
                        likes {
                            id
                        }
                    }
                }
            }
        }
        """
        
        result = await schema.execute(query)
        # This will fail because we don't have actual relationships,
        # but it shouldn't fail due to depth
        if result.errors:
            assert not any("depth" in str(e).lower() for e in result.errors)
    
    @pytest.mark.asyncio
    async def test_depth_limit_with_aggregations(self, db_connection):
        """Test that aggregation queries respect depth limits."""
        server = DuckQL(db_connection, max_query_depth=2)
        schema = server.get_schema()
        
        # Aggregation query with depth 2 - should work
        query = """
        query {
            postsAggregate {
                user_id
                _count
            }
        }
        """
        
        result = await schema.execute(query)
        # Aggregate queries should work within depth limit
        assert not any("depth" in str(e).lower() for e in (result.errors or []))
    
    @pytest.mark.asyncio
    async def test_depth_limit_with_aliases(self, db_connection):
        """Test that aliases don't affect depth calculation."""
        server = DuckQL(db_connection, max_query_depth=2)
        schema = server.get_schema()
        
        # Query with aliases - depth should be same
        query = """
        query {
            allUsers: users {
                fullName: name
                posts {
                    postTitle: title
                }
            }
        }
        """
        
        result = await schema.execute(query)
        assert result.errors
        assert any("exceeds maximum allowed depth" in str(e) for e in result.errors)
    
    def test_create_depth_limit_extension(self):
        """Test the depth limit extension factory function."""
        from duckql.validation import create_depth_limit_extension
        
        ExtClass = create_depth_limit_extension(5)
        
        # Should create a class, not instance
        assert isinstance(ExtClass, type)
        
        # Instance should have correct max_depth
        instance = ExtClass()
        assert instance.max_depth == 5
    
    @pytest.mark.asyncio
    async def test_error_message_includes_depths(self, db_connection):
        """Test that error messages include actual and max depth."""
        server = DuckQL(db_connection, max_query_depth=2)
        schema = server.get_schema()
        
        query = """
        query {
            users {
                posts {
                    comments {
                        text
                    }
                }
            }
        }
        """
        
        result = await schema.execute(query)
        assert result.errors
        error_message = result.errors[0].message
        assert "4" in error_message  # actual depth
        assert "2" in error_message  # max depth