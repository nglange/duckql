"""Example usage of DuckQL with the Pidgin schema."""

import duckdb
from duckql import DuckQL

# Create an in-memory database with sample schema
def create_sample_database():
    conn = duckdb.connect(":memory:")
    
    # Create experiments table
    conn.execute("""
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now(),
            status TEXT DEFAULT 'created',
            config JSON,
            total_conversations INTEGER DEFAULT 0,
            completed_conversations INTEGER DEFAULT 0
        )
    """)
    
    # Create conversations table
    conn.execute("""
        CREATE TABLE conversations (
            conversation_id TEXT PRIMARY KEY,
            experiment_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now(),
            status TEXT DEFAULT 'created',
            agent_a_model TEXT,
            agent_b_model TEXT,
            total_turns INTEGER DEFAULT 0,
            final_convergence_score DOUBLE,
            duration_ms INTEGER
        )
    """)
    
    # Create turn_metrics table
    conn.execute("""
        CREATE TABLE turn_metrics (
            conversation_id TEXT,
            turn_number INTEGER,
            timestamp TIMESTAMP DEFAULT now(),
            convergence_score DOUBLE,
            vocabulary_overlap DOUBLE,
            message_a_length INTEGER,
            message_b_length INTEGER,
            word_frequencies_a JSON,
            word_frequencies_b JSON,
            PRIMARY KEY (conversation_id, turn_number)
        )
    """)
    
    # Insert sample data
    conn.execute("""
        INSERT INTO experiments VALUES 
        ('exp_001', 'GPT-4 vs Claude convergence', now() - INTERVAL '2 days', 'completed', 
         '{"max_turns": 50, "temperature": 0.7}', 10, 10),
        ('exp_002', 'Temperature effects study', now() - INTERVAL '1 day', 'running',
         '{"max_turns": 30, "temperature": 0.5}', 5, 3)
    """)
    
    conn.execute("""
        INSERT INTO conversations VALUES 
        ('conv_001', 'exp_001', now() - INTERVAL '2 days', 'completed', 
         'gpt-4', 'claude-3-opus', 45, 0.92, 120000),
        ('conv_002', 'exp_001', now() - INTERVAL '2 days', 'completed',
         'gpt-4', 'claude-3-opus', 42, 0.88, 115000),
        ('conv_003', 'exp_002', now() - INTERVAL '1 day', 'completed',
         'gpt-3.5-turbo', 'claude-3-haiku', 30, 0.75, 80000)
    """)
    
    conn.execute("""
        INSERT INTO turn_metrics VALUES
        ('conv_001', 1, now() - INTERVAL '2 days', 0.45, 0.3, 150, 145,
         '{"hello": 1, "world": 1}', '{"hello": 1, "there": 1}'),
        ('conv_001', 2, now() - INTERVAL '2 days', 0.52, 0.4, 165, 160,
         '{"hello": 2, "world": 1, "how": 1}', '{"hello": 2, "there": 1, "how": 1}'),
        ('conv_001', 45, now() - INTERVAL '2 days', 0.92, 0.85, 180, 175,
         '{"shared": 10, "vocabulary": 8}', '{"shared": 10, "vocabulary": 9}')
    """)
    
    return conn


def main():
    # Create sample database
    print("🦆 Creating sample DuckDB database...")
    conn = create_sample_database()
    
    # Create DuckQL server
    print("🚀 Initializing DuckQL server...")
    server = DuckQL(conn)
    
    # Add a computed field
    @server.computed_field("experiments", "progress_percentage")
    def progress_percentage(obj) -> float:
        total = obj.get("total_conversations", 0)
        completed = obj.get("completed_conversations", 0)
        return (completed / total * 100) if total > 0 else 0.0
    
    # Add a custom resolver
    @server.resolver("experiment_stats")
    async def experiment_stats(root, info, experiment_id: str) -> dict:
        sql = """
            SELECT 
                e.name,
                e.status,
                COUNT(DISTINCT c.conversation_id) as conversation_count,
                AVG(c.final_convergence_score) as avg_convergence,
                MAX(c.final_convergence_score) as max_convergence,
                MIN(c.final_convergence_score) as min_convergence
            FROM experiments e
            LEFT JOIN conversations c ON e.experiment_id = c.experiment_id
            WHERE e.experiment_id = $1
            GROUP BY e.experiment_id, e.name, e.status
        """
        result = await server.executor.execute_query(sql, {"p0": experiment_id})
        return result.rows[0] if result.rows else None
    
    print("\n📊 Available GraphQL queries:")
    print("  - experiments: List all experiments")
    print("  - conversations: List all conversations") 
    print("  - turnMetrics: List turn-by-turn metrics")
    print("  - experiment_stats(experiment_id): Get statistics for an experiment")
    
    print("\n🎯 Example GraphQL queries to try:")
    print("""
1. List experiments with progress:
   query {
     experiments {
       experiment_id
       name
       status
       progress_percentage
     }
   }

2. Find high-convergence conversations:
   query {
     conversations(
       where: { final_convergence_score: { gte: 0.85 } }
       orderBy: { final_convergence_score: DESC }
     ) {
       conversation_id
       experiment_id
       final_convergence_score
       total_turns
     }
   }

3. Get experiment statistics:
   query {
     experiment_stats(experiment_id: "exp_001") {
       name
       status
       conversation_count
       avg_convergence
       max_convergence
       min_convergence
     }
   }
    """)
    
    # Start server
    print("\n🚀 Starting GraphQL server...")
    server.serve(port=8000)


if __name__ == "__main__":
    main()