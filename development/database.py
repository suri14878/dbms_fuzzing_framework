import psycopg2
from psycopg2 import sql

class PostgresManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            user="admin",
            password="admin",
            host="localhost",
            port="5432",
            database="postgresDB"
        )
        self.conn.autocommit = False  # Use transactions
        self.cursor = self.conn.cursor()
        self._initialize_schema()

    def _initialize_schema(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                age INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                salary INTEGER
            )
        """)
        self.conn.commit()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            if query.strip().lower().startswith("select"):
                return self.cursor.fetchall()
            self.conn.commit()
            return None
        except Exception as e:
            self.conn.rollback()
            print(f"Query failed: {e}")
            return None

    def close(self):
        self.cursor.close()
        self.conn.close()

# Test this phase
print("\nTesting database connection...")
pg_manager = PostgresManager()
pg_manager.execute_query("INSERT INTO users (name, age) VALUES ('Test User', 30)")
results = pg_manager.execute_query("SELECT * FROM users")
print("Test query results:", results)
pg_manager.close()