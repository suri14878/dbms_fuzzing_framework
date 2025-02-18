import os
import random
import logging
import sqlite3
import platform
import multiprocessing
import asyncio
import time
import csv
from time import sleep
from subprocess import Popen, PIPE
from typing import List, Tuple

# Try importing optional database modules
try:
    import psycopg2
except ImportError:
    psycopg2 = None
    logging.warning("psycopg2 module not found, PostgreSQL support disabled.")

try:
    import mysql.connector
except ImportError:
    mysql_connector = None
    logging.warning("mysql.connector module not found, MySQL support disabled.")

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DBFuzzer:
    def __init__(self, db_type: str, connection_params: dict):
        self.db_type = db_type.lower()
        self.conn = self._connect_db(connection_params)
        self.cursor = self.conn.cursor()
        logging.info(f"Connected to {self.db_type} database.")
        self._setup_test_db()

    def _connect_db(self, params: dict):
        if self.db_type == 'sqlite':
            return sqlite3.connect(params.get('database', ':memory:'))
        elif self.db_type == 'postgresql':
            if psycopg2 is None:
                raise ImportError("psycopg2 is not available. Install it to enable PostgreSQL support.")
            return psycopg2.connect(**params)
        elif self.db_type == 'mysql':
            if mysql_connector is None:
                raise ImportError("mysql.connector is not available. Install it to enable MySQL support.")
            return mysql.connector.connect(**params)
        else:
            raise ValueError("Unsupported database type.")
    
    def _setup_test_db(self):
        """Creates required test tables if they don't exist."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, 
                    age INTEGER
                )""")
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    salary INTEGER
                )""")
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error setting up test database: {e}")
    
    def execute_query(self, query: str) -> List[Tuple]:
        try:
            self.cursor.execute(query)
            if query.strip().lower().startswith("select"):
                return self.cursor.fetchall()
            self.conn.commit()
        except Exception as e:
            logging.error(f"Query Execution Error: {e}\nQuery: {query}")
        return []
    
    def close(self):
        self.cursor.close()
        self.conn.close()
        logging.info("Database connection closed.")

# Parallel Execution using Multiprocessing
async def execute_queries_parallel(queries: List[str], db_fuzzer):
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, db_fuzzer.execute_query, query) for query in queries]
    results = await asyncio.gather(*tasks)
    return results

# Query Mutator using Equivalent Expression Transformation
class QueryMutator:
    def __init__(self):
        self.transformations = [
            self.add_boolean_condition,
            self.replace_with_case_when,
            self.alter_predicates
        ]

    def mutate(self, query: str) -> str:
        chosen_transformation = random.choice(self.transformations)
        return chosen_transformation(query)
    
    def add_boolean_condition(self, query: str) -> str:
        return query.replace("WHERE", "WHERE (1=1) AND", 1) if "WHERE" in query else query
    
    def replace_with_case_when(self, query: str) -> str:
        if "WHERE" in query and "=" in query:
            query = query.replace("=", "= (CASE WHEN 1=1 THEN", 1) + " END)"
        return query
    
    def alter_predicates(self, query: str) -> str:
        return query.replace(">=", "!=", 1).replace("<=", "=", 1)

# OS-Level Monitoring and Logging
class SystemMonitor:
    def __init__(self, process_name: str):
        self.process_name = process_name
        self.log_file = "system_usage.csv"

    def monitor_cpu_memory(self):
        logging.info(f"Monitoring process: {self.process_name}")
        if platform.system() == "Windows":
            process = Popen(["tasklist"], stdout=PIPE)
        else:
            process = Popen(["ps", "-C", self.process_name, "-o", "%cpu,%mem"], stdout=PIPE)
        output, _ = process.communicate()
        logging.info(f"Process Stats: {output.decode('utf-8')}")
        
        with open(self.log_file, "a", newline='') as file:
            writer = csv.writer(file)
            writer.writerow([time.time(), output.decode('utf-8')])

# Docker-Based MySQL Container Setup
def start_mysql_container():
    os.system("docker run --name mysql_fuzzer -e MYSQL_ROOT_PASSWORD=root -d -p 3306:3306 mysql")

def stop_mysql_container():
    os.system("docker stop mysql_fuzzer && docker rm mysql_fuzzer")

# Main Fuzzing Framework
class DBFuzzingFramework:
    def __init__(self, db_type: str, connection_params: dict):
        self.db_fuzzer = DBFuzzer(db_type, connection_params)
        self.mutator = QueryMutator()
        self.monitor = SystemMonitor(db_type)
    
    def run(self, base_queries: List[str], iterations: int = 10):
        loop = asyncio.get_event_loop()
        for _ in range(iterations):
            queries = [self.mutator.mutate(random.choice(base_queries)) for _ in range(5)]
            loop.run_until_complete(execute_queries_parallel(queries, self.db_fuzzer))
            self.monitor.monitor_cpu_memory()
            sleep(1)
    
    def close(self):
        self.db_fuzzer.close()

# Example Usage
if __name__ == "__main__":
    start_mysql_container()
    connection_params = {"database": "test.db"}
    fuzz_framework = DBFuzzingFramework("sqlite", connection_params)
    test_queries = ["SELECT * FROM users WHERE age >= 30;", "SELECT name FROM employees WHERE salary <= 50000;"]
    fuzz_framework.run(test_queries, iterations=5)
    fuzz_framework.close()
    stop_mysql_container()
