import random
import psycopg2
import time
import psutil
from sqlglot import parse_one, exp

class PGQueryMutator:
    def __init__(self):
        self.transformations = [
            self._apply_eet_rule
        ]

    def mutate(self, original_query):
        try:
            print("[DEBUG] Parsing original query")
            parsed = parse_one(original_query, dialect="postgres")
            transformed = parsed.copy()
            original_sql = transformed.sql(dialect="postgres")
            for _ in range(5):
                transformation = random.choice(self.transformations)
                transformed = transformed.transform(transformation)
                if transformed.sql(dialect="postgres") != original_sql:
                    print("[DEBUG] Transformation applied")
                    break
            return transformed.sql(dialect="postgres", pretty=True)
        except Exception as e:
            print(f"Mutation error: {e}")
            return original_query

    def _apply_eet_rule(self, node):
        if isinstance(node, exp.Between):
            rule = 3
            print(f"[DEBUG] Applying EET rule {rule}")

            low = node.args['low']
            high = node.args['high']

            # Generate random values for THEN parts that will be visible in the query
            low_random_int = random.randint(1, 100)
            high_random_int = random.randint(101, 200)
            
            print(f"[DEBUG] Generated random low value: {low_random_int}")
            print(f"[DEBUG] Generated random high value: {high_random_int}")

            # Create a simplified version that uses string replacement for the final SQL
            # First create a basic BETWEEN transformation
            modified = exp.Between(
                this=node.this,
                low=low.copy(),
                high=high.copy()
            )
            
            # Get the SQL for the modified expression
            sql = modified.sql(dialect="postgres")
            
            # Replace the bounds with CASE expressions using direct string substitution
            # This ensures the random numbers appear in the query
            original_low_str = low.sql(dialect="postgres")
            original_high_str = high.sql(dialect="postgres")
            
            new_low_case = f"(CASE WHEN 1 = 2 THEN {low_random_int} ELSE {original_low_str} END)"
            new_high_case = f"(CASE WHEN 1 = 2 THEN {high_random_int} ELSE {original_high_str} END)"
            
            # Create a manual replacement
            sql = sql.replace(original_low_str, new_low_case)
            sql = sql.replace(original_high_str, new_high_case)
            
            print(f"[DEBUG] Original SQL: {modified.sql(dialect='postgres')}")
            print(f"[DEBUG] Modified SQL: {sql}")
            
            # Parse the manually constructed SQL back into SQLGlot
            try:
                return parse_one(sql, dialect="postgres")
            except Exception as e:
                print(f"[ERROR] Failed to parse modified SQL: {e}")
                # Fall back to simple string replacement approach
                return exp.SQL(this=sql)

        return node

    def _true_expr(self, p):
        return exp.And(this=p, expression=exp.And(this=p.copy().not_(), expression=exp.Is(this=p.copy(), expression=exp.Null())))

    def _false_expr(self, p):
        not_null_expr = exp.Not(this=exp.Is(this=p.copy(), expression=exp.Null()))
        return exp.And(this=p, expression=exp.And(this=p.copy().not_(), expression=not_null_expr))

    def _rand_bool_expr(self):
        return exp.EQ(this=exp.Literal.number(random.randint(0, 10)), expression=exp.Literal.number(random.randint(0, 10)))

    def _rand_simple_expr(self, node):
        try:
            if isinstance(node, exp.Column) or isinstance(node, exp.Literal):
                node_str = str(node).lower()
                if any(keyword in node_str for keyword in ['age', 'salary', 'id', 'count', 'price']):
                    return exp.Literal.number(random.randint(1, 100))
                else:
                    return exp.Literal.number(random.randint(1, 100))
        except Exception as e:
            print(f"[DEBUG] _rand_simple_expr fallback due to: {e}")
        return exp.Literal.number(random.randint(1, 100))

class DBFuzzer:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.mutator = PGQueryMutator()

    def get_execution_plan(self, query):
        with self.conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            plan = cur.fetchone()
            return plan

    def execute_query(self, query):
        with self.conn.cursor() as cur:
            try:
                print(f"[DEBUG] Executing query: {query}")
                cur.execute(query)
                result = cur.fetchall()
                return result
            except Exception as e:
                self.conn.rollback()
                print(f"[ERROR] Query execution failed: {e}")
                raise e

    def fuzz(self, query, iterations=10):
        print("Starting fuzzer...")
        for i in range(iterations):
            print(f"[DEBUG] Starting iteration {i+1}")
            mutated_query = self.mutator.mutate(query)
            try:
                original_result = self.execute_query(query)
                original_plan = self.get_execution_plan(query)
                mutated_result = self.execute_query(mutated_query)
                mutated_plan = self.get_execution_plan(mutated_query)

                if original_result != mutated_result:
                    self.report_bug(query, mutated_query, original_result, mutated_result, original_plan, mutated_plan)
                else:
                    print(f"[+] Iteration {i+1}: No inconsistency detected.")

            except Exception as e:
                print(f"Error executing query: {e}")

            self.log_system_performance()

    def report_bug(self, original_query, mutated_query, original_result, mutated_result, original_plan, mutated_plan):
        print("[!] Potential bug detected!")
        with open("bug_report.txt", "a") as f:
            f.write("\n==== BUG REPORT ====" + time.strftime("[%Y-%m-%d %H:%M:%S]") + "\n")
            f.write("Original Query:\n" + original_query + "\n")
            f.write("Mutated Query:\n" + mutated_query + "\n")
            f.write("Original Result:\n" + str(original_result) + "\n")
            f.write("Mutated Result:\n" + str(mutated_result) + "\n")
            f.write("Original Plan:\n" + str(original_plan) + "\n")
            f.write("Mutated Plan:\n" + str(mutated_plan) + "\n")
            f.write("=====================\n\n")

    def log_system_performance(self):
        cpu_usage = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()
        print(f"[DEBUG] CPU Usage: {cpu_usage}% Memory Usage: {memory_info.percent}%")
        with open("system_performance_log.txt", "a") as f:
            f.write(f"CPU: {cpu_usage}%, Memory: {memory_info.percent}% at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    db_config = {
        'dbname': 'postgresDB',
        'user': 'admin',
        'password': 'admin',
        'host': 'localhost',
        'port': 5432
    }

    test_query = """
    SELECT name, age, salary 
    FROM users 
    WHERE age BETWEEN 25 AND 60 
    """

    try:
        print("Starting fuzzer...")
        fuzzer = DBFuzzer(db_config)
        fuzzer.fuzz(test_query, iterations=2)
        print("\nFuzzing completed")
    except Exception as e:
        print(f"Fatal error: {e}")