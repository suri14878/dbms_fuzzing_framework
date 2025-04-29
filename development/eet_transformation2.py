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
        if random.random() > 0.7:
            return node  # Skip mutation most of the time to reach deeper nodes

        if isinstance(node, (exp.EQ, exp.GT, exp.LT, exp.And, exp.Or)):
            rule = random.choice([1, 2])
            print(f"[DEBUG] Applying EET rule {rule}")
            if rule == 1:
                return exp.Paren(
                    this=exp.Or(
                        this=exp.Paren(this=self._false_expr(self._rand_bool_expr())),
                        expression=node
                    )
                )
            elif rule == 2:
                return exp.Paren(
                    this=exp.And(
                        this=exp.Paren(this=self._true_expr(self._rand_bool_expr())),
                        expression=node
                    )
                )

        elif isinstance(node, exp.Between):
            rule = random.choice([3, 4])
            print(f"[DEBUG] Applying EET rule {rule}")

            low = node.args['low']
            high = node.args['high']

            if rule == 3:
                return exp.Between(
                    this=node.this,
                    low=exp.Case(
                        ifs=[exp.When(this=self._false_expr(self._rand_bool_expr()), expression=self._rand_simple_expr(low))],
                        default=low
                    ),
                    high=exp.Case(
                        ifs=[exp.When(this=self._false_expr(self._rand_bool_expr()), expression=self._rand_simple_expr(high))],
                        default=high
                    )
                )
            elif rule == 4:
                return exp.Between(
                    this=node.this,
                    low=exp.Case(
                        ifs=[exp.When(this=self._true_expr(self._rand_bool_expr()), expression=low)],
                        default=self._rand_simple_expr(low)
                    ),
                    high=exp.Case(
                        ifs=[exp.When(this=self._true_expr(self._rand_bool_expr()), expression=high)],
                        default=self._rand_simple_expr(high)
                    )
                )

        return node

    def _true_expr(self, p):
        return exp.And(this=p, expression=exp.And(this=p.copy().not_(), expression=exp.Is(this=p.copy(), expression=exp.Null())))

    def _false_expr(self, p):
        not_null_expr = exp.Not(this=exp.Is(this=p.copy(), expression=exp.Null()))
        return exp.And(this=p, expression=exp.And(this=p.copy().not_(), expression=not_null_expr))

    def _rand_bool_expr(self):
        return exp.EQ(this=exp.Literal.number(random.randint(0, 10)), expression=exp.Literal.number(random.randint(0, 10)))

    def _rand_simple_expr(self, node):
        if isinstance(node, exp.Column) or isinstance(node, exp.Literal):
            node_str = str(node).lower()
            if any(keyword in node_str for keyword in ['age', 'salary', 'id', 'count', 'price']):
                return exp.Literal.number(random.randint(1, 100))
            else:
                return exp.Literal.string('random_value')
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

    fuzzer = DBFuzzer(db_config)
    target_query = """SELECT
                    name,
                      age,
                      salary
                    FROM users
                    WHERE
                      age BETWEEN 20 AND 30
                      AND salary BETWEEN 40000 AND 60000
                      AND id BETWEEN 1 AND 100;"""
    fuzzer.fuzz(target_query, iterations=10)
