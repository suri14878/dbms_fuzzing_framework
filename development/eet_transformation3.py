import random
import psycopg2
import time
import psutil
from sqlglot import parse_one, exp
from sqlglot.expressions import Case, When, And, Or, Column, Literal, Paren

class PGQueryMutator:
    def __init__(self):
        self.columns = {
            "name": "string",
            "age": "number",
            "salary": "number"
        }
        self.bool_rules = [1, 2, 5, 6]
        self.non_bool_rules = [3, 4, 5, 6]
        print("[DEBUG] Mutator initialized with schema:", self.columns)

    def mutate(self, original_query):
        try:
            print("\n[DEBUG] Starting mutation process")
            parsed = parse_one(original_query, dialect="postgres")
            transformed = parsed.transform(self.apply_eet_rules)
            
            # Add explicit parentheses around complex expressions
            transformed = transformed.transform(self.parenthesize_complex)
            
            mutated_sql = transformed.sql(dialect="postgres", pretty=True)
            print("[DEBUG] Final mutated query:\n", mutated_sql)
            return mutated_sql
        except Exception as e:
            print(f"[ERROR] Mutation failed: {e}")
            return original_query

    def parenthesize_complex(self, node):
        """Add parentheses around complex expressions"""
        if isinstance(node, (exp.And, exp.Or, exp.Case, exp.Binary)):
            return Paren(this=node)
        return node

    def apply_eet_rules(self, node):
        if isinstance(node, (exp.Column, exp.Literal, exp.Between, exp.Binary, exp.Predicate)):
            if random.random() < 0.4:
                print(f"[TRANSFORM] Targeting node: {node.sql(dialect='postgres')}")
                return self.transform_node(node)
        return node

    def transform_node(self, node):
        if isinstance(node, exp.Predicate):
            print("[TRANSFORM] Boolean node detected")
            return self.transform_boolean(node)
        print("[TRANSFORM] Non-boolean node detected")
        return self.transform_non_boolean(node)

    def transform_boolean(self, node):
        rule = random.choice(self.bool_rules)
        p = self.random_bool_expr()
        print(f"[RULE {rule}] Applying to boolean expression: {node.sql(dialect='postgres')}")

        if rule == 1:
            new_node = Paren(
                this=exp.Or(
                    this=self.false_expr(p),
                    expression=Paren(this=node.copy())
                )
            )
        elif rule == 2:
            new_node = Paren(
                this=exp.And(
                    this=self.true_expr(p),
                    expression=Paren(this=node.copy())
                )
            )
        else:
            new_node = Paren(this=self.redundant_case(node))
        
        print(f"[RESULT] Transformed boolean: {new_node.sql(dialect='postgres')}")
        return new_node

    def transform_non_boolean(self, node):
        rule = random.choice(self.non_bool_rules)
        print(f"[RULE {rule}] Applying to non-boolean expression: {node.sql(dialect='postgres')}")
        
        if rule in (3, 4):
            transformed = Paren(this=self.fixed_case(node, rule))
        else:
            transformed = Paren(this=self.redundant_case(node))
        
        print(f"[RESULT] Transformed non-boolean: {transformed.sql(dialect='postgres')}")
        return transformed

    def fixed_case(self, node, rule):
        print(f"[CASE RULE {rule}] Creating fixed CASE structure")
        then_val = self.safe_value(node)
        else_val = self.safe_value(node)
        condition = Paren(
            this=self.false_expr(self.random_bool_expr()) if rule == 3 
            else self.true_expr(self.random_bool_expr())
        )

        return Case(
            ifs=[When(
                this=condition,
                expression=Paren(this=then_val) if rule == 3 else Paren(this=node.copy())
            )],
            default=Paren(this=node.copy()) if rule == 3 else Paren(this=else_val)
        )

    def redundant_case(self, node):
        print("[CASE] Creating redundant CASE structure")
        return Case(
            ifs=[When(
                this=Paren(this=self.random_bool_expr()),
                expression=Paren(this=node.copy())
            )],
            default=Paren(this=node.copy())
        )

    def random_bool_expr(self):
        col_name = random.choice(list(self.columns.keys()))
        col_type = self.columns[col_name]
        col = Column(this=col_name)
        
        if col_type == "number":
            val = Literal.number(random.randint(1, 100))
            comparisons = [exp.EQ, exp.NEQ, exp.GT, exp.LT]
        else:
            val = Literal.string(f"val_{random.randint(100, 999)}")
            comparisons = [exp.EQ, exp.NEQ, exp.Like]

        comparison = random.choice(comparisons)
        expr = comparison(this=col.copy(), expression=val.copy())
        print(f"[BOOL] Generated comparison: {expr.sql(dialect='postgres')}")
        return Paren(this=expr)

    def safe_value(self, original_node):
        if isinstance(original_node, exp.Column):
            col_name = original_node.name.lower()
            col_type = self.columns.get(col_name, "string")
            
            if col_type == "number":
                if col_name == "age":
                    val = random.randint(20, 70)
                else:
                    val = random.randint(30000, 150000)
                return Literal.number(val)
            return Literal.string(f"dummy_{random.randint(100, 999)}")
            
        if isinstance(original_node, exp.Literal):
            if original_node.is_string:
                return Literal.string(f"val_{random.randint(100, 999)}")
            return Literal.number(random.randint(1, 100))
            
        return Literal.string("default_value")

    def true_expr(self, p):
        expr = Paren(
            this=exp.Or(
                this=p.copy(),
                expression=exp.Or(
                    this=exp.Not(this=p.copy()),
                    expression=exp.Is(this=p.copy(), expression=exp.Null())
                )
            )
        )
        print(f"[TRUE EXPR] Generated: {expr.sql(dialect='postgres')}")
        return expr

    def false_expr(self, p):
        expr = Paren(
            this=exp.And(
                this=p.copy(),
                expression=exp.And(
                    this=exp.Not(this=p.copy()),
                    expression=exp.Is(this=p.copy(), expression=exp.Not(this=exp.Null()))
                )
            )
        )
        print(f"[FALSE EXPR] Generated: {expr.sql(dialect='postgres')}")
        return expr

class DBFuzzer:
    def __init__(self, db_config):
        print("\n[INIT] Initializing DBFuzzer")
        self.db_config = db_config
        self.mutator = PGQueryMutator()
        try:
            self.conn = psycopg2.connect(**db_config)
            self.conn.autocommit = False
            print("[INIT] Database connection successful")
        except Exception as e:
            print(f"[INIT] Connection failed: {e}")
            raise

    def execute_query(self, query):
        print("\n[QUERY] Executing:", query)
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall() if cur.description else []
                self.conn.commit()
                print("[QUERY] Execution successful. Results:", result)
                return result
        except Exception as e:
            self.conn.rollback()
            print(f"[QUERY ERROR] Failed execution: {e}")
            raise

    def fuzz(self, query, iterations=2):
        print(f"\n[FUZZ] Starting fuzzing with {iterations} iterations")
        for i in range(iterations):
            print(f"\n--- Iteration {i+1} ---")
            mutated = self.mutator.mutate(query)
            
            try:
                print("\n[TEST] Original query:", query)
                print("\n[TEST] Mutated query:", mutated)
                
                print("[TEST] Executing original...")
                original_result = self.execute_query(query)
                
                print("[TEST] Executing mutated...")
                mutated_result = self.execute_query(mutated)
                
                if original_result != mutated_result:
                    self.report_bug(query, mutated, original_result, mutated_result)
                else:
                    print("\n[RESULT] Results match")
                    
            except Exception as e:
                print(f"\n[ERROR] Iteration failed: {str(e)}")
            
            self.log_performance()

    def report_bug(self, original, mutated, res1, res2):
        print("\n!!! BUG DETECTED !!!")
        print("Original result:", res1)
        print("Mutated result:", res2)
        with open("bugs.log", "a") as f:
            f.write(f"{time.ctime()}\nORIGINAL:\n{original}\nMUTATED:\n{mutated}\n"
                    f"RESULTS: {res1} vs {res2}\n\n")

    def log_performance(self):
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        print(f"\n[PERF] CPU: {cpu}% MEM: {mem}%")
        with open("performance.log", "a") as f:
            f.write(f"[{time.ctime()}] CPU: {cpu}% MEM: {mem}%\n")

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
      AND salary > 50000
    """

    try:
        print("Starting fuzzer...")
        fuzzer = DBFuzzer(db_config)
        fuzzer.fuzz(test_query, iterations=2)
        print("\nFuzzing completed")
    except Exception as e:
        print(f"Fatal error: {e}")