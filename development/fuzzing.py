from database import PostgresManager
from eet_transformation import PGQueryMutator

class PGFuzzer:
    def __init__(self):
        self.pg = PostgresManager()
        self.mutator = PGQueryMutator()
        self.results = []

    def _insert_test_data(self):
        """Seed with consistent test data"""
        self.pg.execute_query("TRUNCATE users, employees RESTART IDENTITY")
        self.pg.execute_query("""
            INSERT INTO users (name, age) VALUES
            ('Alice', 25),   -- Exact boundary for BETWEEN
            ('Bob', 30),     -- Mid-range
            ('Charlie', 35)  -- Outside typical range
        """)
        self.pg.execute_query("""
            INSERT INTO employees (name, salary) VALUES
            ('Dave', 50000),  -- Exact boundary for salary comparisons
            ('Eve', 50001),   -- Just above threshold
            ('Frank', 49999)   -- Just below threshold
        """)

    def _normalize_results(self, results):
        """Sort results to handle ordering differences"""
        return sorted(results) if results else None

    # def run_test(self, original_query):
    #     self._insert_test_data()
    #     mutated_query = self.mutator.mutate(original_query)
        
    #     original_result = self._normalize_results(self.pg.execute_query(original_query))
    #     mutated_result = self._normalize_results(self.pg.execute_query(mutated_query))
        
    #     if original_result != mutated_result:
    #         self.results.append({
    #             "original": (original_query, original_result),
    #             "mutated": (mutated_query, mutated_result)
    #         })
    #         print("⚠️ Result mismatch found!")


    def run_test(self, original_query):
        self._insert_test_data()
        mutated_query = self.mutator.mutate(original_query)
    
        print(f"\n=== Test Run ===")
        print(f"Original: {original_query}")
        print(f"Mutated: {mutated_query}")
    
        original_result = self._normalize_results(self.pg.execute_query(original_query))
        mutated_result = self._normalize_results(self.pg.execute_query(mutated_query))

        print(f"Original Result: {original_result}")
        print(f"Mutated Result: {mutated_result}")

        if original_result != mutated_result:
            self.results.append({
                "original": (original_query, original_result),
                "mutated": (mutated_query, mutated_result)
            })
            print("⚠️ Result mismatch found!")
        else:
            print("✅ Results match")

# Test this phase
print("\nTesting query comparison...")
fuzzer = PGFuzzer()
test_query = "SELECT * FROM users WHERE age >= 25 AND age <= 35"
fuzzer.run_test(test_query)
print("Test results:", fuzzer.results)
fuzzer.pg.close()