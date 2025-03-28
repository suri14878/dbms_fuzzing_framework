from monitoring import ResourceMonitor
from fuzzing import PGFuzzer 


def main():
    # Initialize components
    fuzzer = PGFuzzer()
    monitor = ResourceMonitor()
    
    # Sample test queries
    queries = [
        "SELECT name FROM users WHERE age BETWEEN 20 AND 40",
        "SELECT * FROM employees WHERE salary > 50000",
        "SELECT id, name FROM users ORDER BY age DESC"
    ]
    
    # Run fuzzing session
    try:
        for query in queries:
            for _ in range(10):  # 10 iterations per query
                fuzzer.run_test(query)
        monitor.save_report()
    finally:
        fuzzer.pg.close()

if __name__ == "__main__":
    main()