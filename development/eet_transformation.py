from sqlglot import parse_one, exp, transpile
import random

class PGQueryMutator:
    def __init__(self):
        self.transformations = [
            self._swap_and_clauses,
            self._convert_between,
            self._reorder_projections,
            self._swap_operators
        ]
    
    def mutate(self, original_query):
        try:
            # Parse with PostgreSQL dialect
            parsed = parse_one(original_query, dialect="postgres")
            transformed = parsed.copy()
            
            # Apply transformations until modification
            original_sql = transformed.sql(dialect="postgres")
            for _ in range(5):
                transformation = random.choice(self.transformations)
                transformed = transformed.transform(transformation)
                if transformed.sql(dialect="postgres") != original_sql:
                    break
            
            # Convert to PostgreSQL syntax explicitly
            return transformed.sql(dialect="postgres", pretty=True)
        except Exception as e:
            print(f"Mutation error: {e}")
            return original_query

    #forcing known mutation
    def mutate(self, original_query):
        try:
            parsed = parse_one(original_query, dialect="postgres")
            
            # FOR TESTING: Always change >= to >
            transformed = parsed.transform(lambda node: (
                exp.GT(this=node.this, expression=node.expression)
                if isinstance(node, exp.GTE)
                else node
            ))
            
            return transformed.sql(dialect="postgres")
        except Exception as e:
            print(f"Mutation error: {e}")
            return original_query

    def _swap_and_clauses(self, node):
        if isinstance(node, exp.And):
            node.args["this"], node.args["expression"] = node.args["expression"], node.args["this"]
        return node

    def _convert_between(self, node):
        if isinstance(node, exp.Between):
            return parse_one(
                f"{node.this.sql(dialect='postgres')} >= {node.args['low'].sql(dialect='postgres')} "
                f"AND {node.this.sql(dialect='postgres')} <= {node.args['high'].sql(dialect='postgres')}",
                dialect="postgres"
            )
        return node

    def _reorder_projections(self, node):
        if isinstance(node, exp.Select) and len(node.expressions) > 1:
            new_order = random.sample(node.expressions, len(node.expressions))
            node.set("expressions", new_order)
        return node

    def _swap_operators(self, node):
        if isinstance(node, exp.GT):
            return exp.LT(this=node.this, expression=node.expression)
        elif isinstance(node, exp.LT):
            return exp.GT(this=node.this, expression=node.expression)
        return node
    

# # Test this phase
# print("\nTesting query mutations...")
# mutator = PGQueryMutator()
# sample_query = "SELECT name, age FROM users WHERE age BETWEEN 25 AND 30 AND salary > 50000"
# print("Original:", sample_query)

# mutated = mutator.mutate(sample_query)
# for _ in range(5):
#     mutated = mutator.mutate(sample_query)
#     print("Mutated:", mutated)
#     print("-" * 50)
