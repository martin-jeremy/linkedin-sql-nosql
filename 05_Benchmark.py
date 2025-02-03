import time
import importlib
import statistics
from typing import List, Dict
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
from tinydb import TinyDB

# Global variables to store timing results and database connections
sql_times = {}
nosql_times = {}
sql_db = None
nosql_db = None

def initialize_databases(sql_path: str, nosql_path: str):
    """Initialize database connections."""
    global sql_db, nosql_db
    sql_db = duckdb.connect(sql_path)
    nosql_db = TinyDB(nosql_path)

def close_databases():
    """Close database connections."""
    global sql_db, nosql_db
    if sql_db:
        sql_db.close()
    if nosql_db:
        nosql_db.close()

def time_operation(func, *args) -> float:
    """Measure execution time of a function."""
    start_time = time.perf_counter()
    func(*args)
    end_time = time.perf_counter()
    return end_time - start_time

def time_sql_queries(num_runs: int) -> Dict[str, List[float]]:
    """Time each SQL query multiple times."""
    global sql_times

    sql_queries = {
        "Category Sales": """
            SELECT SUM(sd.quantity) AS total_quantity, cat.name AS category
            FROM SaleDetails sd
            LEFT JOIN Products pr ON sd.product_id = pr.id
            LEFT JOIN Categories cat ON pr.category_id = cat.id
            GROUP BY category
        """,
        "Product Prices": """
            SELECT pd.name AS name, pd.price AS unit_price,
                COALESCE(SUM(sd.quantity), 0) AS total_saled,
                pd.price * total_saled AS total_earned
            FROM Products pd
            LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
            GROUP BY name, pd.price
            ORDER BY total_earned DESC
        """,
        "Daily Sales": """
            SELECT cat.name AS category, pd.name AS name,
                pd.price AS unit_price, sd.quantity AS quantity,
                pd.price * sd.quantity AS total_price, sl.date AS date
            FROM Categories cat
            LEFT JOIN Products pd ON (cat.id = pd.category_id)
            LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
            LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
            WHERE sl.date = '2024-08-10'
        """,
        "Product Monthly": """
            SELECT pd.name, pd.price AS unit_price, sd.quantity,
                pd.price * sd.quantity AS total_price, sl.date
            FROM Products pd
            LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
            LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
            WHERE pd.name = 'Laptop' 
            AND sl.date >= '2024-08-01' 
            AND sl.date <= '2024-08-31'
        """
    }

    for query_name, query in sql_queries.items():
        times = []
        for _ in range(num_runs):
            duration = time_operation(sql_db.execute, query)
            times.append(duration)
        sql_times[query_name] = times

    return sql_times

def time_nosql_queries(num_runs: int) -> Dict[str, List[float]]:
    """Time each NoSQL operation multiple times."""
    global nosql_times

    # Import NoSQL manipulation module
    nosql_module = importlib.import_module("04_NoSQLManip")

    operations = {
        "Category Sales": nosql_module.get_total_sales_by_category,
        "Product Prices": nosql_module.get_total_price_by_product,
        "Daily Sales": lambda db: nosql_module.get_sales_by_date(db, '2024-08-10'),
        "Product Monthly": lambda db: nosql_module.get_product_sales_by_month(db, 'Laptop', 2024, 8)
    }

    for op_name, operation in operations.items():
        times = []
        for _ in range(num_runs):
            duration = time_operation(operation, nosql_db)
            times.append(duration)
        nosql_times[op_name] = times

    return nosql_times

def generate_statistics(num_runs: int) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Generate statistical summary of timing results."""
    stats = {"SQL": {}, "NoSQL": {}}

    for query_name in sql_times.keys():
        sql_query_times = sql_times[query_name]
        nosql_query_times = nosql_times.get(query_name, [])

        if sql_query_times and nosql_query_times:
            stats["SQL"][query_name] = {
                "mean": statistics.mean(sql_query_times),
                "median": statistics.median(sql_query_times),
                "std_dev": statistics.stdev(sql_query_times) if len(sql_query_times) > 1 else 0,
                "min": min(sql_query_times),
                "max": max(sql_query_times)
            }

            stats["NoSQL"][query_name] = {
                "mean": statistics.mean(nosql_query_times),
                "median": statistics.median(nosql_query_times),
                "std_dev": statistics.stdev(nosql_query_times) if len(nosql_query_times) > 1 else 0,
                "min": min(nosql_query_times),
                "max": max(nosql_query_times)
            }

    return stats

def plot_comparison(stats: Dict[str, Dict[str, Dict[str, float]]]):
    """Create visualization of performance comparison."""
    plt.figure(figsize=(12, 6))

    operations = list(stats["SQL"].keys())
    x = range(len(operations))
    width = 0.35

    sql_means = [stats["SQL"][op]["mean"] for op in operations]
    nosql_means = [stats["NoSQL"][op]["mean"] for op in operations]

    plt.bar([i - width / 2 for i in x], sql_means, width, label='SQL', color='skyblue')
    plt.bar([i + width / 2 for i in x], nosql_means, width, label='NoSQL', color='lightcoral')

    plt.errorbar([i - width / 2 for i in x], sql_means,
                 yerr=[stats["SQL"][op]["std_dev"] for op in operations],
                 fmt='none', ecolor='black', capsize=5)
    plt.errorbar([i + width / 2 for i in x], nosql_means,
                 yerr=[stats["NoSQL"][op]["std_dev"] for op in operations],
                 fmt='none', ecolor='black', capsize=5)

    plt.xlabel('Operation Type')
    plt.ylabel('Execution Time (seconds)')
    plt.title('SQL vs NoSQL Performance Comparison')
    plt.xticks(x, operations, rotation=45)
    plt.legend()
    plt.tight_layout()

    plt.savefig('performance_comparison.png')
    plt.close()

def print_statistics(stats: Dict[str, Dict[str, Dict[str, float]]]):
    """Print formatted statistics."""
    print("\nPerformance Statistics:")
    print("=" * 80)

    for operation in stats["SQL"].keys():
        print(f"\nOperation: {operation}")
        print("-" * 40)

        sql_stats = stats["SQL"][operation]
        nosql_stats = stats["NoSQL"][operation]

        print(f"SQL Implementation:")
        print(f"  Mean: {sql_stats['mean']:.6f} seconds")
        print(f"  Median: {sql_stats['median']:.6f} seconds")
        print(f"  Std Dev: {sql_stats['std_dev']:.6f} seconds")
        print(f"  Range: {sql_stats['min']:.6f} - {sql_stats['max']:.6f} seconds")

        print(f"\nNoSQL Implementation:")
        print(f"  Mean: {nosql_stats['mean']:.6f} seconds")
        print(f"  Median: {nosql_stats['median']:.6f} seconds")
        print(f"  Std Dev: {nosql_stats['std_dev']:.6f} seconds")
        print(f"  Range: {nosql_stats['min']:.6f} - {nosql_stats['max']:.6f} seconds")

        diff_percent = ((nosql_stats['mean'] - sql_stats['mean']) / sql_stats['mean']) * 100
        faster = "SQL" if diff_percent > 0 else "NoSQL"
        print(f"\nPerformance Difference: {abs(diff_percent):.2f}% faster with {faster}")

def main():
    print("Starting performance benchmark...")

    initialize_databases("data/duckdb_shop.db", "data/tinydb_shop.json")

    num_runs = 100
    time_sql_queries(num_runs)
    time_nosql_queries(num_runs)

    stats = generate_statistics(num_runs)
    print_statistics(stats)
    plot_comparison(stats)

    close_databases()
    print("\nBenchmark complete!")

if __name__ == "__main__":
    main()
