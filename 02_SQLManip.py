import duckdb

if __name__ == "__main__":
    # Connect
    con = duckdb.connect("data/duckdb_shop.db")

    # Query to get total sales by category
    query = """
    SELECT
       SUM(sd.quantity) AS total_quantity
        , cat.name AS category
    FROM SaleDetails sd
    LEFT JOIN Products pr ON sd.product_id = pr.id
    LEFT JOIN Categories cat ON pr.category_id = cat.id
    GROUP BY category
    """

    results = con.sql(query).df()
    print(results)

    # Close
    con.close()