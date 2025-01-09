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

    results = con.sql(query)
    print(results)

    # Query to get total price for each items
    query = """
    SELECT
        pd.name AS name
        , pd.price AS unit_price
        , COALESCE( SUM(sd.quantity), 0 ) AS total_saled
        , pd.price * total_saled AS total_earned
    FROM Products pd
    LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
    GROUP BY name, pd.price
    ORDER BY total_earned DESC
    """

    results = con.sql(query)
    print(results)

    # Close
    con.close()