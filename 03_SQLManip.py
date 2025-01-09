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
    print("Get total sales by categories:")
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
    print("Get total prices for each items:")
    results = con.sql(query)
    print(results)

    # Query to get all sales of a given date
    query = """
    SELECT
      cat.name AS category
      , pd.name AS name
      , pd.price AS unit_price
      , sd.quantity AS quantity
      , pd.price * sd.quantity AS total_price
      , sl.date AS date
    FROM Categories cat
    LEFT JOIN Products pd ON (cat.id = pd.category_id)
    LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
    LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
    WHERE sl.date = '2024-08-10'
    ORDER BY category, total_price DESC
    """
    print("Get all sales for a given date: '2024-08-10'")
    results = con.sql(query)
    print(results)

    # Query to get all sales of a given product during August
    query = """
        SELECT
          pd.name AS name
          , pd.price AS unit_price
          , sd.quantity AS quantity
          , pd.price * sd.quantity AS total_price
          , sl.date AS date
        FROM Products pd
        LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
        LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
        WHERE pd.name = 'Laptop' AND sl.date >= '2024-08-01' AND sl.date <= '2024-08-31'
        ORDER BY date, total_price DESC
        """
    print("Get all sales of Laptop during '2024-08'")
    results = con.sql(query)
    print(results)

    # Query to count all sales for each product or categories
    query = """
    SELECT
      cat.name AS Category
      , pd.name AS Product
      , COALESCE(SUM(sd.quantity), 0) AS Quantity
    FROM Categories cat
    LEFT JOIN Products pd ON cat.id = pd.category_id
    LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
    GROUP BY ROLLUP (Category, Product)
    ORDER BY Category, Quantity DESC
    """
    print("Get the count of quantities sold for each product or categories (ROLLUP method):")
    results = con.sql(query)
    print(results)

    # Query to count all sales for each product or categories during the year
    query = """
        SELECT
          cat.name AS Type
          , COALESCE(SUM(sd.quantity), 0) AS Quantity
        FROM Categories cat
        LEFT JOIN Products pd ON cat.id = pd.category_id
        LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
        GROUP BY Type
        
        UNION
        
        SELECT
          pd.name AS Type
          , COALESCE(SUM(sd.quantity), 0) AS Quantity
        FROM Products pd
        LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
        GROUP BY Type
        ORDER BY Quantity DESC, Type
        """
    print("Get the count of quantities sold for each product or categories during the year (UNION method):")
    results = con.sql(query)
    print(results)

    # Close
    con.close()