import duckdb
from tinydb import TinyDB
import os


def convert_db(database) -> {}:
    # Query DuckDB to fetch relational data
    categories = database.execute("SELECT id, name, description FROM Categories").fetchall()
    products = database.execute("SELECT id, name, description, price, category_id FROM Products").fetchall()
    sales = database.execute("SELECT id, date FROM Sales").fetchall()
    sale_details = database.execute("SELECT sale_id, product_id, quantity FROM SaleDetails").fetchall()

    # Transform data into hierarchical JSON structure
    json = {"Categories": {}}

    # Build categories
    for category_id, category_name, category_description in categories:
        json["Categories"][category_name] = {}

    # Build products under each category
    for product_id, product_name, product_description, product_price, category_id in products:
        # Find category name for this product
        category_name = next(cat[1] for cat in categories if cat[0] == category_id)
        json["Categories"][category_name][product_name] = {
            "description": product_description,
            "price": product_price,
            "sales": {}
        }

    # Add sales to products, sorted by date
    for product_id, product_name, product_description, product_price, category_id in products:
        # Find the category name
        category_name = next(cat[1] for cat in categories if cat[0] == category_id)

        # Filter and sort sale details for this product
        product_sales = [
            (sale_id, quantity, next(sale[1] for sale in sales if sale[0] == sale_id))
            for sale_id, prod_id, quantity in sale_details
            if prod_id == product_id
        ]
        # Sort by sale date
        product_sales.sort(key=lambda x: x[2])

        # Add sorted sales to the product
        json["Categories"][category_name][product_name] = {
            "description": product_description,
            "price": product_price,
            "sales": {
                idx + 1: {  # Use an ascending integer index for keys
                    "ticket": sale_id,
                    "date": sale_date.strftime("%Y-%m-%d"),
                    "quantity": quantity,
                }
                for idx, (sale_id, quantity, sale_date) in enumerate(product_sales)
            },
        }

    return json


def main():
    # Paths
    duckdb_path = "data/duckdb_shop.db"
    tinydb_path = "data/tinydb_shop.json"

    # Reset TinyDB
    if os.path.exists(tinydb_path):
        os.remove(tinydb_path)

    # Connect to DuckDB
    con = duckdb.connect(duckdb_path)
    tinydb = TinyDB(tinydb_path)

    # Convert
    data = convert_db(con)

    # Insert into TinyDB
    tinydb.insert(data)

    # Close connections
    con.close()
    tinydb.close()


if __name__ == "__main__":
    main()
