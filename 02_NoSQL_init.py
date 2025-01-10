import duckdb
from tinydb import TinyDB
import os


def convert_to_relational_json(database) -> {}:
    # Query DuckDB to fetch relational data
    categories = database.execute("SELECT id, name, description FROM Categories").fetchdf()
    products = database.execute("SELECT id, name, description, price, category_id FROM Products").fetchdf()
    sales = database.execute("SELECT id, date FROM Sales").fetchdf()
    sale_details = database.execute("SELECT id, sale_id, product_id, quantity FROM SaleDetails").fetchdf()
    sales['date'] = sales['date'].apply(func=lambda str_date: str_date.strftime("%Y-%m-%d"))

    json = {"Categories": categories.set_index('id').to_dict(orient='index'),
            "Products": products.set_index('id').to_dict(orient='index'),
            "Sales": sales.set_index('id').to_dict(orient='index'),
            "SaleDetails": sale_details.set_index('id').to_dict(orient='index')}
    return json

def convert_to_hierarchical_json(database) -> {}:
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
    hierarchical_tinydb_path = "data/hierarchical_tinydb_shop.json"
    relational_tinydb_path = "data/relational_tinydb_shop.json"

    # Reset TinyDB
    if os.path.exists(hierarchical_tinydb_path):
        os.remove(hierarchical_tinydb_path)
    if os.path.exists(relational_tinydb_path):
        os.remove(relational_tinydb_path)

    # Connect to DuckDB
    con = duckdb.connect(duckdb_path)
    hierarchical_tinydb = TinyDB(hierarchical_tinydb_path)
    relational_tinydb = TinyDB(relational_tinydb_path)

    # Convert
    hierarchical_data = convert_to_hierarchical_json(con)
    relational_data = convert_to_relational_json(con)

    # Insert into TinyDB
    hierarchical_tinydb.insert(hierarchical_data)
    for table_name, records in relational_data.items():
        # Get or create the TinyDB table
        tmp_table = relational_tinydb.table(table_name)

        # Insert records into the table
        for record_id, record_value in records.items():
            # Insert with explicit id and unpack the rest of the fields
            tmp_table.insert({**record_value})

    # Close connections
    con.close()
    hierarchical_tinydb.close()
    relational_tinydb.close()


if __name__ == "__main__":
    main()
