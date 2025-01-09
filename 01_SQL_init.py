import os
import random
import duckdb as db
from faker import Faker
from datetime import datetime

fake = Faker()

def sale_date() -> str:
    date = fake.date_between(datetime(2024, 1, 1), datetime(2024, 12, 31))
    return date.strftime("%Y-%m-%d")

def init_db(path: str):
    # Persistent DB
    con = db.connect(path)

    # Categories table
    con.execute("""
CREATE TABLE IF NOT EXISTS Categories (
  id INTEGER PRIMARY KEY,
  name VARCHAR NOT NULL,
  description VARCHAR
);
""")

    # Products table
    con.execute("""
CREATE TABLE IF NOT EXISTS Products (
  id INTEGER PRIMARY KEY, 
  name VARCHAR NOT NULL,
  description VARCHAR,
  price FLOAT,
  category_id INTEGER,
  FOREIGN KEY(category_id) REFERENCES Categories(id)
);
""")

    # Sales table
    con.execute("""
CREATE TABLE IF NOT EXISTS Sales (
  id INTEGER PRIMARY KEY,
  date DATE
);
""")

    # SaleDetails table
    con.execute("""
CREATE TABLE IF NOT EXISTS SaleDetails (
  id INTEGER PRIMARY KEY,
  sale_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  FOREIGN KEY(sale_id) REFERENCES Sales(id),
  FOREIGN KEY(product_id) REFERENCES Products(id)
);
""")

    # Close
    con.close()


def fill_db(path: str):
    con = db.connect(path)

    # Fill Categories
    con.execute("INSERT INTO Categories VALUES (1, 'Electronics', 'Electronic devices and gadgets');")
    con.execute("INSERT INTO Categories VALUES (2, 'Furniture', 'Furniture and home decor');")
    con.execute("INSERT INTO Categories VALUES (3, 'Books', 'Books and literature');")

    # Fill Products
    con.execute("INSERT INTO Products VALUES (1, 'Laptop', 'A high-end gaming laptop', 1500.00, 1);")
    con.execute("INSERT INTO Products VALUES (2, 'Mouse', 'A new-gen gaming mouse', 100.00, 1);")
    con.execute("INSERT INTO Products VALUES (3, 'Keyboard', 'Mechanical keyboard', 120.00, 1);")
    con.execute("INSERT INTO Products VALUES (4, 'Headphones', 'Noise-cancelling headphones', 150.00, 1);")
    con.execute("INSERT INTO Products VALUES (5, 'Chair', 'Ergonomic office chair', 200.00, 2);")
    con.execute("INSERT INTO Products VALUES (6, 'Desk', 'Wooden desk with drawers', 300.00, 2);")
    con.execute("INSERT INTO Products VALUES (7, 'Lamp', 'LED desk lamp', 50.00, 2);")
    con.execute("INSERT INTO Products VALUES (8, 'Novel', 'A best-selling novel', 15.00, 3);")
    con.execute("INSERT INTO Products VALUES (9, 'Magazine', 'Monthly fashion magazine', 5.00, 3);")
    con.execute("INSERT INTO Products VALUES (10, 'Data Cookbook', 'A comprehensive How-To Guide to deals on data with \
Python', 80.00, 3);")
    con.execute("INSERT INTO Products VALUES (11, 'Couch', 'Comfortable two-seat couch', 500.00, 2);")
    con.execute(
        "INSERT INTO Products VALUES (12, 'Filing Cabinet', 'Metal filing cabinet with three drawers', 120.00, 2);")
    con.execute("INSERT INTO Products VALUES (13, 'Whiteboard', 'Magnetic whiteboard with markers', 60.00, 2);")
    con.execute(
        "INSERT INTO Products VALUES (14, 'Science Magazine', 'Monthly scientific discoveries magazine', 7.00, 3);")
    con.execute(
        "INSERT INTO Products VALUES (15, 'Cooking Guide', 'Step-by-step cooking recipes from top chefs', 40.00, 3);")

    # Fill Sales
    sales_date = [sale_date() for _ in range(500)]
    sales_date.sort()
    for sale_id, date in enumerate(sales_date):
        con.execute(f"INSERT INTO Sales VALUES ({sale_id+1}, '{date}')")

    # Fill SaleDetails
    ticket_id = list(range(1,501)) + [random.randint(1,500) for _ in range(1500)]
    ticket_id.sort()

    for i, sale_id in enumerate(ticket_id):
        prod = random.randint(1, 15)
        quant = random.choices([_ for _ in range(1, 6)], weights=[5000, 1000, 100, 10, 1], k=1)[0]
        con.execute(f"INSERT INTO SaleDetails VALUES ({i}, {sale_id}, {prod}, {quant})")

    con.close()


def main():
    # Paths
    duckdb_path = "data/duckdb_shop.db"

    # Reset database
    if os.path.exists(duckdb_path):
        os.remove(duckdb_path)

    # Init and fill
    init_db(duckdb_path)
    fill_db(duckdb_path)


if __name__ == "__main__":
    main()
