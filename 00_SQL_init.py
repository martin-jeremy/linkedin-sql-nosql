import os
import duckdb as db


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

    # Fill Sales
    con.execute("INSERT INTO Sales VALUES (1, '2025-01-03');")
    con.execute("INSERT INTO Sales VALUES (2, '2025-01-03');")
    con.execute("INSERT INTO Sales VALUES (3, '2025-01-04');")
    con.execute("INSERT INTO Sales VALUES (4, '2025-01-06');")
    con.execute("INSERT INTO Sales VALUES (5, '2025-01-07');")

    # Fill SaleDetails
    con.execute("INSERT INTO SaleDetails VALUES (1, 1, 1, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (2, 1, 2, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (3, 1, 3, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (4, 2, 4, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (5, 3, 8, 3);")
    con.execute("INSERT INTO SaleDetails VALUES (6, 3, 9, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (7, 4, 6, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (8, 5, 5, 1);")
    con.execute("INSERT INTO SaleDetails VALUES (9, 5, 7, 1);")

    con.close()


def main():
    print("Hello from linkedin-sql-nosql!")
    if os.path.exists("data/duckdb_shop.db"):
        os.remove("data/duckdb_shop.db")
    init_db("data/duckdb_shop.db")
    fill_db("data/duckdb_shop.db")


if __name__ == "__main__":
    main()
