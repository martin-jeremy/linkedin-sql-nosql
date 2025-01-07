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
  sale_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  sale_date DATE,
  FOREIGN KEY(product_id) REFERENCES Products(id)
);
""")

def main():
    print("Hello from linkedin-sql-nosql!")
    init_db("data/shop.db")


if __name__ == "__main__":
    main()
