from tinydb import TinyDB
from collections import defaultdict
from datetime import datetime


def print_separator(title: str):
    """Helper function to print formatted section titles"""
    print(f"\n{title}")
    print("-" * len(title))


def get_total_sales_by_category(db):
    """
    Equivalent to SQL:
    SELECT SUM(sd.quantity) AS total_quantity, cat.name AS category
    FROM SaleDetails sd
    LEFT JOIN Products pr ON sd.product_id = pr.id
    LEFT JOIN Categories cat ON pr.category_id = cat.id
    GROUP BY category
    """
    # Get the first (and only) document in our TinyDB
    data = db.all()[0]

    # Initialize counters for each category
    category_totals = defaultdict(int)

    # Iterate through the nested structure
    for category_name, products in data['Categories'].items():
        for product_name, product_data in products.items():
            # Sum up quantities from all sales of this product
            total_quantity = sum(sale['quantity'] for sale in product_data['sales'].values())
            category_totals[category_name] += total_quantity

    print_separator("Get total sales by categories:")
    for category, total in category_totals.items():
        print(f"Category: {category}, Total Quantity: {total}")


def get_total_price_by_product(db):
    """
    Equivalent to SQL:
    SELECT pd.name AS name, pd.price AS unit_price,
           COALESCE(SUM(sd.quantity), 0) AS total_saled,
           pd.price * total_saled AS total_earned
    FROM Products pd
    LEFT JOIN SaleDetails sd ON pd.id = sd.product_id
    GROUP BY name, pd.price
    ORDER BY total_earned DESC
    """
    data = db.all()[0]
    product_totals = []

    # Calculate totals for each product
    for category in data['Categories'].values():
        for product_name, product_data in category.items():
            total_quantity = sum(sale['quantity'] for sale in product_data['sales'].values())
            unit_price = product_data['price']
            total_earned = unit_price * total_quantity

            product_totals.append({
                'name': product_name,
                'unit_price': unit_price,
                'total_saled': total_quantity,
                'total_earned': total_earned
            })

    # Sort by total earned, descending
    product_totals.sort(key=lambda x: x['total_earned'], reverse=True)

    print_separator("Get total prices for each items:")
    for product in product_totals:
        print(f"Product: {product['name']}")
        print(f"  Unit Price: ${product['unit_price']:.2f}")
        print(f"  Total Sold: {product['total_saled']}")
        print(f"  Total Earned: ${product['total_earned']:.2f}")


def get_sales_by_date(db, target_date: str):
    """
    Equivalent to SQL:
    SELECT cat.name AS category, pd.name AS name, pd.price AS unit_price,
           sd.quantity AS quantity, pd.price * sd.quantity AS total_price,
           sl.date AS date
    FROM Categories cat
    LEFT JOIN Products pd ON (cat.id = pd.category_id)
    LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
    LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
    WHERE sl.date = target_date
    """
    data = db.all()[0]
    daily_sales = []

    # Collect all sales for the target date
    for category_name, products in data['Categories'].items():
        for product_name, product_data in products.items():
            for sale in product_data['sales'].values():
                if sale['date'] == target_date:
                    daily_sales.append({
                        'category': category_name,
                        'product': product_name,
                        'unit_price': product_data['price'],
                        'quantity': sale['quantity'],
                        'total_price': product_data['price'] * sale['quantity'],
                        'date': sale['date']
                    })

    # Sort by category and total price
    daily_sales.sort(key=lambda x: (x['category'], -x['total_price']))

    print_separator(f"Get all sales for date: {target_date}")
    for sale in daily_sales:
        print(f"Category: {sale['category']}")
        print(f"  Product: {sale['product']}")
        print(f"  Quantity: {sale['quantity']}")
        print(f"  Total Price: ${sale['total_price']:.2f}")


def get_product_sales_by_month(db, product_name: str, year: int, month: int):
    """
    Equivalent to SQL:
    SELECT pd.name, pd.price AS unit_price, sd.quantity,
           pd.price * sd.quantity AS total_price, sl.date
    FROM Products pd
    LEFT JOIN SaleDetails sd ON (pd.id = sd.product_id)
    LEFT JOIN Sales sl ON (sd.sale_id = sl.id)
    WHERE pd.name = product_name AND sl.date BETWEEN start_date AND end_date
    """
    data = db.all()[0]
    monthly_sales = []

    # Find the product in our nested structure
    for category in data['Categories'].values():
        if product_name in category:
            product_data = category[product_name]

            # Filter sales for the specified month
            for sale in product_data['sales'].values():
                sale_date = datetime.strptime(sale['date'], '%Y-%m-%d')
                if sale_date.year == year and sale_date.month == month:
                    monthly_sales.append({
                        'date': sale['date'],
                        'quantity': sale['quantity'],
                        'unit_price': product_data['price'],
                        'total_price': product_data['price'] * sale['quantity']
                    })

    # Sort by date
    monthly_sales.sort(key=lambda x: x['date'])

    print_separator(f"Get all sales of {product_name} for {year}-{month:02d}")
    for sale in monthly_sales:
        print(f"Date: {sale['date']}")
        print(f"  Quantity: {sale['quantity']}")
        print(f"  Total Price: ${sale['total_price']:.2f}")


def get_hierarchical_sales_summary(db):
    """
    Equivalent to SQL ROLLUP query:
    Shows total quantities sold for each product and category
    """
    data = db.all()[0]

    # Calculate totals for both categories and products
    category_totals = defaultdict(int)
    product_totals = defaultdict(int)

    for category_name, products in data['Categories'].items():
        for product_name, product_data in products.items():
            product_quantity = sum(sale['quantity'] for sale in product_data['sales'].values())
            product_totals[product_name] = product_quantity
            category_totals[category_name] += product_quantity

    print_separator("Get the count of quantities sold (hierarchical summary):")
    # Print category totals
    print("\nCategory Totals:")
    for category, total in category_totals.items():
        print(f"{category}: {total}")

    # Print product totals
    print("\nProduct Totals:")
    sorted_products = sorted(product_totals.items(), key=lambda x: (-x[1], x[0]))
    for product, total in sorted_products:
        print(f"{product}: {total}")


def main():
    # Connect to TinyDB
    db = TinyDB('data/tinydb_shop.json')

    # Run all our analysis functions
    get_total_sales_by_category(db)
    get_total_price_by_product(db)
    get_sales_by_date(db, '2024-08-10')
    get_product_sales_by_month(db, 'Laptop', 2024, 8)
    get_hierarchical_sales_summary(db)

    # Close the connection
    db.close()


if __name__ == "__main__":
    main()