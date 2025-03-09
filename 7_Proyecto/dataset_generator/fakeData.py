from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

# Faker is a library used to create fake data
fake = Faker()

def generate_data(num_records=1000, num_sales_df=1):
    data = []
    product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books']
    payment_methods = ['Credit Card', 'PayPal', 'Crypto', 'Bank Transfer']
    country_payment_weights = {
        'Belarus': [0.6, 0.2, 0.1, 0.1],  # Higher preference for Credit Card
        'Russia': [0.5, 0.3, 0.1, 0.1],  # Slightly more PayPal
        'Germany': [0.4, 0.4, 0.1, 0.1],  # More balanced
        'France': [0.3, 0.5, 0.1, 0.1],  # More PayPal
        'United Kingdom': [0.5, 0.3, 0.1, 0.1],  # Credit Card favored
        'Spain': [0.4, 0.4, 0.1, 0.1],  # Balanced
        'Italy': [0.4, 0.3, 0.2, 0.1]  # Higher Crypto
    }

    country_product_weights = {
        'Belarus': [0.4, 0.3, 0.2, 0.1, 0.1],  # Electronics more likely
        'Russia': [0.3, 0.3, 0.2, 0.1, 0.1],  # More balanced
        'Germany': [0.3, 0.2, 0.3, 0.1, 0.1],  # More Home & Kitchen
        'France': [0.2, 0.3, 0.3, 0.1, 0.1],  # Clothing and Home & Kitchen more likely
        'United Kingdom': [0.4, 0.3, 0.1, 0.1, 0.1],  # Electronics favored
        'Spain': [0.3, 0.3, 0.2, 0.1, 0.1],  # Balanced
        'Italy': [0.3, 0.3, 0.1, 0.2, 0.1]  # More Sports and Crypto
    }

    custom_countries = ['Belarus', 'Russia', 'Germany', 'France', 'United Kingdom', 'Spain', 'Italy']
    country_weights = [0.1, 0.2, 0.05, 0.1, 0.05, 0.3, 0.1]


    custom_countries = list(country_payment_weights.keys())  # Use keys from the dictionary

    # Generate product catalog data
    product_catalog = []
    product_id = 1
    for category in product_categories:
        for _ in range(10):  # 10 products per category
            product_catalog.append({
                "product_id": product_id,
                "product_name": fake.word().capitalize() + " " + fake.word().capitalize(),
                "product_category": category,
                "price": round(random.uniform(10.0, 300.0), 2)
            })
            product_id += 1
    product_catalog_df = pd.DataFrame(product_catalog)

    # Generate sales data and match it to product catalog
    sales_data = []
    for _ in range(num_sales_df):
        sales_df = []
        for _ in range(num_records):
            country = random.choice(custom_countries)
            payment_weights = country_payment_weights[country]
            product_weights = country_product_weights[country]

            category_index = random.choices(range(len(product_categories)), weights=product_weights, k=1)[0]
            category = product_categories[category_index]

            # Now select a product from the chosen category
            category_products = [product for product in product_catalog if product["product_category"] == category]
            product = random.choice(category_products)

            record = {
                "transaction_id": fake.uuid4(),
                "timestamp": fake.date_time_between(start_date="-30d", end_date="now"),
                "customer_id": fake.uuid4(),
                "product_id": product["product_id"],
                "product_category": product["product_category"],
                "product_name": product["product_name"],
                "price": product["price"],
                "payment_method": random.choices(payment_methods, weights=payment_weights, k=1)[0],
                "customer_country": country
            }
            sales_df.append(record)
        sales_data.append(pd.DataFrame(sales_df))

    return sales_data, product_catalog_df

# Example usage to create 3 sales_df datasets
num_sales_df = 5
sales_data_list, product_catalog_df = generate_data(25000, num_sales_df)

# Save to CSV files
for i, sales_df in enumerate(sales_data_list, start=1):
    sales_df.to_csv(f'sales_data_{i}.csv', index=False)
product_catalog_df.to_csv('product_catalog.csv', index=False)
