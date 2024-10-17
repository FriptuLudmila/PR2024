import requests
from bs4 import BeautifulSoup
import re
from functools import reduce
from datetime import datetime, timezone

# Base URL of the website to form absolute URLs for products
base_url = "https://makeup.md"

response = requests.get("https://makeup.md/categorys/23467/")
html_content = response.text

# Parse the HTML content
soup = BeautifulSoup(html_content, 'lxml')

# Find all product containers
products = soup.find_all('div', class_='info-product-wrapper')  # Adjust the class name if necessary

# Initialize a list to store validated product data
validated_products = []

for product in products:
    # Extract the product name
    name_tag = product.find('a', class_='simple-slider-list__name')
    product_name = name_tag.text.strip() if name_tag else None

    # Extract the product price
    price_tag = product.find('span', class_='price_item')
    product_price = price_tag.text.strip() if price_tag else None

    # Extract the product link
    if name_tag and 'href' in name_tag.attrs:
        product_link = base_url + name_tag['href']
    else:
        product_link = None

    # Validation 1: Mandatory Field Validation
    if not all([product_name, product_price, product_link]):
        print("Skipping product due to missing essential data.")
        continue  # Skip this product and move to the next one

    # Validation 2: Data Type and Format Validation

    # Validate product price to ensure it's a valid number
    # Remove currency symbols and commas
    price_cleaned = re.sub(r'[^\d.,]', '', product_price).replace(',', '.')
    try:
        price_value = float(price_cleaned)
    except ValueError:
        print(f"Invalid price format for product '{product_name}': {product_price}")
        continue  # Skip this product

    # Validate product link format
    if not re.match(r'^https?://', product_link):
        print(f"Invalid URL format for product '{product_name}': {product_link}")
        continue  # Skip this product

    # Proceed only if the product link is available
    # Fetch the product page content
    product_response = requests.get(product_link)
    product_html = product_response.text

    # Parse the product page HTML
    product_soup = BeautifulSoup(product_html, 'lxml')

    # Extract product description
    description_tag = product_soup.find('li', class_='product-info__description')  # Adjust class name
    product_description = description_tag.text.strip() if description_tag else 'N/A'

    # Store the validated and extracted data
    validated_products.append({
        'name': product_name,
        'price': price_value,
        'link': product_link,
        'description': product_description
    })


# Processing the List with Map/Filter/Reduce Functions

# Exchange rate from MDL to EUR (1 EUR = 20 MDL)
exchange_rate = 20.0


# Map the prices to EUR
def map_to_eur(product):
    product_in_eur = product.copy()
    product_in_eur['price_eur'] = round(product['price'] / exchange_rate, 2)
    return product_in_eur


products_in_eur = list(map(map_to_eur, validated_products))

# Filter products within a specific price range
min_price = 5.0
max_price = 15.0


def filter_by_price(product):
    return min_price <= product['price_eur'] <= max_price


filtered_products = list(filter(filter_by_price, products_in_eur))


# Use reduce to sum up the prices of the filtered products
def sum_prices(total, product):
    return total + product['price_eur']


total_price = reduce(sum_prices, filtered_products, 0)

# Attach the sum and a UTC timestamp to the new data structure

processed_data = {
    'timestamp_utc': datetime.now(timezone.utc).isoformat(),
    'total_price_eur': round(total_price, 2),
    'products': filtered_products
}

# Print the Processed Data
print("\nProcessed Data:")
print(f"Timestamp (UTC): {processed_data['timestamp_utc']}")
print(f"Total Price (EUR): €{processed_data['total_price_eur']}")
print("Filtered Products:")

for product in processed_data['products']:
    print(f" - {product['name']} | Price: €{product['price_eur']} | Link: {product['link']}")
