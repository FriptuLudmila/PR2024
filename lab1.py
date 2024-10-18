import socket
import ssl
from bs4 import BeautifulSoup
import re
from functools import reduce
from datetime import datetime, timezone

# Base URL of the website to form absolute URLs for products
base_url = "https://makeup.md"


def get_http_response(host, port, request):
    # Create a socket connection
    context = ssl.create_default_context()
    with socket.create_connection((host, port)) as sock:
        with context.wrap_socket(sock, server_hostname=host) as ssock:
            ssock.sendall(request.encode())
            response = b""
            while True:
                data = ssock.recv(4096)
                if not data:
                    break
                response += data
    return response


def get_html_content(url):
    # Parse the URL to extract host and path
    from urllib.parse import urlparse
    parsed_url = urlparse(url)
    host = parsed_url.hostname
    port = 443  # HTTPS default port
    path = parsed_url.path or "/"
    if parsed_url.query:
        path += '?' + parsed_url.query

    # Create an HTTP GET request
    request = f"GET {path} HTTP/1.1\r\n" \
              f"Host: {host}\r\n" \
              f"Connection: close\r\n" \
              f"User-Agent: Mozilla/5.0\r\n" \
              f"Accept: text/html\r\n" \
              f"\r\n"

    # Get the raw HTTP response
    raw_response = get_http_response(host, port, request)

    # Decode the response to a string
    response_text = raw_response.decode('utf-8', errors='ignore')

    # Split headers and body
    headers, _, body = response_text.partition('\r\n\r\n')

    return body


# Get the main page content
html_content = get_html_content("https://makeup.md/categorys/23467/")

# Parse the HTML content
soup = BeautifulSoup(html_content, 'lxml')

# Find all product containers
products = soup.find_all('div', class_='info-product-wrapper')

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
    product_html = get_html_content(product_link)

    # Parse the product page HTML
    product_soup = BeautifulSoup(product_html, 'lxml')

    # Extract product description
    description_tag = product_soup.find('li', class_='product-info__description')
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


# Custom Serialization and Deserialization Functions

# Define a custom serialization format
# - Dictionaries are enclosed in { }
# - Lists are enclosed in [ ]
# - Key-value pairs are separated by :
# - Items in lists and dictionaries are separated by ,
# - Strings are enclosed in 'single quotes'

def custom_serialize(obj):
    if isinstance(obj, dict):
        items = []
        for k, v in obj.items():
            key = f"'{k}'"
            value = custom_serialize(v)
            items.append(f"{key}:{value}")
        return '(' + ';'.join(items) + ')'
    elif isinstance(obj, list):
        items = [custom_serialize(element) for element in obj]
        return '<' + ';'.join(items) + '>'
    elif isinstance(obj, str):
        # Escape single quotes in the string
        escaped_str = obj.replace("'", "\\'")
        return f"'{escaped_str}'"
    elif isinstance(obj, bool):
        return 'TRUE' if obj else 'FALSE'
    elif obj is None:
        return 'NONE'
    elif isinstance(obj, (int, float)):
        return str(obj)
    else:
        raise TypeError(f"Type {type(obj)} not serializable")


def custom_deserialize(s):
    def parse_value(index):
        if s[index] == '(':
            return parse_dict(index)
        elif s[index] == '<':
            return parse_list(index)
        elif s[index] == "'":
            return parse_string(index)
        elif s[index].isdigit() or s[index] == '-':
            return parse_number(index)
        elif s.startswith('TRUE', index):
            return True, index + 4
        elif s.startswith('FALSE', index):
            return False, index + 5
        elif s.startswith('NONE', index):
            return None, index + 4
        else:
            raise ValueError(f"Unexpected character at position {index}: {s[index]}")

    def parse_dict(index):
        obj = {}
        index += 1  # Skip '('
        while s[index] != ')':
            key, index = parse_string(index)
            index += 1  # Skip ':'
            value, index = parse_value(index)
            obj[key] = value
            if s[index] == ';':
                index += 1  # Skip ';'
        index += 1  # Skip ')'
        return obj, index

    def parse_list(index):
        lst = []
        index += 1  # Skip '<'
        while s[index] != '>':
            value, index = parse_value(index)
            lst.append(value)
            if s[index] == ';':
                index += 1  # Skip ';'
        index += 1  # Skip '>'
        return lst, index

    def parse_string(index):
        index += 1  # Skip opening "'"
        result = ''
        while s[index] != "'":
            if s[index] == '\\' and s[index + 1] == "'":
                result += "'"
                index += 2
            else:
                result += s[index]
                index += 1
        index += 1  # Skip closing "'"
        return result, index

    def parse_number(index):
        start = index
        while index < len(s) and (s[index].isdigit() or s[index] in '.-'):
            index += 1
        num_str = s[start:index]
        if '.' in num_str:
            return float(num_str), index
        else:
            return int(num_str), index

    value, index = parse_value(0)
    return value


# Serialize the processed data using the custom format
custom_serialized_data = custom_serialize(processed_data)

# Deserialize the custom serialized data back to a Python object
custom_deserialized_data = custom_deserialize(custom_serialized_data)

# Print the Processed Data
print("\nProcessed Data:")
print(f"Timestamp (UTC): {processed_data['timestamp_utc']}")
print(f"Total Price (EUR): €{processed_data['total_price_eur']}")
print("Filtered Products:")

for product in processed_data['products']:
    print(f" - {product['name']} | Price: €{product['price_eur']} | Link: {product['link']}")
    print(f"   Description: {product['description']}")

# Print the custom serialized data
print("\nCustom Serialized Data:")
print(custom_serialized_data)

# Verify that deserialization returns the original data
assert processed_data == custom_deserialized_data, "Deserialized data does not match the original!"

print("\nDeserialization successful. Data integrity verified.")
