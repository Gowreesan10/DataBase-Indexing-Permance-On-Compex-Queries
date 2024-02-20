import random

from faker import Faker

# Create a Faker instance
fake = Faker()


# Function to generate random date of birth
def generate_date_of_birth():
    return fake.date_of_birth(minimum_age=18, maximum_age=65)


# Function to generate data for the countries table
def generate_countries_data(num_country):
    continent = ['North America', 'Europe', 'Oceania', 'Asia', 'South America', 'Africa', 'Europe']
    countries_data = []
    for i in range(1, num_country + 1):  # Assuming 5 countries
        country_data = (i, fake.country(), continent[random.randint(0, len(continent) - 1)],)
        countries_data.append(country_data)
    return countries_data


# Function to generate data for the users table
def generate_users_data(num_users, num_countries):
    users_data = []
    for i in range(1, num_users + 1):
        user_data = (
            i,
            fake.name(),
            fake.email(),
            fake.random_element(elements=('Male', 'Female')),
            generate_date_of_birth(),
            random.randint(1, num_countries),
        )
        users_data.append(user_data)
    return users_data


# Function to generate data for the merchants table
def generate_merchants_data(num_merchants, num_users, num_countries):
    merchants_data = []
    for i in range(1, num_merchants + 1):
        merchant_data = (
            i,
            fake.company(),
            random.randint(1, num_users),
            random.randint(1, num_countries),
        )
        merchants_data.append(merchant_data)
    return merchants_data


# Function to generate data for the orders table
def generate_orders_data(num_orders, num_users):
    orders_data = []
    for i in range(1, num_orders + 1):
        order_data = (
            i,
            random.randint(1, num_users),
            fake.random_element(elements=('Pending', 'Shipped', 'Delivered')),
            fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
        )
        orders_data.append(order_data)
    return orders_data


# Function to generate data for the products table
def generate_products_data(num_products, num_merchants):
    products_data = []
    for i in range(1, num_products + 1):
        product_data = (
            i,
            random.randint(1, num_merchants),
            fake.word(),
            random.randint(10, 100),
            fake.random_element(elements=('Available', 'Out of Stock')),
            fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S"),
        )
        products_data.append(product_data)
    return products_data


# Function to generate data for the order_items table
def generate_order_items_data(num_orders, num_products):
    order_items_data = []
    for i in range(1, num_orders + 1):
        for j in range(1, num_products + 1):
            order_item_data = (i, j, random.randint(1, 5))
            order_items_data.append(order_item_data)
    return order_items_data


def insert_fake(cnx):
    cursor = cnx.cursor()

    num_countries = 100
    num_users = 200
    num_merchants = 50
    num_orders = 25
    num_products = 60

    # Insert data into the countries table
    countries_data = generate_countries_data(num_countries)
    cursor.executemany("INSERT INTO countries VALUES (%s, %s, %s)", countries_data)
    cnx.commit()
    print("country succ")
    # Insert data into the users table
    users_data = generate_users_data(num_users=num_users, num_countries=num_countries)
    cursor.executemany("INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s)", users_data)
    cnx.commit()
    print("users succ")
    # Insert data into the merchants table
    merchants_data = generate_merchants_data(num_merchants=num_merchants, num_users=num_users,
                                             num_countries=num_countries)
    cursor.executemany("INSERT INTO merchants VALUES (%s, %s, %s, %s)", merchants_data)
    cnx.commit()
    print("merchants succ")
    # Insert data into the orders table
    orders_data = generate_orders_data(num_orders=num_orders, num_users=num_users)
    cursor.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s)", orders_data)
    cnx.commit()
    print("orders succ")
    # Insert data into the products table
    products_data = generate_products_data(num_products=num_products, num_merchants=num_merchants)
    cursor.executemany("INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s)", products_data)
    cnx.commit()
    print("product succ")
    # Insert data into the order_items table
    order_items_data = generate_order_items_data(num_orders=num_orders, num_products=num_products)
    cursor.executemany("INSERT INTO order_items VALUES (%s, %s, %s)", order_items_data)
    cnx.commit()
