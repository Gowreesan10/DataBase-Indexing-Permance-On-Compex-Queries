import time

import mysql.connector
import pandas as pd

import fake_data_inserter


class MySQLConnection:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print("Connected to MySQL successfully!")
            return self.connection
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            print(f"Query executed successfully: {query}")
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection to MySQL closed.")

    def createtables(self):
        # https://www3.ntu.edu.sg/home/ehchua/programming/sql/SampleDatabases.html
        try:
            sql_statements = [
                "DROP DATABASE IF EXISTS `trade`",
                "CREATE DATABASE IF NOT EXISTS `trade`",
                "USE `trade`",
                """
                CREATE TABLE countries (
                  country_code INT NOT NULL,
                  name VARCHAR(255) NOT NULL,
                  continent_name VARCHAR(255) NOT NULL,
                  PRIMARY KEY (country_code)
                );""",

                """
                CREATE TABLE users (
                  user_id INT NOT NULL AUTO_INCREMENT,
                  full_name VARCHAR(255) NOT NULL,
                  email VARCHAR(255) NOT NULL,
                  gender VARCHAR(255) NOT NULL,
                  date_of_birth VARCHAR(255) NOT NULL,
                  country_code INT NOT NULL,
                  PRIMARY KEY (user_id),
                  FOREIGN KEY (country_code) REFERENCES countries(country_code)
                );""",

                """
                CREATE TABLE merchants (
                  merchant_id INT NOT NULL AUTO_INCREMENT,
                  merchant_name VARCHAR(255) NOT NULL,
                  user_id INT NOT NULL,
                  country_code INT NOT NULL,
                  PRIMARY KEY (merchant_id),
                  FOREIGN KEY (user_id) REFERENCES users(user_id),
                  FOREIGN KEY (country_code) REFERENCES countries(country_code)
                );""",

                """CREATE TABLE orders (
                    order_id INT NOT NULL AUTO_INCREMENT,
                    user_id INT NOT NULL,
                    status VARCHAR(255) NOT NULL,
                    created_at VARCHAR(255) NOT NULL,
                    PRIMARY KEY (order_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );""",

                """
                CREATE TABLE products (
                  product_id INT NOT NULL AUTO_INCREMENT,
                  merchant_id INT NOT NULL,
                  name VARCHAR(255) NOT NULL,
                  price INT NOT NULL,
                  status VARCHAR(255) NOT NULL,
                  created_at VARCHAR(255) NOT NULL,
                  PRIMARY KEY (product_id),
                  FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
                );""",

                """CREATE TABLE order_items (
                  order_id INT NOT NULL,
                  product_id INT NOT NULL,
                  quantity INT NOT NULL,
                  PRIMARY KEY (order_id,product_id),
                  FOREIGN KEY (order_id) REFERENCES orders(order_id),
                  FOREIGN KEY (product_id) REFERENCES products(product_id)
                );""",
            ]

            for statement in sql_statements:
                self.execute_query(statement)

        except Exception as e:
            print(f"Error: {e}")

    def insert_data(self):
        fake_data_inserter.insert_fake(self.connection)

    def complex_query_runner(self, query):
        exe_time = []
        for x in range(10):
            cursor = self.connection.cursor()
            start_time = time.time()
            cursor.execute(query)
            results = cursor.fetchall()
            end_time = time.time()
            execution_time = end_time - start_time
            exe_time.append(execution_time)
            # print("Query Results:", results)
            # print(f"Execution Time: {execution_time:.8f} seconds")
            cursor.close()
        return sum(exe_time) / 10

    def complex_query_test(self, db):
        q1 = """SELECT full_name, email
                FROM users
                WHERE country_code IN (SELECT country_code FROM countries WHERE continent_name = 'North America');
                """

        q2 = """SELECT
                orders.order_id,
                users.full_name AS user_name,
                users.email AS user_email,
                products.name AS product_name,
                order_items.quantity,
                products.price,
                orders.status,
                orders.created_at
                FROM orders
                JOIN users ON orders.user_id = users.user_id
                JOIN order_items ON orders.order_id = order_items.order_id
                JOIN products ON order_items.product_id = products.product_id
                WHERE orders.status = 'Shipped';"""

        q3 = """SELECT
                merchants.merchant_id,
                merchants.merchant_name,
                SUM(products.price * order_items.quantity) AS total_revenue
                FROM merchants
                JOIN products ON merchants.merchant_id = products.merchant_id
                JOIN order_items ON products.product_id = order_items.product_id
                GROUP BY merchants.merchant_id, merchants.merchant_name;
            """

        q4 = """SELECT
                users.user_id,
                users.full_name,
                AVG(products.price * order_items.quantity) AS avg_order_value
                FROM users
                JOIN orders ON users.user_id = orders.user_id
                JOIN order_items ON orders.order_id = order_items.order_id
                JOIN products ON order_items.product_id = products.product_id
                WHERE orders.status != 'Pending'
                GROUP BY users.user_id, users.full_name;
            """

        exe_time_queries = []
        self.execute_query("USE `" + db + "`")
        exe_time_queries.append(self.complex_query_runner(q1))
        exe_time_queries.append(self.complex_query_runner(q2))
        exe_time_queries.append(self.complex_query_runner(q3))
        exe_time_queries.append(self.complex_query_runner(q4))
        return exe_time_queries

    def copy_database(self):
        statements = ["CREATE DATABASE IF NOT EXISTS `new_trade`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`countries` AS SELECT * FROM `trade`.`countries`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`users` AS SELECT * FROM `trade`.`users`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`merchants` AS SELECT * FROM `trade`.`merchants`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`orders` AS SELECT * FROM `trade`.`orders`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`products` AS SELECT * FROM `trade`.`products`",
                      "CREATE TABLE IF NOT EXISTS `new_trade`.`order_items` AS SELECT * FROM `trade`.`order_items`"]
        for statement in statements:
            self.execute_query(statement)

    def create_index(self):
        # index1 = "CREATE INDEX idx_users_country_code ON users (country_code)"
        # index2 = "CREATE INDEX idx_merchants_user_id ON merchants (user_id)"
        # index3 = "CREATE INDEX idx_merchants_country_code ON merchants (country_code)"
        # index4 = "CREATE INDEX idx_orders_user_id ON orders (user_id)"
        # index5 = "CREATE INDEX idx_products_merchant_id ON products (merchant_id)"
        # index6 = "CREATE INDEX idx_order_items_order_id_product_id ON order_items(order_id, product_id)"
        # index7 = "CREATE INDEX idx_products_merchant_id_product_id ON products(merchant_id, product_id)"
        # index8 = "CREATE INDEX idx_order_items_order_id ON order_items(order_id)"

        # -- Index for Query 1 (q1)
        index1 = "CREATE INDEX idx_users_country_code ON users(country_code)"
        index2 = "CREATE INDEX idx_countries_continent_name ON countries(continent_name)"

        # -- Index for Query 2 (q2)
        index3 = "CREATE INDEX idx_orders_user_id ON orders(user_id)"
        index4 = "CREATE INDEX idx_order_items_order_id ON order_items(order_id)"
        index5 = "CREATE INDEX idx_order_items_product_id ON order_items(product_id)"
        index6 = "CREATE INDEX idx_orders_status ON orders(status)"

        # -- Index for Query 3 (q3)
        index7 = "CREATE INDEX idx_products_merchant_id ON products (merchant_id)"
        # CREATE INDEX idx_order_items_product_id ON order_items(product_id);

        # -- Index for Query 4 (q4)
        #         CREATE INDEX idx_order_items_order_id ON order_items(order_id);
        #         CREATE INDEX idx_order_items_product_id ON order_items(product_id);
        #         CREATE INDEX idx_orders_status_ ON orders(status);

        self.execute_query("USE `new_trade`")
        self.execute_query(index1)
        self.execute_query(index2)
        self.execute_query(index3)
        # self.execute_query(index4)
        self.execute_query(index5)
        self.execute_query(index6)
        self.execute_query(index7)
        # self.execute_query(index8)

    def export_data(self):
        self.execute_query("USE `trade`")
        sql_queries = [
            "SELECT * FROM countries",
            "SELECT * FROM users",
            "SELECT * FROM merchants",
            "SELECT * FROM orders",
            "SELECT * FROM products",
            "SELECT * FROM order_items"
        ]

        for i, query in enumerate(sql_queries):
            table_name = query.split("FROM")[1].strip()
            df = pd.read_sql(query, self.connection)
            df.to_csv(f"{table_name}.csv", index=False)

    def insert_new(self):
        countries_data = [
            (1, 'United States', 'North America'),
            (2, 'United Kingdom', 'Europe'),
            (3, 'Canada', 'North America'),
            (4, 'Germany', 'Europe'),
            (5, 'Australia', 'Oceania'),
            (6, 'Japan', 'Asia'),
            (7, 'Brazil', 'South America'),
            (8, 'South Africa', 'Africa'),
            (9, 'France', 'Europe'),
            (10, 'China', 'Asia')
            # Add more entries as needed
        ]

        # Sample data for users table
        users_data = [
            (1, 'John Doe', 'john@example.com', 'Male', '1990-01-01', 1),
            (2, 'Jane Smith', 'jane@example.com', 'Female', '1995-05-15', 2),
            (3, 'Bob Johnson', 'bob@example.com', 'Male', '1988-08-20', 1),
            (4, 'Alice Brown', 'alice@example.com', 'Female', '1992-03-12', 3),
            (5, 'Carlos Rodriguez', 'carlos@example.com', 'Male', '1985-11-30', 7),
            (6, 'Mia Kim', 'mia@example.com', 'Female', '1998-07-25', 6),
            (7, 'Daniel Chen', 'daniel@example.com', 'Male', '1993-04-18', 5),
            (8, 'Sophie Müller', 'sophie@example.com', 'Female', '1991-09-05', 4),
            (9, 'David Lee', 'david@example.com', 'Male', '1994-06-08', 10),
            (10, 'Sophia Wang', 'sophia@example.com', 'Female', '1997-12-15', 10)

            # Add more entries as needed
        ]

        # Sample data for merchants table
        merchants_data = [
            (1, 'XYZ Mart', 1, 1),
            (2, 'ABC Store', 2, 2),
            (3, 'SuperGoods', 3, 4),
            (4, 'Epic Deals', 4, 3),
            (5, 'Tech Haven', 5, 5),
            (6, 'Global Mart', 6, 6),
            (7, 'Samba Shop', 7, 7),
            (8, 'African Treasures', 8, 8),
            (9, 'Fashion Trends', 9, 9),
            (10, 'Electro Haven', 10, 10)
            # Add more entries as needed
        ]

        # Sample data for orders table
        orders_data = [
            (1, 1, 'Shipped', '2023-01-05'),
            (2, 2, 'Pending', '2023-02-10'),
            (3, 3, 'Delivered', '2023-03-20'),
            (4, 4, 'Processing', '2023-04-15'),
            (5, 5, 'Shipped', '2023-05-02'),
            (6, 6, 'Pending', '2023-06-12'),
            (7, 7, 'Delivered', '2023-07-25'),
            (8, 8, 'Processing', '2023-08-18'),
            (9, 9, 'Shipped', '2023-09-02'),
            (10, 10, 'Pending', '2023-10-18')
            # Add more entries as needed
        ]

        # Sample data for products table
        products_data = [
            (1, 1, 'Product A', 50, 'Available', '2023-01-01'),
            (2, 2, 'Product B', 30, 'Out of Stock', '2023-02-01'),
            (3, 3, 'Gadget X', 100, 'Available', '2023-03-10'),
            (4, 4, 'Widget Y', 20, 'Available', '2023-04-05'),
            (5, 5, 'Smartphone Z', 500, 'Available', '2023-05-15'),
            (6, 6, 'Laptop Pro', 800, 'Out of Stock', '2023-06-20'),
            (7, 7, 'Samba Dance CD', 15, 'Available', '2023-07-02'),
            (8, 8, 'African Artisan Craft', 75, 'Available', '2023-08-10'),
            (9, 9, 'Stylish Shirt', 35, 'Available', '2023-09-10'),
            (10, 10, 'Smartwatch Pro', 200, 'Out of Stock', '2023-10-05')
            # Add more entries as needed
        ]

        # Sample data for order_items table
        order_items_data = [
            (1, 1, 2),
            (1, 2, 1),
            (2, 1, 3),
            (3, 3, 2),
            (4, 4, 1),
            (5, 5, 2),
            (6, 6, 1),
            (7, 7, 3),
            (8, 8, 1),
            (9, 9, 2),
            (9, 10, 1),
            (10, 9, 3)
            # Add more entries as needed
        ]

        insert_countries_query = "INSERT INTO trade.countries (country_code, name, continent_name) VALUES (%s, %s, %s)"
        insert_users_query = "INSERT INTO trade.users (user_id, full_name, email, gender, date_of_birth, country_code) VALUES (%s, %s, %s, %s, %s, %s)"
        insert_merchants_query = "INSERT INTO trade.merchants (merchant_id, merchant_name, user_id, country_code) VALUES (%s, %s, %s, %s)"
        insert_orders_query = "INSERT INTO trade.orders (order_id, user_id, status, created_at) VALUES (%s, %s, %s, %s)"
        insert_products_query = "INSERT INTO trade.products (product_id, merchant_id, name, price, status, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
        insert_order_items_query = "INSERT INTO trade.order_items (order_id, product_id, quantity) VALUES (%s, %s, %s)"

        cursor = self.connection.cursor()

        try:
            cursor.executemany(insert_countries_query, countries_data)
            self.connection.commit()
            print(f"Query executed successfully: {'countries'}")
            cursor.executemany(insert_users_query, users_data)
            self.connection.commit()
            print(f"Query executed successfully: {'users'}")
            cursor.executemany(insert_merchants_query, merchants_data)
            self.connection.commit()
            print(f"Query executed successfully: {'merchant'}")
            cursor.executemany(insert_orders_query, orders_data)
            self.connection.commit()
            print(f"Query executed successfully: {'countries'}")
            cursor.executemany(insert_products_query, products_data)
            self.connection.commit()
            print(f"Query executed successfully: {'product'}")
            cursor.executemany(insert_order_items_query, order_items_data)
            self.connection.commit()
            print(f"Query executed successfully: {'order_items'}")
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            cursor.close()

