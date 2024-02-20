import csv
import time

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class CassandraConnector:

    def __init__(self, contact_points: list[str], port: int = 9042):
        self.contact_points = contact_points
        self.port = port
        self._cluster = None
        self._session = None

    def connect(self):
        if self._cluster is None:
            self._cluster = Cluster(contact_points=self.contact_points, port=self.port)
        if self._session is None:
            self._session = self._cluster.connect()

    def disconnect(self):
        if self._session is not None:
            self._session.shutdown()
        if self._cluster is not None:
            self._cluster.shutdown()

    def execute(self, cql: str, **kwargs):
        self.connect()
        self._session.execute(cql, **kwargs)
        print(cql)

    def list_keyspaces(self):
        self.connect()
        if hasattr(self._session, "list_keyspaces"):
            return self._session.list_keyspaces()
        else:
            # For Cassandra versions < 3.24
            cql = "SELECT keyspace_name FROM system_schema.keyspaces;"
            results = self._session.execute(cql)
            return [row[0] for row in results]

    def create_keyspace(self, keyspace_name: str, replication_factor: int = 1):
        self.connect()
        cql = f"""CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {replication_factor} }}"""
        self._session.execute(cql)

    def execute_multi(self, cql_list: list[str], **kwargs):
        self.connect()
        for cql in cql_list:
            self._session.execute(cql, **kwargs)

    def drop_keyspace(self, keyspace_name: str):
        self.connect()
        cql = f"DROP KEYSPACE IF EXISTS {keyspace_name};"
        self._session.execute(cql)

    def create_tables(self):
        cql = [
            """
            CREATE TABLE IF NOT EXISTS countries (
              country_code int PRIMARY KEY,
              name text,
              continent_name text
            )""",
            """
            CREATE TABLE IF NOT EXISTS users (
              user_id int PRIMARY KEY,
              full_name text,
              email text,
              gender text,
              date_of_birth text,
              country_code int
            )""",
            """
            CREATE TABLE IF NOT EXISTS merchants (
              merchant_id int PRIMARY KEY,
              merchant_name text,
              user_id int,
              country_code int
            )""",
            """
            CREATE TABLE IF NOT EXISTS orders (
              order_id int PRIMARY KEY,
              user_id int,
              status text,
              created_at text
            )""",
            """
            CREATE TABLE IF NOT EXISTS products (
              product_id int PRIMARY KEY,
              merchant_id int,
              name text,
              price int,
              status text,
              created_at text
            )""",
            """
            CREATE TABLE IF NOT EXISTS order_items (
              order_id int,
              product_id int,
              quantity int,
              PRIMARY KEY ((order_id, product_id))
            )""",
        ]

        self.execute_multi(cql)

        #         cql = ["""CREATE TABLE IF NOT EXISTS countries (
        #           continent_name text,
        #           country_code int,
        #           name text,
        #           PRIMARY KEY (country_code,continent_name)
        #         );
        #         """,
        #                """CREATE TABLE IF NOT EXISTS users (
        #   country_code int,
        #   user_id int,
        #   full_name text,
        #   email text,
        #   gender text,
        #   date_of_birth text,
        #   PRIMARY KEY (user_id,country_code)
        # );
        # """,
        #                """CREATE TABLE IF NOT EXISTS merchants (
        #                  country_code int,
        #                  user_id int,
        #                  merchant_id int,
        #                  merchant_name text,
        #                  PRIMARY KEY ( merchant_id, user_id, country_code)
        #                );
        #                """,
        #                """CREATE TABLE IF NOT EXISTS orders (
        #   user_id int,
        #   order_id int,
        #   status text,
        #   created_at text,
        #   PRIMARY KEY (order_id,user_id)
        # );
        # """,
        #                """CREATE TABLE IF NOT EXISTS products (
        #   merchant_id int,
        #   product_id int,
        #   name text,
        #   price int,
        #   status text,
        #   created_at text,
        #   PRIMARY KEY (product_id,merchant_id)
        # );
        # """,
        #                """CREATE TABLE IF NOT EXISTS order_items (
        #   order_id int,
        #   product_id int,
        #   quantity int,
        #   PRIMARY KEY ((order_id, product_id))
        # );
        # """, ]

    def insert_data(self):
        csv_files = {
            'countries': 'countries.csv',
            'users': 'users.csv',
            'merchants': 'merchants.csv',
            'orders': 'orders.csv',
            'products': 'products.csv',
            'order_items': 'order_items.csv',
        }

        integer_columns = ['country_code', 'user_id', 'merchant_id', 'order_id', 'product_id', 'price', 'quantity']

        for table, csv_file in csv_files.items():
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert 'country_code' to integer
                    for col in integer_columns:
                        if col in row and row[col].isdigit():
                            row[col] = int(row[col])

                    # Generate a CQL INSERT statement for each row
                    columns = ', '.join(row.keys())
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

                    # Execute the INSERT statement
                    self._session.execute(query, list(row.values()))

            print("Data imported successfully from " + csv_file)

    def q1(self):
        result = []
        continent_name = 'North America'
        query = SimpleStatement("SELECT country_code FROM countries WHERE continent_name = %s ALLOW FILTERING")
        rows = self._session.execute(query, [continent_name])
        for row in rows:
            country_code = row.country_code
            query = SimpleStatement("SELECT full_name, email FROM users WHERE country_code = %s ALLOW FILTERING")
            user_rows = self._session.execute(query, [country_code])
            for user_row in user_rows:
                result.append(user_row)
        print('q1 success')

    def q2(self):
        query = SimpleStatement("SELECT * FROM orders WHERE status = 'Shipped' ALLOW FILTERING")
        orders = self._session.execute(query)
        result = []

        for order in orders:
            # Get user details for the order
            query = SimpleStatement("SELECT full_name, email FROM users WHERE user_id = %s")
            user = self._session.execute(query, [order.user_id]).one()

            # Get order items for the order
            query = SimpleStatement("SELECT * FROM order_items WHERE order_id = %s ALLOW FILTERING")
            order_items = self._session.execute(query, [order.order_id])

            for order_item in order_items:
                # Get product details for the order item
                query = SimpleStatement("SELECT name, price FROM products WHERE product_id = %s")
                product = self._session.execute(query, [order_item.product_id]).one()

                result.append({
                    'order_id': order.order_id,
                    'user_name': user.full_name,
                    'user_email': user.email,
                    'product_name': product.name,
                    'quantity': order_item.quantity,
                    'price': product.price,
                    'status': order.status,
                    'created_at': order.created_at
                })
        print('q2 success')

    def q3(self):
        query = SimpleStatement("SELECT * FROM merchants")
        merchants = self._session.execute(query)
        result = []

        for merchant in merchants:
            # Get all products offered by the merchant
            query = SimpleStatement("SELECT * FROM products WHERE merchant_id = %s ALLOW FILTERING")
            products = self._session.execute(query, [merchant.merchant_id])

            total_revenue = 0
            for product in products:
                # Get all order items for the product
                query = SimpleStatement("SELECT * FROM order_items WHERE product_id = %s ALLOW FILTERING")
                order_items = self._session.execute(query, [product.product_id])

                for order_item in order_items:
                    total_revenue += product.price * order_item.quantity

            result.append({
                'merchant_id': merchant.merchant_id,
                'merchant_name': merchant.merchant_name,
                'total_revenue': total_revenue
            })
        print('q3 success')

    def q4(self):
        query = SimpleStatement("SELECT * FROM users")
        users = self._session.execute(query)

        result = []
        for user in users:
            query = SimpleStatement("SELECT * FROM orders WHERE user_id = %s ALLOW FILTERING")
            orders = self._session.execute(query, [user.user_id])

            total_order_value = 0
            order_count = 0
            for order in orders:
                if order.status == 'Pending':
                    continue

                query = SimpleStatement("SELECT * FROM order_items WHERE order_id = %s ALLOW FILTERING")
                order_items = self._session.execute(query, [order.order_id])

                for order_item in order_items:
                    query = SimpleStatement("SELECT price FROM products WHERE product_id = %s")
                    product = self._session.execute(query, [order_item.product_id]).one()

                    total_order_value += product.price * order_item.quantity
                    order_count += 1

            avg_order_value = total_order_value / order_count if order_count > 0 else 0

            result.append({
                'user_id': user.user_id,
                'full_name': user.full_name,
                'avg_order_value': avg_order_value
            })
        print('q4 success')

    def avg_execution(self, func):
        exe_time = []
        for x in range(10):
            start_time = time.time()
            func()
            end_time = time.time()
            execution_time = end_time - start_time
            exe_time.append(execution_time)
        return sum(exe_time) / 10

    def query(self):
        functions = [self.q1, self.q2, self.q3, self.q4]
        exe_time_q1234 = []
        for i, func in enumerate(functions, 1):
            exe_time_q1234.append(self.avg_execution(func))
        return exe_time_q1234

    def indexed_query(self):
        functions = [self.indexed_q1, self.indexed_q2, self.indexed_q3, self.indexed_q4]
        exe_time_q1234 = []
        for i, func in enumerate(functions, 1):
            exe_time_q1234.append(self.avg_execution(func))
        return exe_time_q1234

    def create_indexes(self):

        self.execute("""CREATE INDEX IF NOT EXISTS ON countries (continent_name);""")
        self.execute("""CREATE INDEX IF NOT EXISTS ON users (country_code);""")

        self.execute("""CREATE INDEX IF NOT EXISTS ON orders (status);""")
        self.execute("""CREATE INDEX IF NOT EXISTS ON orders (user_id);""")

        # self.execute("""CREATE INDEX IF NOT EXISTS ON order_items (order_id);""")
        self.execute("""CREATE INDEX IF NOT EXISTS ON order_items (product_id);""")

        self.execute("""CREATE INDEX IF NOT EXISTS ON products (merchant_id);""")

    def indexed_q1(self):
        result = []
        continent_name = 'North America'
        query = SimpleStatement("SELECT country_code FROM countries WHERE continent_name = %s ")
        rows = self._session.execute(query, [continent_name])
        for row in rows:
            country_code = row.country_code
            query = SimpleStatement("SELECT full_name, email FROM users WHERE country_code = %s ")
            user_rows = self._session.execute(query, [country_code])
            for user_row in user_rows:
                result.append(user_row)
        print('q1 success')

    def indexed_q2(self):
        query = SimpleStatement("SELECT * FROM orders WHERE status = 'Shipped' ")
        orders = self._session.execute(query)
        result = []

        for order in orders:
            # Get user details for the order
            query = SimpleStatement("SELECT full_name, email FROM users WHERE user_id = %s")
            user = self._session.execute(query, [order.user_id]).one()

            # Get order items for the order
            query = SimpleStatement("SELECT * FROM order_items WHERE order_id = %s ALLOW FILTERING")
            order_items = self._session.execute(query, [order.order_id])

            for order_item in order_items:
                # Get product details for the order item
                query = SimpleStatement("SELECT name, price FROM products WHERE product_id = %s")
                product = self._session.execute(query, [order_item.product_id]).one()

                result.append({
                    'order_id': order.order_id,
                    'user_name': user.full_name,
                    'user_email': user.email,
                    'product_name': product.name,
                    'quantity': order_item.quantity,
                    'price': product.price,
                    'status': order.status,
                    'created_at': order.created_at
                })
        print('q2 success')

    def indexed_q3(self):
        query = SimpleStatement("SELECT * FROM merchants")
        merchants = self._session.execute(query)
        result = []

        for merchant in merchants:
            # Get all products offered by the merchant
            query = SimpleStatement("SELECT * FROM products WHERE merchant_id = %s ")
            products = self._session.execute(query, [merchant.merchant_id])

            total_revenue = 0
            for product in products:
                query = SimpleStatement("SELECT * FROM order_items WHERE product_id = %s ALLOW FILTERING")
                order_items = self._session.execute(query, [product.product_id])

                for order_item in order_items:
                    total_revenue += product.price * order_item.quantity

            result.append({
                'merchant_id': merchant.merchant_id,
                'merchant_name': merchant.merchant_name,
                'total_revenue': total_revenue
            })
        print('q3 success')

    def indexed_q4(self):
        query = SimpleStatement("SELECT * FROM users")
        users = self._session.execute(query)

        result = []
        for user in users:
            query = SimpleStatement("SELECT * FROM orders WHERE user_id = %s ")
            orders = self._session.execute(query, [user.user_id])

            total_order_value = 0
            order_count = 0
            for order in orders:
                if order.status == 'Pending':
                    continue

                query = SimpleStatement("SELECT * FROM order_items WHERE order_id = %s ALLOW FILTERING")
                order_items = self._session.execute(query, [order.order_id])

                for order_item in order_items:
                    query = SimpleStatement("SELECT price FROM products WHERE product_id = %s")
                    product = self._session.execute(query, [order_item.product_id]).one()

                    total_order_value += product.price * order_item.quantity
                    order_count += 1

            avg_order_value = total_order_value / order_count if order_count > 0 else 0

            result.append({
                'user_id': user.user_id,
                'full_name': user.full_name,
                'avg_order_value': avg_order_value
            })
        print('q4 success')
