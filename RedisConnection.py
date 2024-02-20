import csv
import time

import redis


class RedisConnection:
    def __init__(self, host, port, password):
        self.host = host
        self.port = port
        self.password = password
        self.connection = None

    def connect(self):
        try:
            self.connection = redis.StrictRedis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True
            )
            print("Connected to Redis successfully!")
            return self.connection
        except Exception as e:
            print(f"Error: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection to Redis closed.")

    def load_data_to_redis(self, file_path, redis_key):
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)

            keys = next(csv_reader)

            for row in csv_reader:
                record = dict(zip(keys, row))
                redis_key_value = record[keys[0]]
                self.connection.hmset(f"{redis_key}:{redis_key_value}", record)

    def insert_data(self):
        self.load_data_to_redis('countries.csv', 'countries')
        self.load_data_to_redis('users.csv', 'users')
        self.load_data_to_redis('merchants.csv', 'merchants')
        self.load_data_to_redis('orders.csv', 'orders')
        self.load_data_to_redis('products.csv', 'products')
        self.load_data_to_redis('order_items.csv', 'order_items')

    def clear_db(self):
        self.connection.flushdb()

    def create_index(self, data_type, field):
        index_key = f'{data_type}_index:{field}'

        for key in self.connection.keys(f'{data_type}:*'):
            data = self.connection.hgetall(key)
            field_value = data.get(field, '')
            self.connection.sadd(f'{index_key}:{field_value}', key)

    def create_indexes(self):
        self.create_index('countries', 'continent_name')
        self.create_index('users', 'country_code')
        self.create_index('products', 'merchant_id', )
        self.create_index('orders', 'status')

    def q1(self):
        start_time = time.time()
        user_keys = self.connection.keys('users:*')
        country_keys = self.connection.keys('countries:*')

        users_data = [self.connection.hgetall(key) for key in user_keys]
        countries_data = [self.connection.hgetall(key) for key in country_keys]

        filtered_countries = [country for country in countries_data if country['continent_name'] == 'North America']

        filtered_country_codes = [country['country_code'] for country in filtered_countries]

        filtered_users = [user for user in users_data if user['country_code'] in filtered_country_codes]

        result = [(user['full_name'], user['email']) for user in filtered_users]
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q2(self):
        start_time = time.time()
        # Get all keys for orders, users, order_items, and products
        order_keys = self.connection.keys('orders:*')
        user_keys = self.connection.keys('users:*')
        order_item_keys = self.connection.keys('order_items:*')
        product_keys = self.connection.keys('products:*')

        # Fetch all orders, users, order_items, and products data
        orders_data = [self.connection.hgetall(key) for key in order_keys]
        users_data = [self.connection.hgetall(key) for key in user_keys]
        order_items_data = [self.connection.hgetall(key) for key in order_item_keys]
        products_data = [self.connection.hgetall(key) for key in product_keys]

        # Filter orders by status
        filtered_orders = [order for order in orders_data if order['status'] == 'Shipped']

        # Initialize result list
        result = []

        # Loop through each filtered order
        for order in filtered_orders:
            # Find the corresponding user, order_items, and products
            user = next((user for user in users_data if user['user_id'] == order['user_id']), None)
            order_items = [item for item in order_items_data if item['order_id'] == order['order_id']]
            products = [product for product in products_data if
                        any(item['product_id'] == product['product_id'] for item in order_items)]

            # Loop through each order_item and product
            for order_item, product in zip(order_items, products):
                # Append the selected fields to the result list
                result.append({
                    'order_id': order['order_id'],
                    'user_name': user['full_name'],
                    'user_email': user['email'],
                    'product_name': product['name'],
                    'quantity': order_item['quantity'],
                    'price': product['price'],
                    'status': order['status'],
                    'created_at': order['created_at']
                })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q3(self):
        start_time = time.time()
        merchant_keys = self.connection.keys('merchants:*')
        product_keys = self.connection.keys('products:*')
        order_item_keys = self.connection.keys('order_items:*')

        # Fetch all merchants, products, and order_items data
        merchants_data = [self.connection.hgetall(key) for key in merchant_keys]
        products_data = [self.connection.hgetall(key) for key in product_keys]
        order_items_data = [self.connection.hgetall(key) for key in order_item_keys]

        # Initialize result list
        result = []

        # Loop through each merchant
        for merchant in merchants_data:
            # Find the corresponding products and order_items
            products = [product for product in products_data if product['merchant_id'] == merchant['merchant_id']]
            order_items = [item for item in order_items_data if
                           any(item['product_id'] == product['product_id'] for product in products)]

            # Calculate total revenue
            total_revenue = sum(float(product['price']) * int(order_item['quantity']) for product, order_item in
                                zip(products, order_items))

            # Append the selected fields and total revenue to the result list
            result.append({
                'merchant_id': merchant['merchant_id'],
                'merchant_name': merchant['merchant_name'],
                'total_revenue': total_revenue
            })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q4(self):
        start_time = time.time()
        user_keys = self.connection.keys('users:*')
        order_keys = self.connection.keys('orders:*')
        order_item_keys = self.connection.keys('order_items:*')
        product_keys = self.connection.keys('products:*')

        users_data = [self.connection.hgetall(key) for key in user_keys]
        orders_data = [self.connection.hgetall(key) for key in order_keys]
        order_items_data = [self.connection.hgetall(key) for key in order_item_keys]
        products_data = [self.connection.hgetall(key) for key in product_keys]

        # Filter orders by status
        filtered_orders = [order for order in orders_data if order['status'] != 'Pending']

        # Initialize result list
        result = []

        # Loop through each user
        for user in users_data:
            # Find the corresponding orders, order_items, and products
            orders = [order for order in filtered_orders if order['user_id'] == user['user_id']]
            order_items = [item for item in order_items_data if
                           any(item['order_id'] == order['order_id'] for order in orders)]
            products = [product for product in products_data if
                        any(item['product_id'] == product['product_id'] for item in order_items)]

            # Calculate average order value
            total_order_value = sum(float(product['price']) * int(order_item['quantity']) for product, order_item in
                                    zip(products, order_items))
            avg_order_value = total_order_value / len(orders) if orders else 0

            # Append the selected fields and average order value to the result list
            result.append({
                'user_id': user['user_id'],
                'full_name': user['full_name'],
                'avg_order_value': avg_order_value
            })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q1_indexed(self, continent_filter):
        start_time = time.time()
        country_keys = self.connection.smembers(f'countries_index:continent_name:{continent_filter}')
        country_ids = [key.split(":")[1] for key in country_keys]

        user_keys = [
            key
            for country_id in country_ids
            for key in self.connection.smembers(f'users_index:country_code:{country_id}')
        ]

        users_data = [self.connection.hgetall(key) for key in user_keys]

        result = [(user['full_name'], user['email']) for user in users_data]

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q2_indexed(self, status_filter):
        start_time = time.time()

        order_keys = self.connection.smembers(f'orders_index:status:{status_filter}')
        orders_data = [self.connection.hgetall(key) for key in order_keys]

        result = []
        for order in orders_data:
            user_key = f'users:{order["user_id"]}'
            user_name = self.connection.hget(user_key, 'full_name')
            user_email = self.connection.hget(user_key, 'email')

            order_items_key = f'order_items:{order["order_id"]}'
            order_items_data = self.connection.hgetall(order_items_key)

            product_key = f'products:{order_items_data["product_id"]}'
            product_name = self.connection.hget(product_key, 'name')

            result.append({
                'order_id': order['order_id'],
                'user_name': user_name,
                'user_email': user_email,
                'product_name': product_name,
                'quantity': order_items_data['quantity'],
                'price': self.connection.hget(product_key, 'price'),
                'status': order['status'],
                'created_at': order['created_at']
            })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q3_indexed(self):
        start_time = time.time()

        merchant_keys = self.connection.keys('merchants:*')
        merchants_data = [self.connection.hgetall(key) for key in merchant_keys]

        order_item_keys = self.connection.keys('order_items:*')
        order_items_data = [self.connection.hgetall(key) for key in order_item_keys]

        result = []
        for merchant in merchants_data:
            product_keys = self.connection.smembers(f'products_index:merchant_id:{merchant["merchant_id"]}')
            products_data = [self.connection.hgetall(key) for key in product_keys]

            order_items = [item for item in order_items_data if
                           any(item['product_id'] == product['product_id'] for product in products_data)]

            total_revenue = sum(float(product['price']) * int(order_item['quantity']) for product, order_item in
                                zip(products_data, order_items))
            # Append the selected fields and total revenue to the result list
            result.append({
                'merchant_id': merchant['merchant_id'],
                'merchant_name': merchant['merchant_name'],
                'total_revenue': total_revenue
            })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def q4_indexed(self):
        start_time = time.time()
        user_keys = self.connection.keys('users:*')
        order_keys = self.connection.keys('orders:status:*')
        order_item_keys = self.connection.keys('order_items:*')
        product_keys = self.connection.keys('products:*')

        users_data = [self.connection.hgetall(key) for key in user_keys]
        orders_data = [self.connection.hgetall(key) for key in order_keys]
        order_items_data = [self.connection.hgetall(key) for key in order_item_keys]
        products_data = [self.connection.hgetall(key) for key in product_keys]

        result = []

        for user in users_data:
            orders = [order for order in orders_data if order['user_id'] == user['user_id']]
            order_items = [item for item in order_items_data if
                           any(item['order_id'] == order['order_id'] for order in orders)]
            products = [product for product in products_data if
                        any(item['product_id'] == product['product_id'] for item in order_items)]
            total_order_value = sum(float(product['price']) * int(order_item['quantity']) for product, order_item in
                                    zip(products, order_items))
            avg_order_value = total_order_value / len(orders) if orders else 0
            result.append({
                'user_id': user['user_id'],
                'full_name': user['full_name'],
                'avg_order_value': avg_order_value
            })

        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time

    def complex_query_tester(self):

        que_exe_avg = []

        q1 = []
        for x in range(10):
            q1.append(self.q1()[1])
        que_exe_avg.append(sum(q1) / 10)

        q2 = []
        for x in range(10):
            q2.append(self.q2()[1])
        que_exe_avg.append(sum(q2) / 10)

        q3 = []
        for x in range(10):
            q3.append(self.q3()[1])
        que_exe_avg.append(sum(q3) / 10)

        q4 = []
        for x in range(10):
            q4.append(self.q4()[1])
        que_exe_avg.append(sum(q4) / 10)

        return que_exe_avg

    def execute_all_queries(self):
        # results = {}
        #
        # result_q1, time_q1 = self.q1_indexed('North America')
        # results['q1'] = {'result': result_q1, 'execution_time': time_q1}
        # print(time_q1)
        #
        # result_q2, time_q2 = self.q2_indexed('Shipped')
        # results['q2'] = {'result': result_q2, 'execution_time': time_q2}
        # print(time_q2)
        #
        # result_q3, time_q3 = self.q3_indexed()
        # results['q3'] = {'result': result_q3, 'execution_time': time_q3}
        # print(time_q3)
        #
        # result_q4, time_q4 = self.q4_indexed()
        # results['q4'] = {'result': result_q4, 'execution_time': time_q4}
        # print(time_q4)
        que_exe_avg = []

        q1 = []
        for x in range(10):
            q1.append(self.q1_indexed('North America')[1])
        que_exe_avg.append(sum(q1) / 10)

        q2 = []
        for x in range(10):
            q2.append(self.q2_indexed('Shipped')[1])
        que_exe_avg.append(sum(q2) / 10)

        q3 = []
        for x in range(10):
            q3.append(self.q3_indexed()[1])
        que_exe_avg.append(sum(q3) / 10)

        q4 = []
        for x in range(10):
            q4.append(self.q4_indexed()[1])
        que_exe_avg.append(sum(q4) / 10)

        return que_exe_avg
