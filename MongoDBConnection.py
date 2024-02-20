import time

import pandas as pd
import pymongo


class MongoDBConnection:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.client = None

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.connection_string)
            print("Connected to MongoDB successfully!")
            return self.client
        except Exception as e:
            print(f"Error: {e}")
            return None

    def close(self):
        if self.client:
            self.client.close()
            print("Connection to MongoDB closed.")

    def insert_data(self, collection_name, dataframe, db):
        collection_data = dataframe.to_dict(orient='records')
        db[collection_name].insert_many(collection_data)

    def insert_from_file(self, db_name):
        self.client.drop_database(db_name)
        db = self.client[db_name]
        countries_df = pd.read_csv('countries.csv')
        users_df = pd.read_csv('users.csv')
        merchants_df = pd.read_csv('merchants.csv')
        orders_df = pd.read_csv('orders.csv')
        products_df = pd.read_csv('products.csv')
        order_items_df = pd.read_csv('order_items.csv')

        self.insert_data('countries', countries_df, db)
        self.insert_data('users', users_df, db)
        self.insert_data('merchants', merchants_df, db)
        self.insert_data('orders', orders_df, db)
        self.insert_data('products', products_df, db)
        self.insert_data('order_items', order_items_df, db)

        relevant_orders = db.order_items.distinct('order_id')
        for order_id in relevant_orders:
            order_items = db.order_items.find({'order_id': order_id})
            products_info = []

            for order_item in order_items:
                product_id = order_item['product_id']
                quantity = order_item['quantity']

                product_info = {
                    'product_id': product_id,
                    'quantity': quantity
                }

                products_info.append(product_info)

            db.orders.update_one({'order_id': order_id}, {'$set': {'products_info': products_info}})

        db['order_items'].drop()

    def complex_queries(self, db_name):
        db = self.client[db_name]

        query_exe_time = []

        q1_exe = []
        for x in range(10):
            start_time_q1 = time.time()
            result_q1 = db.users.find(
                {'country_code': {
                    '$in': db.countries.find({'continent_name': 'North America'}).distinct('country_code')}}
            )
            end_time_q1 = time.time()
            execution_time_q1 = end_time_q1 - start_time_q1
            q1_exe.append(execution_time_q1)
        query_exe_time.append(sum(q1_exe) / 10)

        q2_exe = []
        for x in range(10):
            start_time_q2 = time.time()
            result_q2 = db.orders.aggregate([
                {'$match': {'status': 'Shipped'}},
                {'$lookup': {
                    'from': 'users',
                    'localField': 'user_id',
                    'foreignField': 'user_id',
                    'as': 'user'
                }},
                {'$unwind': '$user'},
                {'$unwind': '$products_info'},  # Assuming you have embedded products_info in orders
                {'$lookup': {
                    'from': 'products',
                    'localField': 'products_info.product_id',
                    'foreignField': 'product_id',
                    'as': 'product'
                }},
                {'$unwind': '$product'},
                {'$project': {
                    'order_id': 1,
                    'user_name': '$user.full_name',
                    'user_email': '$user.email',
                    'product_name': '$product.name',
                    'quantity': '$products_info.quantity',
                    'price': '$product.price',
                    'status': 1,
                    'created_at': 1
                }}
            ])
            end_time_q2 = time.time()
            execution_time_q2 = end_time_q2 - start_time_q2
            q2_exe.append(execution_time_q2)
        query_exe_time.append(sum(q2_exe) / 10)

        q3_exe = []
        for x in range(10):
            start_time_q3 = time.time()
            result_q3 = db.merchants.aggregate([
                {'$lookup': {
                    'from': 'products',
                    'localField': 'merchant_id',
                    'foreignField': 'merchant_id',
                    'as': 'products'
                }},
                {'$unwind': '$products'},
                {'$lookup': {
                    'from': 'orders',
                    'localField': 'products_info.order_id',  # Assuming you have embedded products_info in orders
                    'foreignField': 'order_id',
                    'as': 'order'
                }},
                {'$unwind': '$order'},
                {'$group': {
                    '_id': {'merchant_id': '$merchant_id', 'merchant_name': '$merchant_name'},
                    'total_revenue': {'$sum': {'$multiply': ['$products.price', '$order.products_info.quantity']}}
                }}
            ])
            end_time_q3 = time.time()
            execution_time_q3 = end_time_q3 - start_time_q3
            q3_exe.append(execution_time_q3)
        query_exe_time.append(sum(q3_exe) / 10)

        q4_exe = []
        for x in range(10):
            start_time_q4 = time.time()
            result_q4 = db.users.aggregate([
                {'$lookup': {
                    'from': 'orders',
                    'localField': 'user_id',
                    'foreignField': 'user_id',
                    'as': 'orders'
                }},
                {'$unwind': '$orders'},
                {'$match': {'orders.status': {'$ne': 'Pending'}}},
                {'$unwind': '$orders.products_info'},  # Assuming you have embedded products_info in orders
                {'$lookup': {
                    'from': 'products',
                    'localField': 'orders.products_info.product_id',
                    'foreignField': 'product_id',
                    'as': 'product'
                }},
                {'$unwind': '$product'},
                {'$group': {
                    '_id': {'user_id': '$user_id', 'full_name': '$full_name'},
                    'avg_order_value': {'$avg': {'$multiply': ['$product.price', '$orders.products_info.quantity']}}
                }}
            ])
            end_time_q4 = time.time()
            execution_time_q4 = end_time_q4 - start_time_q4
            q4_exe.append(execution_time_q4)
        query_exe_time.append(sum(q4_exe) / 10)

    # print(f"Query 1 Execution Time: {execution_time_q1:.8f} seconds\n")
    # print(f"Query 2 Execution Time: {execution_time_q2:.8f} seconds\n")
    # print(f"Query 3 Execution Time: {execution_time_q3:.8f} seconds\n")
    # print(f"Query 4 Execution Time: {execution_time_q4:.8f} seconds\n")
        return query_exe_time

    def create_index(self, db_name):
        db = self.client[db_name]
        db.countries.create_index([('continent_name', 1), ('country_code', 1)])
        db.orders.create_index('order_id', unique=True)
        db.countries.create_index('country_code', unique=True)
        db.users.create_index('user_id', unique=True)
        db.merchants.create_index('merchant_id', unique=True)
        db.products.create_index([("product_id", pymongo.DESCENDING), ("merchant_id", pymongo.ASCENDING)], unique=True)

