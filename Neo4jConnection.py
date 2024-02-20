import time

from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None

    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            print("Connected to Neo4j successfully!")
            return self.driver
        except Exception as e:
            print(f"Error: {e}")
            return None

    def close(self):
        if self.driver:
            self.driver.close()
            print("Connection to Neo4j closed.")

    def insert_data(self):
        cypher_script = [
            """LOAD CSV WITH HEADERS FROM "file:///countries.csv" AS row
            CREATE (:Country {country_code: toInteger(row.country_code), name: row.name, continent_name: row.continent_name});""",

            """LOAD CSV WITH HEADERS FROM "file:///users.csv" AS row
            CREATE (:User {user_id: toInteger(row.user_id), full_name: row.full_name, email: row.email, gender: row.gender, date_of_birth: row.date_of_birth});""",

            """LOAD CSV WITH HEADERS FROM "file:///merchants.csv" AS row
            CREATE (:Merchant {merchant_id: toInteger(row.merchant_id), merchant_name: row.merchant_name});""",

            """LOAD CSV WITH HEADERS FROM "file:///orders.csv" AS row
            CREATE (:Order {order_id: toInteger(row.order_id), status: row.status, created_at: row.created_at});""",

            """LOAD CSV WITH HEADERS FROM "file:///products.csv" AS row
            CREATE (:Product {product_id: toInteger(row.product_id), name: row.name, price: toInteger(row.price), status: row.status, created_at: row.created_at});""",

            """LOAD CSV WITH HEADERS FROM "file:///order_items.csv" AS row
            MATCH (order:Order {order_id: toInteger(row.order_id)})
            MATCH (product:Product {product_id: toInteger(row.product_id)})
            MERGE (order)-[:CONTAINS {quantity: toInteger(row.quantity)}]->(product);""",

            """LOAD CSV WITH HEADERS FROM "file:///merchants.csv" AS row
            MATCH (merchant:Merchant {merchant_id: toInteger(row.merchant_id)})
            MATCH (country:Country {country_code: toInteger(row.country_code)})
            MERGE (merchant)-[:OPERATES_IN]->(country);""",

            """LOAD CSV WITH HEADERS FROM "file:///users.csv" AS row
            MATCH (user:User {user_id: toInteger(row.user_id)})
            MATCH (country:Country {country_code: toInteger(row.country_code)})
            MERGE (user)-[:LIVES_IN]->(country);""",

            """LOAD CSV WITH HEADERS FROM "file:///merchants.csv" AS row
            MATCH (merchant:Merchant {merchant_id: toInteger(row.merchant_id)})
            MATCH (user:User {user_id: toInteger(row.user_id)})
            MERGE (merchant)-[:OWNED_BY]->(user);""",

            """LOAD CSV WITH HEADERS FROM "file:///orders.csv" AS row
            MATCH (order:Order {order_id: toInteger(row.order_id)})
            MATCH (user:User {user_id: toInteger(row.user_id)})
            MERGE (order)-[:PLACED_BY]->(user);""",

            """LOAD CSV WITH HEADERS FROM "file:///products.csv" AS row
            MATCH (product:Product {product_id: toInteger(row.product_id)})
            MATCH (merchant:Merchant {merchant_id: toInteger(row.merchant_id)})
            MERGE (product)-[:SOLD_BY]->(merchant);"""
        ]

        with self.driver.session() as session:
            for cyp_scr in cypher_script:
                session.run(cyp_scr)
            print("Data imported into Neo4j successfully.")

    def complex_queries_test(self):
        q1 = """MATCH (user:User)-[:LIVES_IN]->(country:Country {continent_name: 'North America'})
RETURN user.full_name, user.email;
"""
        q2 = """MATCH (order:Order {status: 'Shipped'})
MATCH (user:User)-[:PLACED_BY]->(order)
MATCH (order)-[:CONTAINS]->(product:Product)
RETURN
  order.order_id,
  user.full_name AS user_name,
  user.email AS user_email,
  product.name AS product_name,
  order.quantity,
  product.price,
  order.status,
  order.created_at;
"""
        q3 = """MATCH (merchant:Merchant)-[:OPERATES_IN]->(country:Country)
MATCH (merchant)-[:SOLD_BY]->(product:Product)-[:CONTAINS]->(order:Order)
WITH merchant, SUM(product.price * order.quantity) AS total_revenue
RETURN merchant.merchant_id, merchant.merchant_name, total_revenue;
"""
        q4 = """MATCH (user:User)-[:PLACED_BY]->(order:Order)-[:CONTAINS]->(product:Product)
WHERE order.status <> 'Pending'
WITH user, AVG(product.price * order.quantity) AS avg_order_value
RETURN user.user_id, user.full_name, avg_order_value;
"""
        queries = [q1, q2, q3, q4]

        avg_exe_time_4_querirs = []
        with self.driver.session() as session:
            for q in queries:
                exe_time = []
                for x in range(10):
                    start_time = time.time()
                    results = session.run(q)
                    end_time = time.time()
                    execution_time = end_time - start_time
                    exe_time.append(execution_time)
                    # print("Query Results:", results)
                    # print(f"Execution Time: {execution_time:.8f} seconds")
                avg_exe_time_4_querirs.append(sum(exe_time) / 10)

        return avg_exe_time_4_querirs

    def create_indexes(self):

        indexes = [
            # """CALL apoc.schema.assert({},{},true) YIELD label, key RETURN *""",
            """CREATE INDEX FOR (c:Country) ON (c.name);""",
            """CREATE INDEX FOR (c:Country) ON (c.continent_name);""",

            """CREATE INDEX FOR (u:User) ON (u.email);""",
            """CREATE INDEX FOR (u:User) ON (u.gender);""",
            """CREATE INDEX FOR (u:User) ON (u.date_of_birth);""",

            """CREATE INDEX FOR (m:Merchant) ON (m.merchant_name);""",

            """CREATE INDEX FOR (o:Order) ON (o.status);""",
            """CREATE INDEX FOR (o:Order) ON (o.created_at);""",

            """CREATE INDEX FOR (p:Product) ON (p.price);""",
            """CREATE INDEX FOR (p:Product) ON (p.status);""",
            """CREATE INDEX FOR (p:Product) ON (p.created_at);""",

            """CREATE INDEX FOR (oi:Order_Items) ON (oi.quantity);""",

            """CREATE CONSTRAINT FOR (country:Country) REQUIRE (country.country_code) IS NODE KEY;""",
            """CREATE CONSTRAINT FOR (user:User) REQUIRE (user.user_id) IS NODE KEY;""",
            """CREATE CONSTRAINT FOR (merchant:Merchant) REQUIRE (merchant.merchant_id) IS NODE KEY;""",
            """CREATE CONSTRAINT FOR (order:Order) REQUIRE (order.order_id) IS NODE KEY;""",
            """CREATE CONSTRAINT FOR (product:Product) REQUIRE (product.product_id) IS NODE KEY;"""
        ]

        with self.driver.session() as session:
            for q in indexes:
                results = session.run(q)

    def define_CALL(self):
        pass

    def complex_query_with_CALL(self):
        pass
