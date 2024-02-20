from Cassandra import CassandraConnector
from MongoDBConnection import MongoDBConnection
from MySQLConnection import MySQLConnection
from Neo4jConnection import Neo4jConnection
from RedisConnection import RedisConnection


def mysql():
    mysql_connection = MySQLConnection("localhost", "root", "admin", "")
    mysql_connection.connect()

    mysql_results = []
    mysql_results2 = []
    if mysql_connection:
        # mysql_connection.createtables()
        # mysql_connection.insert_data()
        # # mysql_connection.insert_new()
        mysql_results.append(mysql_connection.complex_query_test("trade"))
        # mysql_connection.copy_database()
        # mysql_connection.create_index()
        mysql_results2.append(mysql_connection.complex_query_test("new_trade"))
        print(mysql_results)
        print(mysql_results2)
        mysql_connection.export_data()
        mysql_connection.close()

def mongo():
    mongodb_connection = MongoDBConnection("mongodb://localhost:27017")
    mongodb_connection.connect()
    mongodb_results = []
    if mongodb_connection:
        mongodb_connection.insert_from_file('trade')
        mongodb_results.append(mongodb_connection.complex_queries('trade'))
        mongodb_connection.create_index('trade')
        mongodb_results.append(mongodb_connection.complex_queries('trade'))
        print(mongodb_results)
        mongodb_connection.close()


def redis():
    redis_connection = RedisConnection("localhost", 6379, "")
    redis_connection.connect()
    redis_results = []
    if redis_connection:
        redis_connection.clear_db()
        redis_connection.insert_data()
        redis_results.append(redis_connection.complex_query_tester())
        redis_connection.create_indexes()
        redis_results.append(redis_connection.execute_all_queries())
        print(redis_results)
        redis_connection.close()


def neo4j():
    neo4j_connection = Neo4jConnection("bolt://localhost:7687", "neo4j", "adminadmin")
    neo4j_connection.connect()
    neo4j_results = []
    if neo4j_connection:
        neo4j_connection.insert_data()
        neo4j_results.append(neo4j_connection.complex_queries_test())
        neo4j_connection.create_indexes()
        neo4j_results.append(neo4j_connection.complex_queries_test())
        print(neo4j_results)
        neo4j_connection.close()


def cassandra():
    connector = CassandraConnector(["localhost"])
    connector.connect()
    connector.drop_keyspace('trade')
    connector.create_keyspace("trade")
    connector.execute("USE trade;")
    connector.create_tables()
    connector.insert_data()
    print(connector.query())
    # connector.create_indexes()
    # print(connector.indexed_query())
    # connector.disconnect()


if __name__ == "__main__":
    # mysql()
    # neo4j()
    # mongo()
    # redis()
    cassandra()

