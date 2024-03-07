# DataBase-Indexing-Permance-On-Compex-Queries
This repository contains scripts and utilities for testing and benchmarking various databases, including MySQL, MongoDB, Neo4j, Redis, and Cassandra. The repository includes functionalities for database creation, indexing, query execution, and benchmarking.

## Introduction

Modern applications often rely on different types of databases to store and manage data efficiently. The choice of database technology depends on various factors such as scalability, performance, data structure, and query requirements. This repository aims to provide a comprehensive testing framework for evaluating the performance and capabilities of different database systems.

The supported databases for testing and benchmarking include:

- MySQL
- MongoDB
- Neo4j
- Redis
- Cassandra

The repository includes scripts and utilities for:

- Database creation
- Indexing
- Query execution
- Benchmarking

## Setup

To use the scripts in this repository, follow these installation instructions:

1.  Clone the repository to your local machine:
    
    bashCopy code
    
    `git clone <repository_url>` 
    
2.  Install the necessary dependencies for each database driver:
    
    -   For MySQL: Install the MySQL Connector/Python driver.
    -   For MongoDB: Install the PyMongo driver.
    -   For Neo4j: Install the Neo4j Python driver.
    -   For Redis: Install the Redis Python client.
    -   For Cassandra: Install the Cassandra Python driver.

## Usage

Before using the scripts, make sure to update the connection details for each database in the respective scripts (`MySQLConnection.py`, `MongoDBConnection.py`, `Neo4jConnection.py`, `RedisConnection.py`, `CassandraConnector.py`).

To execute the scripts:

1.  Open the terminal.
    
2.  Navigate to the root directory of the cloned repository.
    
3.  Run the desired script using Python. For example:
    `python main.py` 
    
4.  Uncomment the function calls for the databases you want to interact with in the `__main__` block at the end of the script.
