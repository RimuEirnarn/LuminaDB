# LuminaDB (Beginner-Friendly Guide to Table API)

This library is a simple wrapper for Python's built-in SQLite package. It simplifies database operations, making it easier to interact with SQLite without writing complex SQL queries.

## Table of Contents

- [LuminaDB (Beginner-Friendly Guide to Table API)](#luminadb-beginner-friendly-guide-to-table-api)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Getting Started](#getting-started)
    - [What is SQLite?](#what-is-sqlite)
    - [LuminaDB](#luminadb)
    - [Installation](#installation)
    - [Creating a Database](#creating-a-database)
    - [Creating a Table](#creating-a-table)
    - [Inserting Data](#inserting-data)
    - [Retrieving Data](#retrieving-data)
    - [Updating Data](#updating-data)
    - [Deleting Data](#deleting-data)
  - [Advanced Features](#advanced-features)
    - [Filtering Data](#filtering-data)
    - [Sorting and Pagination](#sorting-and-pagination)
    - [Exporting Data](#exporting-data)
  - [Conclusion](#conclusion)

---

## Introduction

SQLite is a lightweight, serverless database that stores data in a single file. This wrapper makes working with SQLite easier by providing a simple API similar to NoSQL databases.

## Getting Started

### What is SQLite?

SQLite is a small and fast database engine that stores all data in a single file. It is widely used in applications where a full-fledged database server is unnecessary.

### LuminaDB

This library itself is intended to help around your life from having to write SQL statements unless in specific cases. For now, implemented API is Table API which is what is this documentation will show to you.

There's 2 taste what we've developed, both are named as "TableAPI" and "ModelAPI". Refer to [this for ModelAPI](./ModelAPI.md)

### Installation

If the library is available on PyPI, install it using:

```bash
pip install luminadb
```

Then, import it in your script:

```python
from luminadb import Database, integer, text
```

### Creating a Database

To create an **in-memory** database:

```python
db = Database(":memory:")  # Temporary storage (erased when the script ends)
```

To create a database stored in a file:

```python
db = Database("my_database.db")  # Stores data persistently
```

### Creating a Table

A table is like a spreadsheet where each row is a record, and columns define the data structure. Let’s create a `users` table with `id` and `name` columns:

```python
users = db.create_table("users", [
    integer("id"),  # ID column (integer)
    text("name")     # Name column (text)
])
```

### Inserting Data

To add a user:

```python
users.insert({"id": 1, "name": "Alice"})
```

To insert multiple users:

```python
users.insert_multiple([
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"}
])
```

### Retrieving Data

To fetch all users:

```python
all_users = users.select()
print(all_users)  # Output: [Row(id=1, name='Alice'), Row(id=2, name='Bob'), Row(id=3, name='Charlie')]
```

To fetch only names:

```python
user_names = users.select(what=("name",))
print(user_names)  # Output: ['Alice', 'Bob', 'Charlie']
```

### Updating Data

To change Alice’s name to Bob:

```python
users.update({"id": 1}, {"name": "Bob"})
```

### Deleting Data

To remove Bob from the database:

```python
users.delete({"id": 1})
```

---

## Advanced Features

### Filtering Data

You can filter data using operators:

```python
from luminadb.operators import eq, like, between
```

Example: Select users where `id` is 2:

```python
data = users.select({"id": eq(2)})
```

Search for users whose names start with "A":

```python
data = users.select([like("name", "A%")])
```

### Sorting and Pagination

Sort results in ascending or descending order:

```python
users.select(order=("age", "desc"))  # Sort in descending order
```

Paginate results (fetch 2 users per page):

```python
for page in users.paginate_select(length=2):
    print(page)  # Each page contains 2 users
```

### Exporting Data

Export a table to CSV:

```python
from luminadb.csv import to_csv_file
to_csv_file(users, "users.csv")
```

Export an entire database:

```python
to_csv_file(db, "DatabasePath")
```

---

## Conclusion

This library simplifies SQLite operations, making it beginner-friendly while retaining powerful features. Whether you're building a simple app or need lightweight database management, it’s a great tool to use!

For more advanced use cases, refer to the full documentation or explore more features.
