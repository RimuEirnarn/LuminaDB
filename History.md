# "Why are you developing this repo?"

I have various reasons, first i want to use SQLite as easy as i want. Easy and simple. I remember about MongoDB, it has quite easy API interface (although it's NoSQL).

In short minute, i asked ChatGPT to create a python class that have quite the same functionality as MongoDB collections.

Here's what ChatGPT gave to me:

```python
import sqlite3

class Database:
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def insert_one(self, collection, document):
        keys = document.keys()
        values = list(document.values())

        query = f"INSERT INTO {collection} ({', '.join(keys)}) VALUES ({', '.join(['?' for _ in range(len(values))])})"

        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()

        return cursor.lastrowid

    def find(self, collection, filter):
        keys = filter.keys()
        values = tuple(filter.values())

        query = f"SELECT * FROM {collection} WHERE {' AND '.join([f'{key}=?' for key in keys])}"

        cursor = self.conn.cursor()
        cursor.execute(query, values)

        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        return [dict(zip(columns, row)) for row in rows]

    def update_one(self, collection, filter, update):
        filter_keys = filter.keys()
        filter_values = tuple(filter.values())
        update_keys = update.keys()
        update_values = tuple(update.values())

        query = f"UPDATE {collection} SET {', '.join([f'{key}=?' for key in update_keys])} WHERE {' AND '.join([f'{key}=?' for key in filter_keys])}"

        cursor = self.conn.cursor()
        cursor.execute(query, (*update_values, *filter_values))
        self.conn.commit()

        return cursor.rowcount

    def delete_one(self, collection, filter):
        keys = filter.keys()
        values = tuple(filter.values())

        query = f"DELETE FROM {collection} WHERE {' AND '.join([f'{key}=?' for key in keys])}"

        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()

        return cursor.rowcount
```

i'm not satisfied by the result and then began to develop this repo!

## "Why is the name 'LuminaDB'?"

I have some problems with myself, the name just be like that because i have trouble naming something and i usually think quickly (and it's a quite of problem)
