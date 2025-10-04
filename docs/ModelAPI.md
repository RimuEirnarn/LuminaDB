# 📘 ModelAPI – A Friendly Guide to a Lightweight SQLite ORM

> **Note**: ModelAPI uses a different structure than TableAPI! So if you're coming from that, treat this as a fresh start.

---

## 📚 Table of Contents

- [📘 ModelAPI – A Friendly Guide to a Lightweight SQLite ORM](#-modelapi--a-friendly-guide-to-a-lightweight-sqlite-orm)
  - [📚 Table of Contents](#-table-of-contents)
  - [🧠 Introduction](#-introduction)
  - [🚀 Getting Started](#-getting-started)
    - [1. Bootstrapping the Database](#1-bootstrapping-the-database)
    - [2. Creating a Model](#2-creating-a-model)
      - [💡 How it works](#-how-it-works)
    - [3. Running CRUD Operations](#3-running-crud-operations)
      - [CREATE](#create)
      - [READ](#read)
        - [Advanced `.where()` filtering](#advanced-where-filtering)
      - [UPDATE](#update)
      - [DELETE](#delete)
  - [🎮 Example App – CRUD in Action](#-example-app--crud-in-action)
  - [🙋 FAQ](#-faq)
  - [💡 Tips \& Notes](#-tips--notes)

---

## 🧠 Introduction

**ModelAPI** gives you a Laravel-style, class-based ORM interface for SQLite in Python. It's minimal, fast, and easy to use.

Think of it like this:

- You define your data with Python classes.
- The ORM handles schema definition, inserts, reads, updates, and deletes.
- It's flexible enough for small scripts and powerful enough for CLI tools.

---

## 🚀 Getting Started

Let’s walk through setting things up step-by-step!

---

### 1. Bootstrapping the Database

Create a file like `db.py` to initialize your SQLite database.

```py
# db.py
from luminadb import Database

db = Database(":memory:")  # or use "your_file.db" to persist data
```

This creates a simple in-memory SQLite database. For persistent storage, pass a filename instead of `":memory:"`.

---

### 2. Creating a Model

Each model is a Python class with typed fields and a schema definition.

```py
# model/notes.py
from uuid import uuid4
from luminadb import model, BaseModel, Primary
from ..db import db

@model(db)
class Notes(BaseModel):
    __schema__ = (Primary('id'),)
    __auto_id__ = lambda: str(uuid4())

    id: str
    title: str
    content: str
```

#### 💡 How it works

- `@model(db)` binds the class to the database.
- `__schema__` defines your table schema. You can use `Primary()`, `Unique()`, `Foreign()`, etc.
- `__auto_id__` is optional. It helps generate IDs automatically (e.g. UUIDs).

---

### 3. Running CRUD Operations

#### CREATE

```py
Notes.create(title="Hello", content="World!")
```

You can gather user input too:

```py
title = input('Title: ')
content = input('Content: ')
Notes.create(title=title, content=content)
```

---

#### READ

You can fetch all rows, one row, or use filters:

```py
Notes.all()                  # Returns a list of all notes
Notes.first(id="123")        # Returns the first match or None
Notes.one(id="123")          # Returns exactly one result, else throws error
```

##### Advanced `.where()` filtering

```py
# Find a note by title
note = Notes.where(title="Shopping List").fetch_one()

# Limit or offset
Notes.where().limit(5).fetch()     # Get top 5 notes
Notes.where().offset(5).fetch()    # Skip 5 and get the rest

# Count matching rows
count = Notes.where().count()
```

---

#### UPDATE

Updating a row is super simple. Fetch it first, then call `.update()`:

```py
note = Notes.first(id="some-id")
note.update(title="Updated", content="Updated content here.")
```

---

#### DELETE

Delete just like you'd update:

```py
note = Notes.first(id="some-id")
note.delete()
```

---

## 🎮 Example App – CRUD in Action

Here’s a small CLI app to tie it all together:

```py
from enum import IntEnum
from uuid import uuid4
from luminadb import Database, model, BaseModel, Primary, Null

db = Database(":memory:")

@model(db)
class Notes(BaseModel):
    __schema__ = (Primary('id'),)
    __auto_id__ = lambda: str(uuid4())

    id: str
    title: str
    content: str

def display():
    print('-'*3)
    for note in Notes.all():
        print(f"ID      : {note.id}")
        print(f"Title   : {note.title}")
        print(f"Content : {note.content}")
        print("-"*3)

def read(prompt: str):
    try:
        return input(prompt)
    except KeyboardInterrupt:
        return Null

def create():
    title = input("Title: ")
    content = input("Content: ")
    Notes.create(title=title, content=content)

def update():
    note_id = input('ID: ')
    note = Notes.first(id=note_id)
    if note:
        title = read("New title: ")
        content = read("New content: ")
        note.update(title=title, content=content)
    else:
        print("Note not found.")

def delete():
    note_id = input('ID: ')
    note = Notes.first(id=note_id)
    if note:
        note.delete()
    else:
        print("Note not found.")

class CMD(IntEnum):
    DISPLAY = 1
    CREATE = 2
    UPDATE = 3
    DELETE = 4
    EXIT = 5

def main():
    while True:
        print('-'*8)
        print('1. Display all notes')
        print('2. Create a note')
        print('3. Update a note')
        print('4. Delete a note')
        print('5. Exit')
        try:
            cmd = int(input("Command: "))
            if cmd == CMD.DISPLAY:
                display()
            elif cmd == CMD.CREATE:
                create()
            elif cmd == CMD.UPDATE:
                update()
            elif cmd == CMD.DELETE:
                delete()
            elif cmd == CMD.EXIT:
                break
        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(f"{type(exc).__name__}: {exc}")

if __name__ == "__main__":
    main()
```

---

## 🙋 FAQ

**Q: Can I define relationships between models?**
A: Yes, using `Foreign()` in `__schema__`. This doc will be updated with examples soon.

**Q: Does it support migrations?**
A: Not directly. The schema is defined per-model, so migrations require manual changes.

**Q: How are models stored?**
A: The models reflect SQLite tables. Everything is automatically synced on class definition.

---

## 💡 Tips & Notes

- You can use any primitive Python types (e.g. `int`, `str`, `float`) in your model class.
- Keep `__auto_id__` short and efficient — UUIDs are recommended.
- Don’t forget to call `Database()` only once and reuse the instance.
