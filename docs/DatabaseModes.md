# Database Modes

The basic `Database` class has different flavor, but they all works as usual.

There's at least 2 implemented Databases, for now:

1. Generic `Database` is what you'll use the most, single-threaded database that doesn't need explicit `.close()`
2. Database Thread Worker `DatabaseWorker` found in `luminadb.workers`, intended multithreaded environment, just remember to `.close()` like regular threads.
