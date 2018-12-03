Lodex
=====

Lodex is a very, very simple mvcc-capable nosql key-value database. Calling it
a database might actually be pushing it. Essentially, values are appended to
the database file, and then the updated index is appended afterwards. This
means that the database can only grow, which might not be what you want.


