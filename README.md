# Relational DB
This project is an implementation of fundamental relational database operations using lists in the Racket programming language. The primary goal is to simulate relational tables and provide a set of functions for data manipulation, such as insertion, selection, projection, sorting, renaming, and joins.

DB table definition: A table consists of a schema and rows.
Column Information: Each column has a name and a type (number, string, symbol, or boolean).

# Operations:
- table-insert: Insert a new row while validating schema compatibility.
- table-project: Select a subset of columns from a table.
- table-rename: Rename a column.
- table-sort: Sort rows based on specified columns.
- table-select: Filter rows based on logical conditions.
- table-cross-join: Perform a Cartesian product of two tables.
- table-natural-join: Perform a natural join between two tables.
