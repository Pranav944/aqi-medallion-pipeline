import sys

import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)
sql = open(sys.argv[1]).read()

for statement in sql.split(";"):
    if statement.strip():
        print("\n" + "-" * 60)
        print(statement.strip())
        con.sql(statement).show()