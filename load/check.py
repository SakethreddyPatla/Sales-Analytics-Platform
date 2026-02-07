import duckdb
print(duckdb.__version__)
con = duckdb.connect("mydb.duckdb")
print(con.execute("PRAGMA version").fetchall())
