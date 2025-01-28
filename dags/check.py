import duckdb
import pandas
import numpy
conn = duckdb.connect('sales_database1.duckdb')

tables = conn.execute("SHOW TABLES").fetchall()
columns_info = conn.execute("PRAGMA table_info(sales)").fetchall()
#print(columns_info)
#print(tables)
#result = conn.execute("SELECT item_qty, taxes, item_item_name FROM sales where name = 'ABC private LTD-0002'").fetchdf()
result = conn.execute("SELECT items  FROM sales ").fetchone()
print(result)

conn.close()


