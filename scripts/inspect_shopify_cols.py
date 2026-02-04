import duckdb
import os

token = os.environ.get("MOTHERDUCK_TOKEN")
con = duckdb.connect(f'md:my_db?motherduck_token={token}')

try:
    tables = con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='shopify'").fetchall()
    print("Tables in shopify schema:")
    for t in tables:
        print(t[0])
except Exception as e:
    print(f"Error: {e}")
