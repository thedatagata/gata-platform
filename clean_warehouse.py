import duckdb

CON_PATH = 'warehouse/sandbox.duckdb'

def clean_raw_tables():
    try:
        con = duckdb.connect(CON_PATH)
        tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE 'raw_%'").fetchall()
        
        print(f"Found {len(tables)} raw tables to drop.")
        for schema, table in tables:
            full_name = f"{schema}.{table}"
            print(f"Dropping {full_name}...")
            con.execute(f"DROP TABLE IF EXISTS {full_name}")
            
        print("Cleanup complete.")
        con.close()
    except Exception as e:
        print(f"Error cleaning tables: {e}")

if __name__ == '__main__':
    clean_raw_tables()
