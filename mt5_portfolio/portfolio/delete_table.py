import sqlite3

def drop_table(broker_name):
    table = f"{broker_name.lower()}_returns"
    conn = sqlite3.connect("returns.db")
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    conn.close()

    print(f"Table '{table}' has been dropped.")



import sqlite3

def drop_column(broker_name, column_to_drop):
    table = f"{broker_name.lower()}_returns"
    conn = sqlite3.connect("returns.db")
    cursor = conn.cursor()

    # Get existing columns
    cursor.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cursor.fetchall()]

    if column_to_drop not in cols:
        print(f"Column '{column_to_drop}' does not exist in table '{table}'.")
        conn.close()
        return

    # Build new column list (everything except the one to drop)
    new_cols = [c for c in cols if c != column_to_drop]
    col_list = ", ".join(new_cols)

    # Create a temporary table
    temp_table = f"{table}_temp"

    cursor.execute(f"CREATE TABLE {temp_table} AS SELECT {col_list} FROM {table}")

    # Drop old table
    cursor.execute(f"DROP TABLE {table}")

    # Rename temp table to original name
    cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")

    conn.commit()
    conn.close()

    print(f"Column '{column_to_drop}' has been removed from table '{table}'.")


#drop_column(broker_name="aquafunded", column_to_drop="XAUUSD")
#drop_table("icmarkets")
#drop_table("aquafunded")
drop_table("acg")


