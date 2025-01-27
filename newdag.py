def savesales_data_to_duckdb(file_path):
    DUCKDB_PATH = "/opt/airflow/apidata/sales_database.duckdb"
    # Connect to DuckDB (this will create the database file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_PATH)
 
    # Read the data from the JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)
 
    # Create the table if it doesn't exist (adjust schema according to the API data)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales (
           
            name TEXT,
            owner TEXT,
            workflow_state TEXT,
            customer TEXT,
            customer_name TEXT,
            order_type TEXT,
            company TEXT,
            total_qty FLOAT,
            base_total FLOAT,
            customer_group TEXT,
            territory TEXT,
            item_name TEXT,
            item_owner TEXT,
            payment_name TEXT,
            payment_amount FLOAT,
            payment_outstanding FLOAT,
             
        )
    """)
 
    # Insert user data into the table
    for sales in data:
        conn.execute("""
            INSERT INTO sales (
                 name, owner, workflow_state,
                customer, customer_name, order_type, company,
                total_qty, base_total, customer_group,territory,
                item_name,item_owner,payment_name,payment_amount,payment_outstanding
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)
        """, (
            sales['name'], sales['owner'], sales.get('workflow_state', None),sales['customer'],
            sales['customer_name'], sales['order_type'], sales['company'], sales['total_qty'],
            sales['base_total'], sales['customer_group'], sales['territory'],  sales['items'][0].get('name', None),  # Safely access 'name' of the first item
            sales['items'][0].get('owner', None),  # Safely access 'owner' of the first item
            sales['payment_schedule'][0].get('name', None),  # Safely access 'name' in payment schedule
            sales['payment_schedule'][0].get('payment_amount', None),  # Safely access 'payment_amount'
            sales['payment_schedule'][0].get('outstanding', None)  
        ))
 
    conn.close()