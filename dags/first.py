from airflow import DAG
import duckdb
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import connect1
import json

# Function to fetch sales order data and save it as a JSON file
def fetch_and_save_sales_order_data():
    connection = connect1.Connect()
    doctype = 'Sales Order'
    erp = connect1.API()

    # Get sales_order_name/id
    sales_order_id = erp.get_dataframe(doctype)
    sales_order = []
    for i in sales_order_id.get('name'):
        sales_order.append(i)
    sales_order = sales_order[:10]

    final_details = []
    for sales_order_name in sales_order:
        # Fetch all fields of the sales order
        details_of_sales_order = erp.get_doc_sales(doctype, sales_order_name)
        final_details.append(details_of_sales_order)

    # Write all data to a JSON file
    with open('/opt/airflow/dags/json_files/final_details.json', 'w') as salesorder_details:
        json.dump(final_details, salesorder_details, indent=4)
    
    print(f"All sales order data saved. Total records: {len(final_details)}")




#######################################################################################
def savesales_data_to_duckdb():
    DUCKDB_PATH = "/opt/airflow/dags/sales_database1.duckdb"
    # Connect to DuckDB (this will create the database file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_PATH)
    #file_path = "/opt/airflow/dags/json_files/final_details.json"
 
    # Read the data from the JSON file
    with open('/opt/airflow/dags/json_files/final_details.json', 'r') as f:
        data = json.load(f)
        print('datas are',data)
 
    # Create the table if it doesn't exist (adjust schema according to the API data)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales (
           
            name TEXT,
            owner TEXT,
            creation TIMESTAMP,
            modified TIMESTAMP,
            modified_by TEXT,
            docstatus INTEGER,
            idx INTEGER,
            workflow_state TEXT,
            title TEXT,
            naming_series TEXT,
            customer TEXT,
            customer_name TEXT,
            order_type TEXT,
            transaction_date DATE,
            delivery_date DATE,
            custom_shipping_date DATE,
            company TEXT,
            skip_delivery_note INTEGER,
            currency TEXT,
            conversion_rate FLOAT,
            selling_price_list TEXT,
            price_list_currency TEXT,
            plc_conversion_rate FLOAT,
            ignore_pricing_rule INTEGER,
            reserve_stock INTEGER,
            total_qty FLOAT,
            total_net_weight FLOAT,
            custom_total_buying_price FLOAT,
            custom_total_margin_percentage FLOAT,
            base_total FLOAT,
            base_net_total FLOAT,
            custom_total_margin_amount FLOAT,
            total FLOAT,
            net_total FLOAT,
            tax_category TEXT,
            base_total_taxes_and_charges FLOAT,
            total_taxes_and_charges FLOAT,
            base_grand_total FLOAT,
            base_rounding_adjustment FLOAT,
            base_rounded_total FLOAT,
            base_in_words TEXT,
            grand_total FLOAT,
            rounding_adjustment FLOAT,
            rounded_total FLOAT,
            in_words TEXT,
            advance_paid FLOAT,
            disable_rounded_total INTEGER,
            apply_discount_on TEXT,
            base_discount_amount FLOAT,
            additional_discount_percentage FLOAT,
            discount_amount FLOAT,
            customer_address TEXT,
            address_display TEXT,
            customer_group TEXT,
            territory TEXT,
            shipping_address_name TEXT,
            shipping_address TEXT,
            status TEXT,
            delivery_status TEXT,
            per_delivered FLOAT,
            per_billed FLOAT,
            per_picked FLOAT,
            billing_status TEXT,
            amount_eligible_for_commission FLOAT,
            commission_rate FLOAT,
            total_commission FLOAT,
            loyalty_points INTEGER,
            loyalty_amount FLOAT,
            group_same_items INTEGER,
            language TEXT,
            is_internal_customer INTEGER,
            party_account_currency TEXT,
            doctype TEXT,
            taxes JSON,
            sales_team JSON,
            pricing_rules JSON,
            packed_items JSON,
            items JSON,
            payment_schedule JSON
            
            
            


            
             
        )
    """)
    tables=conn.execute("SHOW TABLES").fetchall()
    print('tables are ',tables)
    # Insert user data into the table
    for sales in data:
        conn.execute("""
            INSERT INTO sales (
                 name, owner, creation, modified, modified_by, docstatus, idx, workflow_state, title,
                naming_series, customer, customer_name, order_type, transaction_date, delivery_date, custom_shipping_date,
                company, skip_delivery_note, currency, conversion_rate, selling_price_list, price_list_currency,
                plc_conversion_rate, ignore_pricing_rule, reserve_stock, total_qty, total_net_weight, custom_total_buying_price,
                custom_total_margin_percentage, base_total, base_net_total, custom_total_margin_amount, total, net_total,
                tax_category, base_total_taxes_and_charges, total_taxes_and_charges, base_grand_total, base_rounding_adjustment,
                base_rounded_total, base_in_words, grand_total, rounding_adjustment, rounded_total, in_words, advance_paid,
                disable_rounded_total, apply_discount_on, base_discount_amount, additional_discount_percentage, discount_amount,
                customer_address, address_display, customer_group, territory, shipping_address_name, shipping_address,
                status, delivery_status, per_delivered, per_billed, per_picked, billing_status, amount_eligible_for_commission,
                commission_rate, total_commission, loyalty_points, loyalty_amount, group_same_items, language,
                is_internal_customer, party_account_currency, doctype, taxes, sales_team, pricing_rules,packed_items,items,payment_schedule
                     
                



            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                   
                    
                    
                    )
        """, (
            sales.get("name"), sales.get("owner"), sales.get("creation"), sales.get("modified"), sales.get("modified_by"),
            sales.get("docstatus"), sales.get("idx"), sales.get("workflow_state"), sales.get("title"), sales.get("naming_series"),
            sales.get("customer"), sales.get("customer_name"), sales.get("order_type"), sales.get("transaction_date"),
            sales.get("delivery_date"), sales.get("custom_shipping_date"), sales.get("company"), sales.get("skip_delivery_note"),
            sales.get("currency"), sales.get("conversion_rate"), sales.get("selling_price_list"), sales.get("price_list_currency"),
            sales.get("plc_conversion_rate"), sales.get("ignore_pricing_rule"), sales.get("reserve_stock"), sales.get("total_qty"),
            sales.get("total_net_weight"), sales.get("custom_total_buying_price"), sales.get("custom_total_margin_percentage"),
            sales.get("base_total"), sales.get("base_net_total"), sales.get("custom_total_margin_amount"), sales.get("total"),
            sales.get("net_total"), sales.get("tax_category"), sales.get("base_total_taxes_and_charges"), sales.get("total_taxes_and_charges"),
            sales.get("base_grand_total"), sales.get("base_rounding_adjustment"), sales.get("base_rounded_total"),
            sales.get("base_in_words"), sales.get("grand_total"), sales.get("rounding_adjustment"), sales.get("rounded_total"),
            sales.get("in_words"), sales.get("advance_paid"), sales.get("disable_rounded_total"), sales.get("apply_discount_on"),
            sales.get("base_discount_amount"), sales.get("additional_discount_percentage"), sales.get("discount_amount"),
            sales.get("customer_address"), sales.get("address_display"), sales.get("customer_group"), sales.get("territory"),
            sales.get("shipping_address_name"), sales.get("shipping_address"), sales.get("status"), sales.get("delivery_status"),
            sales.get("per_delivered"), sales.get("per_billed"), sales.get("per_picked"), sales.get("billing_status"),
            sales.get("amount_eligible_for_commission"), sales.get("commission_rate"), sales.get("total_commission"),
            sales.get("loyalty_points"), sales.get("loyalty_amount"), sales.get("group_same_items"), sales.get("language"),
            sales.get("is_internal_customer"), sales.get("party_account_currency"), sales.get("doctype"),
            json.dumps(sales.get("taxes")),json.dumps(sales.get('sales_team')),json.dumps(sales.get('pricing_rules')),json.dumps(sales.get('packed_items')),
            json.dumps(sales.get('items')),json.dumps(sales.get('payment_schedule'))

            


        ))
    result = conn.execute("SELECT * FROM sales").fetchall()
    print('results from first',result)
 
    conn.close()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 25),  # Set your start date
}

# Create the DAG instance
with DAG(
    'sales_order_ingestion',  # DAG name
    default_args=default_args,
    description='A DAG to fetch and save sales order data to JSON',
    schedule_interval='@daily',  # Set the schedule, e.g., once a day
    catchup=False,  # Skip missed schedules if DAG is not run on time
) as dag:

    # Define the PythonOperator that will run the fetch_and_save_sales_order_data function
    ingest_sales_order_task = PythonOperator(
        task_id='ingest_sales_order_data',  # Task name
        python_callable=fetch_and_save_sales_order_data,  # Function to run
    )

    store_duckdb = PythonOperator(
        task_id='store_duckdb',  
        python_callable=savesales_data_to_duckdb,  # Function to run
    )

    # Task execution order (no dependencies, just run the ingestion task)
    ingest_sales_order_task >> store_duckdb
