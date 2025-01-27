from airflow import DAG
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

    final_details = []
    for sales_order_name in sales_order:
        # Fetch all fields of the sales order
        details_of_sales_order = erp.get_doc_sales(doctype, sales_order_name)
        final_details.append(details_of_sales_order)

    # Write all data to a JSON file
    with open('/opt/airflow/dags/json_files/final_details.json', 'w') as salesorder_details:
        json.dump(final_details, salesorder_details, indent=4)
    
    print(f"All sales order data saved. Total records: {len(final_details)}")

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

    # Task execution order (no dependencies, just run the ingestion task)
    ingest_sales_order_task
