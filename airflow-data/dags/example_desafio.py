from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['henrique.rios@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Task 1: Lê a tabela 'Order' e exporta para 'output_orders.csv'
def export_orders_to_csv():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM 'Order'")
    orders = cursor.fetchall()
    
    with open('output_orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([i[0] for i in cursor.description])  # Escreve o cabeçalho
        writer.writerows(orders)
    
    conn.close()

# Task 2: Lê a tabela 'OrderDetail', faz join com 'output_orders.csv' e salva 'count.txt'
def count_orders_for_rio():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    cursor = conn.cursor()
    cursor.execute("""
        SELECT od.Quantity 
        FROM OrderDetail od 
        JOIN 'Order' o ON od.OrderId = o.Id 
        WHERE o.ShipCity = 'Rio de Janeiro'
    """)
    quantities = cursor.fetchall()
    
    total_quantity = sum([q[0] for q in quantities])

    with open('count.txt', 'w') as f:
        f.write(str(total_quantity))
    
    conn.close()

# Task final: Gera o arquivo final_output.txt
def export_final_answer():
    import base64

    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt", "w") as f:
        f.write(base64_message)
    return None

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    # Task 1: Exportar pedidos para CSV
    task1 = PythonOperator(
        task_id='export_orders_to_csv',
        python_callable=export_orders_to_csv
    )

    # Task 2: Fazer join e contar pedidos para Rio de Janeiro
    task2 = PythonOperator(
        task_id='count_orders_for_rio',
        python_callable=count_orders_for_rio
    )

    # Task Final: Gera o arquivo final codificado
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer
    )

    # Definir a ordem de execução das tasks
    task1 >> task2 >> export_final_output
