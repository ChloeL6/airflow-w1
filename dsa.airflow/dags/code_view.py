from datetime import timedelta, datetime
import random

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago # handy scheduling tool


APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
    with open('/opt/airflow/dags/code_review.txt') as file:
        my_name = file.read()
        print(f'Hello {my_name}!')

def pick_apple():
    apple = random.choice(APPLES)
    print(apple)


# We can pass in DAG arguments using a default args dict. All these could be passed directly to the DAG as well.
default_args = {
    'start_date': days_ago(2), # The start date for DAG running. This function allows us to set the start date to two days ago
    'schedule_interval': timedelta(days=1), # How often our DAG will run. After the start_date, airflow waits for the schedule_interval to pass then triggers the DAG run
    'retries': 1, # How many times to retry in case of failure
    'retry_delay': timedelta(minutes=2), # How long to wait before retrying
}

# instantiate a DAG!
with DAG(
    'pick_apples',
    description='A DAG to pick apples',
    default_args=default_args,
) as dag:
    
    echo_to_file = BashOperator(
        task_id='echo_to_file',
        bash_command='echo Chloe Le >> /opt/airflow/dags/code_review.txt'
    )

    greeting = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )

    print_words = BashOperator(
        task_id="echo_words",
        bash_command='echo picking three random apples'
    )

    first_apple = PythonOperator(
        task_id="first_apple",
        python_callable=pick_apple
    )

    second_apple = PythonOperator(
        task_id="second_apple",
        python_callable=pick_apple
    )

    third_apple = PythonOperator(
        task_id="third_apple",
        python_callable=pick_apple
    )

    end = DummyOperator(
        task_id='end'
    )

    # set the task order
    echo_to_file >> greeting >> print_words >> [first_apple, second_apple, third_apple] >> end