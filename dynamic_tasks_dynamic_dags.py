from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import datetime

task_schedule_input = {
    'task1': {'schedule': '*/2 * * * *', 'first_input': 1, 'second_input': 2},
    'task2': {'schedule':'0 * * * *', 'first_input': 3, 'second_input':  4},
    'task3': {'schedule':'*/5 * * * *', 'first_input': 5, 'second_input': 6}
}

default_args = {
    'start_date': datetime.datetime(2023, 6, 12),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def print_data(start, end):
    print('now', datetime.datetime.now())
    print('start', start)
    print('end', end)


def create_dynamic_tasks(task_ids, op_args):
    tasks = []
    for task_id, op_arg in zip(task_ids, op_args):
        task = PythonOperator(
            task_id=task_id, python_callable=print_data, op_kwargs=op_arg
        )
        tasks.append(task)
    return tasks


def create_dag(dag_id, schedule, task_array, default_args, op_args):
    dag = DAG(dag_id, schedule_interval=schedule, start_date=datetime.datetime.now(),
    catchup=False, default_args=default_args)
    with dag:
        tasks = create_dynamic_tasks(task_array, op_args)
        for task in tasks:
            task.dag = dag

        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]
    return dag


for task_name, item in task_schedule_input.items():
    dag_id = "counter_{}".format(str(task_name))

    default_args = {"owner": "admin", "start_date": datetime.datetime(2021, 1, 1), "catchup": False}

    schedule = item['schedule']

    task_array = []
    op_args = []

    for q in ['a', 'b', 'c', 'd']:
        op_arg = {'start': item['first_input'], 'end': item['second_input']}
        task_array.append(f'counter_{q}')
        op_args.append(op_arg)
        print(op_arg)

    globals()[dag_id] = create_dag(dag_id, schedule, task_array, default_args, op_args)

