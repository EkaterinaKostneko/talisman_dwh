"""
Пример передачи значения из DockerOperatora в XCom:
 - значение кладется в скрипте `/app/ws/source/df/python/scripts/examples/print_value.pyj` с помощью функции
`context_push(key: Optional[str] = None, value: Any)`;
 - для получения внутри DockerOperator используется
`context_pull(key: Optional[str] = None, default: Optional[Any] = None) -> Any`;
Если key не передан, используется значение os.getenv('AF_TASK_ID')
 - cкрипт выполняется внутри DockerOperator системным раннером `/app/ws/source/df/python/run.py`
 - передача значения между тасками реализована с помощью переменной окружения AF_VALUE и `ti.xcom_pull`.
Функционал необходим для разграничения отладочных сообщений контейнера и значений, которые необходимо вернуть
из DockerOperator
"""
from datetime import datetime

from airflow.models import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'description': 'Example Docker XCom Context',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_example_docker_xcom_context'
push_task_id = f'{dag_id}.push_value'  # таск, добавляющий переменную в XCom
update_task_id = f'{dag_id}.update_value'  # таск, обновляющий добавленную прееменную в XCom
py_script = '/app/ws/source/df/python/scripts/examples/print_value.pyj'  # скрипт использующий переменную в разных тасках

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_DAG_ID': dag_id,  # обязательно наличие в env, для загрузки метаинформации о выполняемых задачах
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_JOB_ID': '{{ ti.job_id }}',
               'AF_LOGLEVEL': 'debug'
               }


def run_py(task_id, script_path, params={}):
    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': script_path},
                     **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )


def run_bash(task_id, cmd=None, params={}):
    if cmd:
        command = f'/bin/bash -c \'{cmd}\''
    elif 'AF_SCRIPT_PATH' in params:
        command = '/bin/bash -c \'$AF_SCRIPT_PATH\' '
    else:
        raise AssertionError(
            f'Должно быть передано cmd или $AF_SCRIPT_PATH'
        )

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command=command,
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )


def run_fake(task_id):
    return DummyOperator(task_id=task_id)


with DAG(dag_id,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t_start = run_fake(f'{dag_id}.start')
    t_push_value = run_py(f'{dag_id}.push_value', py_script, params={'AF_ACTION': 'push'})
    t_update_value = run_py(f'{dag_id}.update_value', py_script,
                            params={'AF_ACTION': 'pull',
                                    'AF_VALUE': f'{{{{ ti.xcom_pull(task_ids="{push_task_id}") '
                                                f'if ti.xcom_pull(task_ids="{push_task_id}") else "" }}}}'})
    t_print_value = run_bash(f'{dag_id}.print_value', 'echo $AF_VALUE',
                             params={'AF_VALUE': f'{{{{ ti.xcom_pull(task_ids="{update_task_id}") '
                                                 f'if ti.xcom_pull(task_ids="{update_task_id}") else "" }}}}'})
    t_end = run_fake(f'{dag_id}.end')

    t_start >> t_push_value >> t_update_value >> t_print_value >> t_end

    dag.doc_md = __doc__
