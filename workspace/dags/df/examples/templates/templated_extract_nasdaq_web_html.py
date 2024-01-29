from datetime import datetime, timedelta

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash_operator import BashOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport


def get_days(date_begin, date_end):
    if not date_begin or not date_end:
        return [date_begin or date_end]

    data_format = '%Y-%m-%d'
    start_date = datetime.strptime(date_begin, data_format)
    end_date = datetime.strptime(date_end, data_format)
    return [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range((end_date - start_date).days + 1)]


default_args = {
    'owner': 'airflow',
    'description': 'Nasdaqhtml Extractor',
    'depend_on_past': False,
    'start_date': datetime(2020, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql'),
               'AF_LOGLEVEL': 'debug'
               }

extractors = []
dag_id = 'df_example_templated_extract_nasdaq_web_html'
check_task = f'{dag_id}.check_task'
bash_task = f'{dag_id}.bash_task'
finish_task_id = f'{dag_id}.finish_task'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    def _get_task(_day, purge_existing_date=False):
        return DockerOperator(
            task_id=f'{dag_id}.extract_{_day}',
            image='df_operator:latest',
            api_version='auto',
            auto_remove=True,
            environment={**environment,
                         **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('http_nasdaqhtml_mocker_local'),
                            'AF_PRODUCER': '/app/ws/metadata/df/models/examples/nasdaqhtml/source_nasdaq_web_html.json',
                            'AF_CONSUMER': '/app/ws/metadata/df/models/examples/nasdaqhtml/raw_nasdaq_web_html.json',
                            'AF_DATE_BEGIN_VARIABLE': _day,
                            'AF_DATE_END_VARIABLE': _day,
                            'AF_PURGE_EXISTING_DATA': 'true' if purge_existing_date else 'false'
                            }
                         },
            mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
            working_dir='/app',
            command='python /app/ws/source/df/excel/run.py',
            docker_url='unix://var/run/docker.sock',
            network_mode='host'
        )


    bash_task = BashOperator(
        task_id=bash_task,
        bash_command='echo "Here is the trigger params: $env1 and $env2"',
        env={'env1': '<{ env1 }>', 'env2': '<{ env2 }>', 'env3': '<{ ds }>'}
    )

    days = get_days('<{ dateFrom }>', '<{ dateTo }>')
    initial_extractor = _get_task(days.pop(0), purge_existing_date=True)
    for day in days:
        extractors.append(_get_task(day))

    finish_task = BashOperator(
        task_id=finish_task_id,
        bash_command='echo "DAG finished"'
    )

    bash_task >> initial_extractor >> extractors >> finish_task
