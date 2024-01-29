"""
Сервисный процесс для периодической очистки содержимого df.op_data с vacuum:
  opDataRetentionInDays - период очистки таблицы df.op_data.
airflow trigger_dag --conf '{"opDataRetentionInDays":15}' df_op_data_cleanup
--conf options:
    opDataRetentionInDays:<INT> - Optional
"""
from os import getenv
from datetime import datetime
from docker.types import Mount
from airflow.models import DAG, Variable
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

DAG_ID = 'df_op_data_cleanup'
# точка отсчета периода выполнения
START_DATE = datetime(2023, 3, 1)
# период запуска, например. @daily, @weekly, @monthly
SCHEDULE_INTERVAL = "0 0 * * 6"
# значение столбца Owner в вебе
DAG_OWNER_NAME = 'admin'
# перечень email получателей падений DAG'а
ALERT_EMAIL_ADDRESSES = []

# период хранения файлов, загруженных в df.op_data
DEFAULT_OP_DATA_RETENTION_IN_DAYS = Variable.get('op_data_retention_in_days', 15)

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('df'),
               'AF_DWH_DB_SCHEMA': getenv('DATAFLOW_SCHEMA')
               }

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval=SCHEDULE_INTERVAL,
          start_date=START_DATE,
          catchup=False,
          tags=['system'])
dag.doc_md = __doc__

clear_op_data = DockerOperator(
    task_id='clear_op_data',
    image='df_operator:latest',
    api_version='auto',
    auto_remove=True,
    environment={**environment,
                 **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/clear_op_data.sql',
                    'AF_DEFAULT_OP_DATA_RETENTION_IN_DAYS': DEFAULT_OP_DATA_RETENTION_IN_DAYS,
                    'AF_SINGLETRAN': True,
                    'AF_ISOLATION_LEVEL': 'AUTOCOMMIT',
                    'AF_RUN_VACUUM': True}
                 },
    mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
    working_dir='/app',
    command='python /app/ws/source/df/sql/run_statement.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    dag=dag
)
clear_op_data