"""
Сервисный процесс для периодической очистки логов и содержимого df.op_data:
 maxLogAgeInDays/maxProcLogAgeInDays - период очистки,
 opDataRetentionInDays - период очистки таблицы df.op_data.
airflow trigger_dag --conf '{"maxLogAgeInDays":15, "maxProcLogAgeInDays":7, "opDataRetentionInDays":15}' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
    maxProcLogAgeInDays:<INT> - Optional
    opDataRetentionInDays:<INT> - Optional
"""
import os
import logging

from os import getenv
from datetime import datetime
from docker.types import Mount
from airflow.models import DAG, Variable
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

DAG_ID = 'df_airflow_log_cleanup'
BASE_LOG_FOLDER = conf.get('logging', 'BASE_LOG_FOLDER')
# глобальный переключатель - удалять логи старше периода хранения или нет
ENABLE_DELETE = True
# точка отсчета периода выполнения
START_DATE = datetime(2019, 8, 1)
# период запуска, например. @daily, @weekly, @monthly
SCHEDULE_INTERVAL = '@daily'
# значение столбца Owner в вебе
DAG_OWNER_NAME = 'admin'
# перечень email получателей падений DAG'а
ALERT_EMAIL_ADDRESSES = []
# период хранения логов, если не задан в файле конфигурации
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get('max_log_age_in_days', 15)
# период хранения логов менеджера процессов, если не задан в файле конфигурации
DEFAULT_MAX_PROC_LOG_AGE_IN_DAYS = Variable.get('max_proc_log_age_in_days', 7)
# количество узлов с Airflow worker'ами, для очистки логов на каждом
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
ENABLE_DELETE_CHILD_LOG = Variable.get('enable_delete_child_log', 'False')

# период хранения файлов, загруженных в df.op_data
DEFAULT_OP_DATA_RETENTION_IN_DAYS = Variable.get('op_data_retention_in_days', 15)

if ENABLE_DELETE_CHILD_LOG.lower() == 'true':
    logging.info('DELETE_CHILD_LOG enabled')
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY')
        if CHILD_PROCESS_LOG_DIRECTORY != ' ':
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception:
        logging.exception('Не задан параметр CHILD_PROCESS_LOG_DIRECTORY в конфигурации Airflow.')

try:
    dpm_log_folder = os.path.dirname(os.path.abspath(conf.get('logging', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')))
    DIRECTORIES_TO_DELETE.append(
        {'directory': dpm_log_folder, 'log_mask': '/*', 'log_age': DEFAULT_MAX_PROC_LOG_AGE_IN_DAYS})
except OSError as e:
    logging.exception(f'Не задан параметр DAG_PROCESSOR_MANAGER_LOG_LOCATION в конфигурации Airflow:\n{e}')

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

log_cleanup = """
echo "Getting Configurations..."
BASE_LOG_FOLDER="{{params.directory}}"
LOG_MASK="{{params.log_mask}}"
TYPE="{{params.type}}"
MAX_LOG_AGE_IN_DAYS='{{params.log_age}}'
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "LOG_MASK:             '${LOG_MASK}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
echo "TYPE:                 '${TYPE}'"

echo ""
echo "Running Cleanup Process..."
if [ $TYPE == file ];
then
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}${LOG_MASK} -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
else
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}${LOG_MASK} -type d -empty "
fi
echo "Executing Find Statement: ${FIND_STATEMENT}"
FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
echo "Process will be Deleting the following File/directory:"
echo "${FILES_MARKED_FOR_DELETE}"
echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l ` file/directory(s)"
echo ""
if [ "${ENABLE_DELETE}" == "true" ];
then
    DELETE_STMT="${FIND_STATEMENT} -delete"
    echo "Executing Delete Statement: ${DELETE_STMT}"
    eval ${DELETE_STMT}
    DELETE_STMT_EXIT_CODE=$?
    if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
        echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
        exit ${DELETE_STMT_EXIT_CODE}
    fi
else
    echo "WARN: You're opted to skip deleting the file(s)/directory(s)!!!"
fi
echo "Finished Running Cleanup Process"
"""
i = 0
for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for directory in DIRECTORIES_TO_DELETE:
        if isinstance(directory, dict):
            log_dir = str(directory['directory'])
            log_mask = str(directory['log_mask'])
            log_age = str(directory['log_age'])
        else:
            log_dir = str(directory)
            log_mask = '/*/*'
            log_age = str(DEFAULT_MAX_LOG_AGE_IN_DAYS)

        log_cleanup_file_op = BashOperator(
            task_id='log_cleanup_file_' + str(i),
            bash_command=log_cleanup,
            params={'type': 'file', 'directory': log_dir, 'log_mask': log_mask, 'log_age': log_age},
            dag=dag)

        log_cleanup_dir_op = BashOperator(
            task_id='log_cleanup_directory_' + str(i),
            bash_command=log_cleanup,
            params={'type': 'directory', 'directory': log_dir, 'log_mask': log_mask, 'log_age': log_age},
            dag=dag)
        i = i + 1

        log_cleanup_file_op.set_downstream(log_cleanup_dir_op)

clear_op_data = DockerOperator(
    task_id='clear_op_data',
    image='df_operator:latest',
    api_version='auto',
    auto_remove=True,
    environment={**environment,
                 **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/clear_op_data.sql',
                    'AF_DEFAULT_OP_DATA_RETENTION_IN_DAYS': DEFAULT_OP_DATA_RETENTION_IN_DAYS}
                 },
    mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
    working_dir='/app',
    command='python /app/ws/source/df/sql/run_statement.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    dag=dag
)
clear_op_data