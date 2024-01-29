"""1 initial structure (#405)

Revision ID: 2629d56a23e3
Revises: aee52ef29eed
Create Date: 2022-02-24 10:02:41.112198

"""
import os
import site
import sqlalchemy as sa

# no cwd in sys.path, fixing
site.addsitedir(os.path.abspath('../../../'))

from alembic import op
from df.common.exceptions import DFException  # @UnresolvedImport
from df.common.helpers.sql import UTCNow, UTCDate  # @UnresolvedImport
from df.common.constants import DEFAULT_SCHEMA_ENV  # @UnresolvedImport
from df.common.helpers.general import get_db_dialect, get_clean_schema, get_df_schema  # @UnresolvedImport
from df.common.helpers.sql import drop_table, drop_function  # @UnresolvedImport

# revision identifiers, used by Alembic.
revision = '2629d56a23e3'
down_revision = 'aee52ef29eed'
branch_labels = None
depends_on = None
schema = get_df_schema()
schema_old = get_clean_schema(DEFAULT_SCHEMA_ENV)
drop_alembic_version = True
if schema == schema_old:
    drop_alembic_version = False
    schema_old = ''

dialect = get_db_dialect()
if dialect != 'postgresql':
    raise DFException(f'СУБД {dialect} не может быть использована для системных объектов')


def upgrade():
    if schema_old:
        drop_function('r5_set_rule_run_state', schema_old)
        drop_function('r5_add_rule_run', schema_old)
        drop_function('r5_json_data', schema_old)
        drop_function('r5_json_skip', schema_old)
        drop_function('r5_json_string', schema_old)
        drop_function('r5_json_skip_until', schema_old)
        drop_function('r5_json_object', schema_old)
        drop_function('r5_json_number', schema_old)
        drop_function('r5_json_name', schema_old)
        drop_function('r5_json_natural', schema_old)
        drop_function('r5_json_message', schema_old)
        drop_function('r5_json_item', schema_old)
        drop_function('r5_json_array', schema_old)
        drop_function('r5_json_parse', schema_old)
        drop_function('r5_json_to_json', schema_old)
        drop_function('r5_json_value', schema_old)
        drop_function('r5_json_value2', schema_old)
        drop_function('r5_json_parse_pf', schema_old)
        drop_function('r5_try_convert_int', schema_old)
        drop_function('r5_try_convert_float', schema_old)
        drop_function('r5_try_convert_dtm', schema_old)
        drop_function('r5_try_convert_dt', schema_old)
        drop_function('r5_try_convert_dt_ymd', schema_old)
        drop_function('r5_try_convert_dec', schema_old)
        drop_function('r5_try_cast_int', schema_old)
        drop_function('r5_try_cast_float', schema_old)
        drop_function('r5_try_cast_dtm', schema_old)
        drop_function('r5_try_cast_dt', schema_old)
        drop_function('r5_try_cast_dec', schema_old)
        drop_function('r5_trim', schema_old)
        drop_function('r5_trim_right', schema_old)
        drop_function('r5_trim_left', schema_old)
        drop_function('r5_to_log', schema_old)
        drop_function('r5_split_string', schema_old)
        drop_function('r5_split_string_ord', schema_old)
        drop_function('r5_split_part_lab', schema_old)
        drop_function('r5_split_part', schema_old)
        drop_function('r5_check_format', schema_old)
        drop_function('r5_check_dependencies', schema_old)

        drop_table('r5_entity_relation_ctx', schema_old)
        drop_table('r5_entity_ctx', schema_old)
        drop_table('r5_load_run', schema_old)
        drop_table('r5_load_run_data', schema_old)
        drop_table('r5_error', schema_old)
        drop_table('r5_rule_check', schema_old)
        drop_table('r5_rule_set', schema_old)
        drop_table('r5_rule_run', schema_old)
        drop_table('r5_rule', schema_old)
        drop_table('r5_value_type', schema_old)
        drop_table('r5_rule_type', schema_old)
        drop_table('r5_rule_action', schema_old)
        drop_table('r5_rule_severity', schema_old)
        drop_table('r5_app_log', schema_old)
        drop_table('r5_log_level', schema_old)
        drop_table('r5_module', schema_old)
        if drop_alembic_version:
            drop_table('alembic_version', schema_old)

    op.create_table(  # @UndefinedVariable
        'module',
        sa.Column('code', sa.VARCHAR(32), primary_key=True),
        sa.Column('name', sa.Unicode(128)),
        sa.Column('description', sa.Unicode(2048)),
        schema=schema
    )

    op.create_table(  # @UndefinedVariable
        'log_level',
        sa.Column('code', sa.VARCHAR(10), primary_key=True),
        sa.Column('name', sa.Unicode(64)),
        schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'app_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('source_id', sa.VARCHAR(128), nullable=False),
        sa.Column('run_id', sa.VARCHAR(128), nullable=False, index=True),
        sa.Column('job_id', sa.Integer),
        sa.Column('dt', sa.Date, server_default=UTCDate()),
        sa.Column('ts', sa.DateTime, server_default=UTCNow()),
        sa.Column('level', sa.VARCHAR(10), nullable=False, index=True),
        sa.Column('caller', sa.VARCHAR(128)),
        sa.Column('message', sa.Unicode(256)),
        sa.Column('description', sa.Unicode(1024)),
        sa.Column('type', sa.Unicode(32)),
        sa.Column('value', sa.Unicode(256)),
        sa.Column('data', sa.UnicodeText),
        schema=schema
    )
    op.create_foreign_key('fk_applog_level',  # @UndefinedVariable
                          'app_log',
                          'log_level',
                          ['level'],
                          ['code'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'load_run',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('dag_id', sa.VARCHAR(128), nullable=False),
        sa.Column('source_id', sa.VARCHAR(128), nullable=False),
        sa.Column('run_id', sa.VARCHAR(128), nullable=False, index=True),
        sa.Column('unit_id', sa.VARCHAR(128)),
        sa.Column('date_begin', sa.Date),
        sa.Column('date_end', sa.Date),
        sa.Column('dt', sa.Date, server_default=UTCDate()),
        sa.Column('ts', sa.DateTime, server_default=UTCNow()),
        schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'entity_ctx',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('job_id', sa.Integer, nullable=False, index=True),
        sa.Column('table_name', sa.VARCHAR(128), nullable=False),
        sa.Column('table_role', sa.VARCHAR(32), nullable=False),
        sa.Column('col_name', sa.VARCHAR(128), nullable=False),
        sa.Column('col_type', sa.VARCHAR(32), nullable=False),
        sa.Column('col_pk', sa.Boolean),
        sa.Column('col_lk', sa.Boolean),
        sa.Column('col_length', sa.VARCHAR(32)),
        sa.Column('col_unique', sa.Boolean),
        sa.Column('col_nullable', sa.Boolean),
        sa.Column('col_format', sa.Unicode(1024)),
        sa.Column('col_role', sa.VARCHAR(32)),
        sa.Column('col_default', sa.Unicode),
        schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'entity_relation_ctx',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('job_id', sa.Integer, nullable=False, index=True),
        sa.Column('from_entity', sa.VARCHAR(128), nullable=False),
        sa.Column('from_column', sa.VARCHAR(128), nullable=False),
        sa.Column('to_entity', sa.VARCHAR(128), nullable=False),
        sa.Column('to_column', sa.VARCHAR(128), nullable=False),
        schema=schema
    )


def downgrade():
    drop_table('entity_relation_ctx', schema=schema)  # @UndefinedVariable
    drop_table('entity_ctx', schema=schema)  # @UndefinedVariable
    drop_table('load_run_data', schema=schema)  # @UndefinedVariable
    drop_table('load_run', schema=schema)  # @UndefinedVariable
    drop_table('error', schema=schema)  # @UndefinedVariable
    drop_table('rule_check', schema=schema)  # @UndefinedVariable
    drop_table('rule_set', schema=schema)  # @UndefinedVariable
    drop_table('rule_run', schema=schema)  # @UndefinedVariable
    drop_table('rule', schema=schema)  # @UndefinedVariable
    drop_table('value_type', schema=schema)  # @UndefinedVariable
    drop_table('rule_type', schema=schema)  # @UndefinedVariable
    drop_table('rule_action', schema=schema)  # @UndefinedVariable
    drop_table('rule_severity', schema=schema)  # @UndefinedVariable
    drop_table('app_log', schema=schema)  # @UndefinedVariable
    drop_table('log_level', schema=schema)  # @UndefinedVariable
    drop_table('module', schema=schema)  # @UndefinedVariable

