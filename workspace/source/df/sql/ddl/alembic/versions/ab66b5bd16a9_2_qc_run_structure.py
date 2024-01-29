"""2 simplified structure

Revision ID: ab66b5bd16a9
Revises: 2629d56a23e3
Create Date: 2022-04-25 22:00:41.035264

"""
import os
import site
import sqlalchemy as sa

# no cwd in sys.path, fixing
site.addsitedir(os.path.abspath('../../../'))

from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSON, UUID
from df.common.exceptions import DFException  # @UnresolvedImport
from df.common.helpers.sql import UTCDate, UTCNow  # @UnresolvedImport
from df.common.helpers.general import get_db_dialect, get_df_schema  # @UnresolvedImport
from df.common.helpers.sql import drop_table, drop_function, drop_type  # @UnresolvedImport

# revision identifiers, used by Alembic.
revision = 'ab66b5bd16a9'
down_revision = '2629d56a23e3'
branch_labels = None
depends_on = None
schema=get_df_schema()

dialect = get_db_dialect()
if dialect != 'postgresql':
    raise DFException(f'СУБД {dialect} не может быть использована для системных объектов')


def upgrade():

    drop_function('set_rule_run_state', schema=schema)
    drop_function('add_rule_run', schema=schema)
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

    op.create_table(  # @UndefinedVariable
        'op',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('parent_id', sa.BigInteger),
        sa.Column('proc_id', sa.VARCHAR(128), nullable=False, index=True),
        sa.Column('run_id', sa.VARCHAR(128), index=True),
        sa.Column('ts', sa.DateTime, server_default=UTCNow()),
        sa.Column('module', sa.Enum('AIRFLOW', name='opmodule'), server_default='AIRFLOW'),
        sa.UniqueConstraint('proc_id', 'run_id'),
        schema=schema
    )
    op.create_foreign_key('fk_op_op',  # @UndefinedVariable
                          'op',
                          'op',
                          ['parent_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'op_ins',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_id', sa.BigInteger, nullable=False),
        sa.Column('step_id', sa.VARCHAR(128), nullable=False, index=True),
        sa.Column('job_id', sa.VARCHAR(128)),
        sa.Column('ts_start', sa.DateTime, server_default=UTCNow()),
        sa.Column('ts_finish', sa.DateTime),
        sa.UniqueConstraint('op_id', 'step_id', 'job_id'),
        schema=schema
    )
    op.create_foreign_key('fk_op_ins_op',  # @UndefinedVariable
                          'op_ins',
                          'op',
                          ['op_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'log',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_ins_id', sa.BigInteger, nullable=False),
        sa.Column('dt', sa.Date, server_default=UTCDate()),
        sa.Column('ts', sa.DateTime, server_default=UTCNow()),
        sa.Column('level', sa.Enum('INFO', name='loglevel'), server_default='INFO', nullable=False, index=True),
        sa.Column('caller', sa.VARCHAR(128)),
        sa.Column('message', sa.Unicode(256)),
        sa.Column('description', sa.Unicode(1024)),
        sa.Column('type', sa.Unicode(32)),
        sa.Column('value', sa.Unicode(256)),
        sa.Column('data', sa.UnicodeText),
        sa.Column('jdata', JSON),
        schema=schema
    )
    op.create_foreign_key('fk_log_op_ins',  # @UndefinedVariable
                          'log',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'op_ctx',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_id', sa.BigInteger, nullable=False),
        sa.Column('op_ins_id', sa.BigInteger),
        sa.Column('params', JSON),
        sa.UniqueConstraint('op_id', 'op_ins_id'),
        schema=schema
    )
    op.create_foreign_key('fk_op_ctx_op',  # @UndefinedVariable
                          'op_ctx',
                          'op',
                          ['op_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_foreign_key('fk_op_ctx_op_ins',  # @UndefinedVariable
                          'op_ctx',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'qc',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_ins_id', sa.BigInteger, nullable=False),
        sa.Column('name', sa.VARCHAR(256), nullable=False, index=True),
        sa.Column('controller', sa.VARCHAR(256)),
        sa.Column('unit', sa.VARCHAR(256)),
        schema=schema
    )
    op.create_foreign_key('fk_qc_op_ins',  # @UndefinedVariable
                          'qc',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'qc_error',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('qc_id', sa.BigInteger, nullable=False),
        sa.Column('partition', sa.Integer),
        sa.Column('row', sa.BigInteger),
        sa.Column('col', sa.VARCHAR(128)),
        sa.Column('jrow', JSON),
        sa.Column('jcol', JSON),
        sa.Column('details', JSON),
        schema=schema
    )
    op.create_foreign_key('fk_qc_error_qc',  # @UndefinedVariable
                          'qc_error',
                          'qc',
                          ['qc_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'entity_ctx',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=text('public.uuid_generate_v4()')),
        sa.Column('op_ins_id', sa.BigInteger, nullable=False, index=True),
        sa.Column('name', sa.VARCHAR(128), nullable=False),
        sa.Column('schema', sa.VARCHAR(64)),
        sa.Column('role', sa.VARCHAR(32), nullable=False),
        sa.UniqueConstraint('op_ins_id', 'name', 'schema'),
        schema=schema
    )
    op.create_foreign_key('fk_entity_ctx_op_ins',  # @UndefinedVariable
                          'entity_ctx',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'attr_ctx',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('entity_ctx_id', UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.VARCHAR(128), nullable=False),
        sa.Column('ord', sa.Integer, nullable=False),
        sa.Column('type', sa.VARCHAR(32), nullable=False),
        sa.Column('pk', sa.Boolean),
        sa.Column('lk', sa.Boolean),
        sa.Column('length', sa.VARCHAR(32)),
        sa.Column('unique', sa.Boolean),
        sa.Column('nullable', sa.Boolean),
        sa.Column('format', sa.Unicode(1024)),
        sa.Column('role', sa.VARCHAR(32)),
        sa.Column('default', sa.Unicode(512)),
        sa.UniqueConstraint('entity_ctx_id', 'name'),
        schema=schema
    )
    op.create_foreign_key('fk_attr_ctx_entity_ctx',  # @UndefinedVariable
                          'attr_ctx',
                          'entity_ctx',
                          ['entity_ctx_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )
    op.create_table(  # @UndefinedVariable
        'relation_ctx',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_ins_id', sa.BigInteger, nullable=False, index=True),
        sa.Column('from_entity', sa.VARCHAR(128), nullable=False),
        sa.Column('from_column', sa.VARCHAR(128), nullable=False),
        sa.Column('to_entity', sa.VARCHAR(128), nullable=False),
        sa.Column('to_column', sa.VARCHAR(128), nullable=False),
        sa.UniqueConstraint('op_ins_id',
                            'from_entity',
                            'from_column',
                            'to_entity',
                            'to_column'),
        schema=schema
    )
    op.create_foreign_key('fk_relation_ctx_op_ins',  # @UndefinedVariable
                          'relation_ctx',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
    )


def downgrade():
    drop_table('relation_ctx', schema=schema)  # @UndefinedVariable
    drop_table('attr_ctx', schema=schema)  # @UndefinedVariable
    drop_table('entity_ctx', schema=schema)  # @UndefinedVariable
    drop_table('log', schema=schema)  # @UndefinedVariable
    drop_table('qc_error', schema=schema)  # @UndefinedVariable
    drop_table('qc', schema=schema)  # @UndefinedVariable
    drop_table('op_ctx', schema=schema)  # @UndefinedVariable
    drop_table('op_ins', schema=schema)  # @UndefinedVariable
    drop_table('op', schema=schema)  # @UndefinedVariable
    drop_type('opmodule', schema=schema)  # @UndefinedVariable
    drop_type('loglevel', schema=schema)  # @UndefinedVariable

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
