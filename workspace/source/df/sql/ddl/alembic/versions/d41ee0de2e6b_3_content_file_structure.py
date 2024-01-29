"""3 content_file_structure

Revision ID: d41ee0de2e6b
Revises: ab66b5bd16a9
Create Date: 2022-12-13 14:12:37.088636

"""
import os
import site
import sqlalchemy as sa

# no cwd in sys.path, fixing
site.addsitedir(os.path.abspath('../../../'))

from alembic import op
from sqlalchemy.dialects.postgresql import JSON, UUID

from df.common.exceptions import DFException  # @UnresolvedImport
from df.common.helpers.sql import UTCNow  # @UnresolvedImport
from df.common.helpers.general import get_db_dialect, get_df_schema  # @UnresolvedImport
from df.common.helpers.sql import drop_table, XMLType  # @UnresolvedImport

# revision identifiers, used by Alembic.
revision = 'd41ee0de2e6b'
down_revision = 'ab66b5bd16a9'
branch_labels = None
depends_on = None

schema = get_df_schema()
dialect = get_db_dialect()
if dialect != 'postgresql':
    raise DFException(f'СУБД {dialect} не может быть использована для системных объектов')


def upgrade():
    op.create_table(  # @UndefinedVariable
        'op_data',
        sa.Column('id', sa.BigInteger, primary_key=True),
        sa.Column('op_ins_id', sa.BigInteger, nullable=False),
        sa.Column('created', sa.DateTime, server_default=UTCNow()),
        sa.Column('expiration', sa.DateTime),
        sa.Column('meta', JSON),
        sa.Column('value', sa.UnicodeText),
        sa.Column('bvalue', sa.Binary),
        sa.Column('jvalue', sa.JSON),
        sa.Column('xvalue', XMLType),
        schema=schema
    )
    op.create_foreign_key('fk_op_data_op_ins',  # @UndefinedVariable
                          'op_data',
                          'op_ins',
                          ['op_ins_id'],
                          ['id'],
                          ondelete='CASCADE',
                          source_schema=schema,
                          referent_schema=schema
                          )


def downgrade():
    drop_table('op_data', schema=schema)  # @UndefinedVariable
