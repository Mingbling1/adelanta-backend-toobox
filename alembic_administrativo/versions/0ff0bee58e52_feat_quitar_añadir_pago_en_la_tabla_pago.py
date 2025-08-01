"""feat: quitar añadir pago en la tabla pago

Revision ID: 0ff0bee58e52
Revises: 00545abfdcab
Create Date: 2024-11-05 08:08:57.565357

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '0ff0bee58e52'
down_revision: Union[str, None] = '00545abfdcab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('pago', 'pago_estado')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('pago', sa.Column('pago_estado', mysql.VARCHAR(length=255), nullable=False))
    # ### end Alembic commands ###
