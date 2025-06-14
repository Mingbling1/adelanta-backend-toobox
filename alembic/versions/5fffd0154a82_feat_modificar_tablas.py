"""feat: modificar tablas

Revision ID: 5fffd0154a82
Revises: 7371459965cf
Create Date: 2024-09-24 18:23:31.855168

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '5fffd0154a82'
down_revision: Union[str, None] = '7371459965cf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nuevos_clientes_nuevos_pagadores', sa.Column('FechaOperacion', sa.DateTime(), nullable=True))
    op.drop_column('nuevos_clientes_nuevos_pagadores', 'Mes')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('nuevos_clientes_nuevos_pagadores', sa.Column('Mes', mysql.VARCHAR(length=255), nullable=True))
    op.drop_column('nuevos_clientes_nuevos_pagadores', 'FechaOperacion')
    # ### end Alembic commands ###
