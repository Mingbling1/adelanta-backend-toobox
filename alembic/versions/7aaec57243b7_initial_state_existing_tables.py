"""Initial state - existing tables

Revision ID: 7aaec57243b7
Revises: 0d5e35d11f2d
Create Date: 2025-06-20 09:51:39.796760

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7aaec57243b7'
down_revision: Union[str, None] = '0d5e35d11f2d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('CXCAcumuladoDim', sa.Column('SaldoTotal', sa.Numeric(precision=18, scale=2), nullable=False))
    op.add_column('CXCAcumuladoDim', sa.Column('SaldoTotalPen', sa.Numeric(precision=18, scale=2), nullable=False))
    op.add_column('CXCAcumuladoDim', sa.Column('TipoPagoReal', sa.String(length=50), nullable=False))
    op.add_column('CXCAcumuladoDim', sa.Column('EstadoCuenta', sa.String(length=20), nullable=False))
    op.add_column('CXCAcumuladoDim', sa.Column('EstadoReal', sa.String(length=50), nullable=False))
    op.add_column('CXCAcumuladoDim', sa.Column('Sector', sa.String(length=200), nullable=True))
    op.add_column('CXCAcumuladoDim', sa.Column('GrupoEco', sa.String(length=200), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('CXCAcumuladoDim', 'GrupoEco')
    op.drop_column('CXCAcumuladoDim', 'Sector')
    op.drop_column('CXCAcumuladoDim', 'EstadoReal')
    op.drop_column('CXCAcumuladoDim', 'EstadoCuenta')
    op.drop_column('CXCAcumuladoDim', 'TipoPagoReal')
    op.drop_column('CXCAcumuladoDim', 'SaldoTotalPen')
    op.drop_column('CXCAcumuladoDim', 'SaldoTotal')
    # ### end Alembic commands ###
