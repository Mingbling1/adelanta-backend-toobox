"""agregar_jerarquia_modulos_permisos

Revision ID: 2ec2c63e7781
Revises: fda7e53b939d
Create Date: 2025-05-10 19:48:00.934653

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2ec2c63e7781'
down_revision: Union[str, None] = 'fda7e53b939d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
