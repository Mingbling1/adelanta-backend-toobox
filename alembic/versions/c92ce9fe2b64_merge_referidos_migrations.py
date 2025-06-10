"""merge_referidos_migrations

Revision ID: c92ce9fe2b64
Revises: feb54b67bfe0, 269599300867
Create Date: 2025-04-11 14:00:11.904490

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c92ce9fe2b64'
down_revision: Union[str, None] = ('feb54b67bfe0', '269599300867')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
