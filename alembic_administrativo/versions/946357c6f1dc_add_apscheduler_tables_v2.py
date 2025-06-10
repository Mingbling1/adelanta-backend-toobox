"""Add APScheduler tables v2

Revision ID: 946357c6f1dc
Revises: a40909327049
Create Date: 2024-11-09 12:03:25.026050

"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "946357c6f1dc"
down_revision: Union[str, None] = "a40909327049"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Crear tabla apscheduler_jobs
    op.create_table(
        "apscheduler_jobs",
        sa.Column("id", sa.String(length=255), primary_key=True),
        sa.Column("next_run_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("job_state", sa.String(length=255), nullable=False),
    )


def downgrade() -> None:
    # Eliminar tabla apscheduler_jobs
    op.drop_table("apscheduler_jobs")
