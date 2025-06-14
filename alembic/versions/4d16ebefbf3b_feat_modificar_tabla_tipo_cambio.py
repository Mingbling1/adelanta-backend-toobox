"""feat: modificar tabla Tipo Cambio

Revision ID: 4d16ebefbf3b
Revises: 
Create Date: 2024-09-23 21:33:53.386048

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4d16ebefbf3b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('colocaciones',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('CodigoLiquidacion', sa.String(length=255), nullable=False),
    sa.Column('CodigoSolicitud', sa.String(length=255), nullable=True),
    sa.Column('RUCCliente', sa.String(length=255), nullable=False),
    sa.Column('RazonSocialCliente', sa.String(length=255), nullable=False),
    sa.Column('RUCPagador', sa.String(length=255), nullable=False),
    sa.Column('RazonSocialPagador', sa.String(length=255), nullable=False),
    sa.Column('Moneda', sa.String(length=255), nullable=False),
    sa.Column('DeudaAnterior', sa.Float(), nullable=False),
    sa.Column('ObservacionLiquidacion', sa.String(length=255), nullable=True),
    sa.Column('ObservacionSolicitud', sa.String(length=255), nullable=True),
    sa.Column('FlagPagoInteresConfirming', sa.String(length=255), nullable=True),
    sa.Column('TipoOperacion', sa.String(length=255), nullable=False),
    sa.Column('Estado', sa.String(length=255), nullable=False),
    sa.Column('NroDocumento', sa.String(length=255), nullable=False),
    sa.Column('TasaNominalMensualPorc', sa.Float(), nullable=False),
    sa.Column('FinanciamientoPorc', sa.Float(), nullable=False),
    sa.Column('FechaConfirmado', sa.DateTime(), nullable=False),
    sa.Column('FechaOperacion', sa.DateTime(), nullable=False),
    sa.Column('DiasEfectivo', sa.Integer(), nullable=False),
    sa.Column('NetoConfirmado', sa.Float(), nullable=False),
    sa.Column('FondoResguardo', sa.Float(), nullable=False),
    sa.Column('MontoComisionEstructuracion', sa.Float(), nullable=False),
    sa.Column('ComisionEstructuracionIGV', sa.Float(), nullable=False),
    sa.Column('ComisionEstructuracionConIGV', sa.Float(), nullable=False),
    sa.Column('MontoCobrar', sa.Float(), nullable=False),
    sa.Column('Interes', sa.Float(), nullable=False),
    sa.Column('InteresConIGV', sa.Float(), nullable=False),
    sa.Column('GastosContrato', sa.Float(), nullable=False),
    sa.Column('GastoVigenciaPoder', sa.Float(), nullable=False),
    sa.Column('ServicioCobranza', sa.Float(), nullable=False),
    sa.Column('ServicioCustodia', sa.Float(), nullable=False),
    sa.Column('GastosDiversosIGV', sa.Float(), nullable=False),
    sa.Column('GastosDiversosConIGV', sa.Float(), nullable=False),
    sa.Column('MontoTotalFacturado', sa.Float(), nullable=False),
    sa.Column('MontoDesembolso', sa.Float(), nullable=False),
    sa.Column('FacturasGeneradas', sa.String(length=255), nullable=False),
    sa.Column('Ejecutivo', sa.String(length=255), nullable=False),
    sa.Column('MontoPago', sa.Float(), nullable=False),
    sa.Column('MontoPagoSoles', sa.Float(), nullable=False),
    sa.Column('ExcesoPago', sa.Float(), nullable=False),
    sa.Column('FechaDesembolso', sa.DateTime(), nullable=True),
    sa.Column('MontoDevolucion', sa.Float(), nullable=False),
    sa.Column('EstadoDevolucion', sa.String(length=255), nullable=True),
    sa.Column('Anticipo', sa.String(length=255), nullable=True),
    sa.Column('TramoAnticipo', sa.String(length=255), nullable=True),
    sa.Column('Mes', sa.String(length=255), nullable=False),
    sa.Column('Año', sa.String(length=255), nullable=False),
    sa.Column('MesAño', sa.String(length=255), nullable=False),
    sa.Column('tcFecha', sa.DateTime(), nullable=True),
    sa.Column('tcCompra', sa.Float(), nullable=True),
    sa.Column('tcVenta', sa.Float(), nullable=True),
    sa.Column('ColocacionSoles', sa.Float(), nullable=False),
    sa.Column('MontoDesembolsoSoles', sa.Float(), nullable=False),
    sa.Column('Ingresos', sa.Float(), nullable=False),
    sa.Column('IngresosSoles', sa.Float(), nullable=False),
    sa.Column('MesSemana', sa.String(length=255), nullable=False),
    sa.Column('CostosFondo', sa.Float(), nullable=False),
    sa.Column('TotalIngresos', sa.Float(), nullable=False),
    sa.Column('CostosFondoSoles', sa.Float(), nullable=False),
    sa.Column('TotalIngresosSoles', sa.Float(), nullable=False),
    sa.Column('Utilidad', sa.Float(), nullable=False),
    sa.Column('Sector', sa.String(length=255), nullable=True),
    sa.Column('GrupoEco', sa.String(length=255), nullable=True),
    sa.Column('FueraSistema', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('nuevos_clientes_nuevos_pagadores',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('Mes', sa.String(length=255), nullable=True),
    sa.Column('Ejecutivo', sa.String(length=255), nullable=True),
    sa.Column('RUCCliente', sa.String(length=255), nullable=True),
    sa.Column('RUCPagador', sa.String(length=255), nullable=True),
    sa.Column('TipoOperacion', sa.String(length=255), nullable=True),
    sa.Column('RazonSocial', sa.String(length=255), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('retomas',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('RUCPagador', sa.String(length=255), nullable=True),
    sa.Column('RazonSocialPagador', sa.String(length=255), nullable=True),
    sa.Column('Cobranzas_MontoPagoSoles', sa.Float(), nullable=False),
    sa.Column('Desembolsos_MontoDesembolsoSoles', sa.Float(), nullable=False),
    sa.Column('PorRetomar', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('saldos',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('Fecha', sa.DateTime(), nullable=False),
    sa.Column('EvolucionCaja', sa.Float(), nullable=True),
    sa.Column('CostoExcesoCaja', sa.Float(), nullable=True),
    sa.Column('IngresoNoRecibidoPorExcesoCaja', sa.Float(), nullable=True),
    sa.Column('MontoOvernight', sa.Float(), nullable=True),
    sa.Column('IngresosOvernightNeto', sa.Float(), nullable=True),
    sa.Column('SaldoTotalCaja', sa.Float(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('tipo_cambio',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('TipoCambioFecha', sa.String(length=255), nullable=False),
    sa.Column('TipoCambioCompra', sa.Float(), nullable=False),
    sa.Column('TipoCambioVenta', sa.Float(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('TipoCambioFecha'),
    sa.UniqueConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('tipo_cambio')
    op.drop_table('saldos')
    op.drop_table('retomas')
    op.drop_table('nuevos_clientes_nuevos_pagadores')
    op.drop_table('colocaciones')
    # ### end Alembic commands ###
