from .KPICalcular import KPICalcular
from .OperacionesFueraSistemaCalcular import OperacionesFueraSistemaCalcular
from .NuevosClientesNuevosPagadoresCalcular import NuevosClientesNuevosPagadoresCalcular
from .RetomasCalcular import RetomasCalcular
from .SaldosCalcular import SaldosCalcular
from .SectorPagadoresCalcular import SectorPagadoresCalcular
from .BaseCalcular import BaseCalcular
from .ReferidosCalcular import ReferidosCalcular

# from .DiferidoExternoCalcular import DiferidoExternoCalcular
# from .DiferidoInternoCalcular import DiferidoInternoCalcular
# from .DiferidoCalcular import DiferidoCalcular

__all__ = [
    "KPICalcular",
    "OperacionesFueraSistemaCalcular",
    "NuevosClientesNuevosPagadoresCalcular",
    "RetomasCalcular",
    "SaldosCalcular",
    "SectorPagadoresCalcular",
    "BaseCalcular",
    "ReferidosCalcular",
    # "DiferidoExternoCalcular",
    # "DiferidoInternoCalcular",
    # "DiferidoCalcular",
]


class Calculos(BaseCalcular):
    """
    Esta clase contiene todos los calculos de adelantafactoring
    """

    def __init__(self) -> None:
        super().__init__()
        # self.diferido_externo = DiferidoExternoCalcular()
        # self.diferido_interno = DiferidoInternoCalcular()
        # self.diferido = DiferidoCalcular()
