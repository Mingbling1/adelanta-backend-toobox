from .DiferidoCalcular import DiferidoCalcular
from .DiferidoExternoCalcular import DiferidoExternoCalcular
from .DiferidoInternoCalcular import DiferidoInternoCalcular
from ..BaseCalcular import BaseCalcular

__all__ = [
    "DiferidoCalcular",
    "DiferidoExternoCalcular",
    "DiferidoInternoCalcular",
]


class Diferido(BaseCalcular):
    def __init__(self) -> None:
        super().__init__()
        self.diferido_externo = DiferidoExternoCalcular()
        self.diferido_interno = DiferidoInternoCalcular()
        self.diferido = DiferidoCalcular()
