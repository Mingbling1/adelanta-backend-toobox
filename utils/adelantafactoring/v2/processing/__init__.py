"""
� Processing V2 - Pipeline de procesamiento
"""

from .transformers import KPITransformer
from .nuevos_clientes_pagadores import NuevosClientesNuevosPagadoresTransformer

__all__ = [
    "KPITransformer",
    "NuevosClientesNuevosPagadoresTransformer"
]
