"""
� IO V2 - Comunicación externa
"""

from .webservice import KPIWebserviceAdapter
from .sector_pagadores_data_source import SectorPagadoresDataSource
from .webservice_client import BaseWebserviceClient

__all__ = [
    "KPIWebserviceAdapter",
    "SectorPagadoresDataSource",
    "BaseWebserviceClient",
]
