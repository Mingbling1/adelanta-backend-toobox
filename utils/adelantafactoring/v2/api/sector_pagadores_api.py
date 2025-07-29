"""
API layer para compatibilidad con SectorPagadoresCalcular original
"""

from typing import List, Dict, Any

from ..engines.sector_pagadores_engine import SectorPagadoresEngine
from ..schemas.sector_pagadores import SectorPagadoresRequest, SectorPagadoresResult


class SectorPagadoresCalcularV2:
    """
    Wrapper de compatibilidad para mantener API original de SectorPagadoresCalcular
    """

    def __init__(self):
        self.engine = SectorPagadoresEngine()

    def calcular(self, **kwargs) -> Dict[str, Any]:
        """
        Método principal compatible con API original

        Returns:
            Dict con resultados procesados
        """
        # Crear request usando parámetros de kwargs o valores por defecto
        request = SectorPagadoresRequest()

        # Ejecutar el engine
        result = self.engine.calculate(request)

        # Convertir resultado a formato original
        return self._convertir_a_formato_original(result)

    def _convertir_a_formato_original(
        self, result: SectorPagadoresResult
    ) -> Dict[str, Any]:
        """
        Convierte el resultado del engine al formato esperado por el sistema original
        """
        return {
            "datos_procesados": [record.model_dump() for record in result.records],
            "total_registros": result.records_count,
            "errores": [],
            "metadatos": {"origen": "SectorPagadoresCalcular", "version": "v2"},
        }

    # Métodos de compatibilidad adicionales (si son necesarios)
    def procesar_datos(self, datos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Método de compatibilidad para procesamiento directo de datos"""
        # Delegar al engine
        request = SectorPagadoresRequest()
        result = self.engine.calculate(request)
        return result.records

    def validar_datos(self, datos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Método de compatibilidad para validación de datos"""
        return self.engine._validar_datos(datos)
