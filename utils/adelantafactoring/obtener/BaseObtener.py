import requests
import httpx

# import asyncio
from utils.timing_decorator import timing_decorator
from tenacity import retry, stop_after_attempt, wait_exponential
from config.logger import logger
from httpx import HTTPStatusError
from ..Base import Base
from typing import Dict, Any, Optional


class BaseObtener(Base):
    """
    Clase base para obtener datos de APIs externas.
    Soporta tanto métodos síncronos como asíncronos.
    """

    def __init__(self) -> None:
        self.token: Optional[str] = None
        self._timeout_sync = 60  # Timeout para requests síncronos
        self._timeout_async = 120  # Timeout para httpx asíncronos

    # === MÉTODOS SÍNCRONOS (Para Google Sheets y APIs simples) ===

    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def obtener_data(self, url: str, timeout: int = None, headers: dict = None) -> dict:
        """
        Obtiene datos de una URL de forma síncrona con reintentos automáticos.
        
        Args:
            url: URL de la cual obtener los datos
            timeout: Timeout en segundos (por defecto usa self._timeout_sync)
            headers: Headers adicionales para la petición
            
        Returns:
            dict: Datos JSON obtenidos de la URL
            
        Raises:
            ValueError: Si la URL está vacía o es inválida
            requests.exceptions.RequestException: Para errores de red/HTTP
            Exception: Para otros errores inesperados
        """
        # Validaciones de entrada
        if not url or not isinstance(url, str):
            raise ValueError("URL debe ser una cadena no vacía")
        
        if not url.startswith(('http://', 'https://')):
            raise ValueError("URL debe comenzar con http:// o https://")
        
        # Configuración de la petición
        request_timeout = timeout or self._timeout_sync
        request_headers = {
            'User-Agent': 'Adelanta-Toolbox/1.0',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        }
        
        # Agregar headers adicionales si se proporcionan
        if headers and isinstance(headers, dict):
            request_headers.update(headers)
        
        try:
            logger.debug(f"Obteniendo datos síncronos desde: {url}")
            logger.debug(f"Timeout configurado: {request_timeout}s")
            
            # Realizar la petición HTTP
            response = requests.get(
                url,
                timeout=request_timeout,
                headers=request_headers,
                allow_redirects=True,
                verify=True  # Verificar certificados SSL
            )
            
            # Verificar el código de estado
            response.raise_for_status()
            
            # Verificar que la respuesta tenga contenido
            if not response.content:
                logger.warning(f"Respuesta vacía desde: {url}")
                return {}
            
            # Verificar que el content-type sea JSON
            content_type = response.headers.get('content-type', '').lower()
            if 'json' not in content_type:
                logger.warning(f"Content-Type no es JSON: {content_type}")
            
            # Parsear JSON
            try:
                data = response.json()
            except ValueError as json_error:
                logger.error(f"Error al parsear JSON: {json_error}")
                logger.error(f"Contenido recibido: {response.text[:500]}")
                raise Exception(f"Respuesta no es JSON válido: {json_error}")
            
            # Log de éxito
            data_length = len(data) if isinstance(data, (list, dict)) else "N/A"
            logger.debug(f"Datos síncronos obtenidos exitosamente. Elementos: {data_length}")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout ({request_timeout}s) al obtener datos de: {url}")
            raise Exception(f"Timeout al obtener datos de {url}")
        
        except requests.exceptions.ConnectionError as conn_error:
            logger.error(f"Error de conexión al obtener datos de: {url} - {conn_error}")
            raise Exception(f"Error de conexión: {conn_error}")
        
        except requests.exceptions.HTTPError as http_error:
            status_code = http_error.response.status_code if http_error.response else "N/A"
            error_text = http_error.response.text if http_error.response else "N/A"
            
            logger.error(f"Error HTTP {status_code} al obtener datos de: {url}")
            logger.error(f"Detalle del error: {error_text}")
            
            # Diferentes mensajes según el código de estado
            if status_code == 400:
                raise Exception(f"Petición inválida (400): {error_text}")
            elif status_code == 401:
                raise Exception("No autorizado (401): Verificar credenciales")
            elif status_code == 403:
                raise Exception("Acceso prohibido (403): Sin permisos suficientes")
            elif status_code == 404:
                raise Exception("Recurso no encontrado (404): {url}")
            elif status_code == 429:
                raise Exception("Demasiadas peticiones (429): Intenta más tarde")
            elif status_code >= 500:
                raise Exception(f"Error del servidor ({status_code}): {error_text}")
            else:
                raise Exception(f"Error HTTP {status_code}: {error_text}")
        
        except requests.exceptions.RequestException as req_error:
            logger.error(f"Error general de requests: {req_error}")
            raise Exception(f"Error en la petición HTTP: {req_error}")
        
        except Exception as unexpected_error:
            logger.error(f"Error inesperado al obtener datos de {url}: {unexpected_error}")
            raise Exception(f"Error inesperado: {unexpected_error}")

    # === MÉTODOS ASÍNCRONOS (Para APIs con autenticación) ===
    @timing_decorator
    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def obtener_data_async_simple(self, url: str) -> Dict[Any, Any]:
        """
        Método asíncrono para obtener datos sin autenticación.

        Args:
            url: URL de la API

        Returns:
            Datos obtenidos de la API
        """
        try:
            logger.debug(f"Obteniendo data async desde: {url}")

            async with httpx.AsyncClient(timeout=self._timeout_async) as client:
                response = await client.get(url)
                response.raise_for_status()

                data = response.json()
                logger.debug(
                    f"Data async obtenida exitosamente. Registros: {len(data) if isinstance(data, list) else 'N/A'}"
                )
                return data

        except httpx.TimeoutException:
            logger.error(f"Timeout al obtener data async de: {url}")
            raise Exception("Timeout en consulta async")
        except HTTPStatusError as e:
            logger.error(f"Error HTTP: {e.response.status_code} - {e.response.text}")
            raise Exception(f"Error HTTP {e.response.status_code}: {e.response.text}")
        except Exception as e:
            logger.error(f"Error inesperado en obtener_data_async_simple: {e}")
            raise

    @timing_decorator
    async def obtener_token(self, token_url: str, credentials: Dict[str, str]) -> None:
        """
        Obtiene token de autenticación de manera asíncrona.

        Args:
            token_url: URL para obtener el token
            credentials: Credenciales de autenticación
        """
        if not token_url or not credentials:
            raise ValueError("URL de token y credenciales son requeridos")

        try:
            async with httpx.AsyncClient(timeout=self._timeout_async) as client:
                logger.debug("Solicitando token de acceso...")
                response = await client.post(token_url, data=credentials)
                response.raise_for_status()

                token_data = response.json()
                self.token = token_data.get("access_token")

                if not self.token:
                    raise ValueError("Token no encontrado en la respuesta")

                logger.debug("Token de acceso obtenido exitosamente")

        except httpx.TimeoutException:
            logger.error(f"Timeout al obtener token de: {token_url}")
            raise Exception("Timeout al obtener token")
        except httpx.HTTPStatusError as e:
            logger.error(f"Error HTTP al obtener token: {e.response.status_code}")
            raise Exception(f"Error de autenticación: {e.response.status_code}")
        except Exception as e:
            logger.error(f"Error inesperado al obtener token: {e}")
            raise

    @timing_decorator
    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def obtener_data_async(
        self, client: httpx.AsyncClient, url: str, params: Dict[str, Any]
    ) -> Dict[Any, Any]:
        """
        Obtiene datos de manera asíncrona con autenticación.

        Args:
            client: Cliente HTTP asíncrono
            url: URL de la API
            params: Parámetros de la consulta

        Returns:
            Datos obtenidos de la API
        """
        if not self.token:
            raise ValueError("Token debe ser obtenido antes de consultar datos")

        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            logger.debug(f"Obteniendo data async desde: {url}")
            logger.debug(f"Parámetros: {params}")

            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            logger.debug(
                f"Data async obtenida exitosamente. Registros: {len(data) if isinstance(data, list) else 'N/A'}"
            )
            return data

        except httpx.TimeoutException:
            logger.error(f"Timeout al obtener data async de: {url}")
            raise Exception("Timeout en consulta async")
        except HTTPStatusError as e:
            logger.error(f"Error HTTP: {e.response.status_code} - {e.response.text}")
            raise Exception(f"Error HTTP {e.response.status_code}: {e.response.text}")
        except Exception as e:
            logger.error(f"Error inesperado en obtener_data_async: {e}")
            raise

    # === MÉTODOS DE UTILIDAD ===

    def tiene_token(self) -> bool:
        """Verifica si se tiene un token válido"""
        return self.token is not None

    def limpiar_token(self) -> None:
        """Limpia el token almacenado"""
        self.token = None
        logger.debug("Token limpiado")

    async def obtener_data_con_autenticacion(
        self,
        url: str,
        params: Dict[str, Any],
        token_url: str,
        credentials: Dict[str, str],
    ) -> Dict[Any, Any]:
        """
        Método de conveniencia que obtiene token y datos en una sola llamada.
        """
        try:
            # Obtener token si no existe
            if not self.tiene_token():
                await self.obtener_token(token_url, credentials)

            # Obtener datos
            async with httpx.AsyncClient(timeout=self._timeout_async) as client:
                return await self.obtener_data_async(client, url, params)

        except Exception as e:
            logger.error(f"Error en obtener_data_con_autenticacion: {e}")
            # Intentar limpiar token y volver a intentar una vez
            if self.tiene_token():
                logger.debug("Limpiando token e intentando nuevamente...")
                self.limpiar_token()
                await self.obtener_token(token_url, credentials)
                async with httpx.AsyncClient(timeout=self._timeout_async) as client:
                    return await self.obtener_data_async(client, url, params)
            raise
