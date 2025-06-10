import requests
import httpx
from utils.timing_decorator import timing_decorator
from tenacity import retry, stop_after_attempt, wait_exponential
from config.logger import logger
from httpx import HTTPStatusError
from ..Base import Base


class BaseObtener(Base):
    def __init__(self) -> None:
        self.token = None

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def obtener_data(self, url) -> dict:
        response = requests.get(url)
        response.raise_for_status()
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}:{response.text}")

        return response.json()

    @timing_decorator
    async def get_token(self, token_url, credentials):
        if not token_url or not credentials:
            raise ValueError("Token URL and credentials must be provided")

        async with httpx.AsyncClient(timeout=120) as client:
            logger.debug("Requesting access token...")
            response = await client.post(token_url, data=credentials)
            response.raise_for_status()
            self.token = response.json().get("access_token")
            logger.debug("Access token obtained")

    @timing_decorator
    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch_data(self, client, url, params):
        if not self.token:
            raise ValueError("Token must be obtained before fetching data")

        headers = {"Authorization": f"Bearer {self.token}"}
        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
        except HTTPStatusError as e:
            logger.debug(
                f"HTTP error occurred: {e.response.status_code} - {e.response.text}"
            )
            raise

        return response.json()
