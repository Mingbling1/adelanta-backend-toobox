from .BaseObtener import BaseObtener


class SectorPagadoresObtener(BaseObtener):
    SECTOR_PAGADORES_URL = "https://script.google.com/macros/s/AKfycbxxdJazJbEJ7qbGgi8oBAJrzIZjpnD1cYKv1RkcBQtQSx7KA60UGaXMYHTKxKOeRC3c/exec"

    def __init__(self):
        super().__init__()

    def obtener_sector_pagadores(self) -> dict:
        return self.obtener_data(self.SECTOR_PAGADORES_URL)
