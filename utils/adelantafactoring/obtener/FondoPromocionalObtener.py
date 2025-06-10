from .BaseObtener import BaseObtener


class FondoPromocionalObtener(BaseObtener):
    FONDOPROMOCIONAL_URL = "https://script.google.com/macros/s/AKfycbzpX9RKtvJwN1QgFMU15hi1DXHtRhFlIC6jW8_QYTB-sQQIntsDO7fG6jWgKJb95V6X/exec"

    def __init__(self):
        super().__init__()

    def fondo_promocional_obtener(self):
        return self.obtener_data(self.FONDOPROMOCIONAL_URL)
