from .BaseObtener import BaseObtener


class OperacionesFueraSistemaObtener(BaseObtener):
    OPERACIONES_FUERA_SISTEMA_PEN_URL = "https://script.google.com/macros/s/AKfycbyWsLb2hCr7cuXJIUorkmQJZEPnH-7i24Zy2yRlSfOwb4BZGPs01zdmiRVKuzRN8uKCtg/exec"
    OPERACIONES_FUERA_SISTEMA_USD_URL = "https://script.google.com/macros/s/AKfycbz3Jt1zj_29QPEM4hexYdlvcgSMNUICsF9xNsEPlDaGSF-0VXv7JPouSkhGH8AubU5-/exec"

    def __init__(self):
        super().__init__()

    def obtener_operaciones_fuera_sistema_pen(self):
        return self.obtener_data(self.OPERACIONES_FUERA_SISTEMA_PEN_URL)

    def obtener_operaciones_fuera_sistema_usd(self):
        return self.obtener_data(self.OPERACIONES_FUERA_SISTEMA_USD_URL)
