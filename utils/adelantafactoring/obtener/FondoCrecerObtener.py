from .BaseObtener import BaseObtener


class FondoCrecerObtener(BaseObtener):
    FONDOCRECER_URL = "https://script.google.com/macros/s/AKfycbyFKvZcqZNBm2XktdOR4lrv5Wwd_PwovO85INFieEqzQexXgwXD5XuF-nPWPME1sjGFlQ/exec"

    def __init__(self):
        super().__init__()

    def fondo_crecer_obtener(self):
        return self.obtener_data(self.FONDOCRECER_URL)
