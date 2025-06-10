from .BaseObtener import BaseObtener


class ReferidosObtener(BaseObtener):
    REFERIDOS_URL = "https://script.google.com/macros/s/AKfycbxZS8ahi8BnlBJcRx4H9E_qy1JHbhIATqnNUx_P-OJGrDstcGjDtACpeftKozeOCp0_/exec"

    def __init__(self):
        super().__init__()

    def referidos_obtener(self):
        return self.obtener_data(self.REFERIDOS_URL)
