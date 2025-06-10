from .BaseObtener import BaseObtener


class SaldosObtener(BaseObtener):
    SALDO_URL = "https://script.google.com/macros/s/AKfycbzSFKR3DyDo9Ezxsq_75DDJ1vze76Lj_kC4iXiFMvAE_t6Xbi9rHrejT9v8CnWqWV9UKw/exec"

    def __init__(self):
        super().__init__()

    def obtener_saldos(self) -> dict:
        return self.obtener_data(self.SALDO_URL)
