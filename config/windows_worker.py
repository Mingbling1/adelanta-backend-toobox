from rq import SimpleWorker
from rq.timeouts import BaseDeathPenalty, JobTimeoutException
import threading


class WindowsDeathPenalty(BaseDeathPenalty):
    """Implementación de timeout para Windows usando threading"""

    def setup_death_penalty(self):
        # Cambiado self.timeout a self._timeout
        self.timer = threading.Timer(self._timeout, self.handle_death_penalty)
        self.timer.daemon = True
        self.timer.start()

    def cancel_death_penalty(self):
        if hasattr(self, "timer"):
            self.timer.cancel()

    def handle_death_penalty(self):
        # Cambiado self.timeout a self._timeout
        exception_message = "[{}] Timeout de {:.1f}s excedido".format(
            self.job_id, self._timeout
        )
        raise JobTimeoutException(exception_message)


class WindowsWorker(SimpleWorker):
    """Worker para Windows con soporte apropiado de timeouts"""

    death_penalty_class = WindowsDeathPenalty
