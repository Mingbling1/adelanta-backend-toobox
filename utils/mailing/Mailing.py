import logging
import resend
from jinja2 import Environment, FileSystemLoader
from config.settings import settings


class Mailing:
    def __init__(self):
        self.api_key = settings.RESEND_API_KEY
        self.template_dir = "templates"  # Directorio de tus plantillas HTML
        resend.api_key = self.api_key

    def enviar_solicitud_lead(
        self,
        monto: str,
        dias_credito: int,
        tasa_mensual: str,
        moneda: str,
        comision_operacional: str,
        monto_total_recibir: str,
        email: str,
    ):

        # Reinicializar la API key desde settings en cada llamada
        from config.settings import settings

        resend.api_key = settings.RESEND_API_KEY

        # Imprimir para debug (solo temporalmente)
        import logging

        logging.info(f"Usando API key: {resend.api_key}")
        """
        Envía un correo con los datos de la solicitud de adelanto de factoring.
        Utiliza la plantilla 'solicitud_lead_template.html' que debe contener las variables
        necesarias para mostrar los datos calculados.
        Args:
            monto (str): Monto del adelanto.
            dias_credito (int): Días de crédito.
            tasa_mensual (str): Tasa mensual del adelanto.
            moneda (str): Moneda del adelanto (PEN, USD, etc.).
            comision_operacional (str): Comisión operacional del adelanto.
            monto_total_recibir (str): Monto total a recibir.
            email (str): Correo electrónico del destinatario.
        """

        # Renderizar la plantilla con los datos
        env = Environment(loader=FileSystemLoader(self.template_dir))
        template = env.get_template("solicitud_lead_template.html")
        html_content = template.render(
            monto=monto,
            dias_credito=dias_credito,
            tasa_mensual=tasa_mensual,
            moneda=moneda,
            comision_operacional=comision_operacional,
            monto_total_recibir=monto_total_recibir,
        )

        params: resend.Emails.SendParams = {
            "to": [email],
            "subject": "¡Tu Simulación Está Aquí! Contáctanos para Saber más de Ella 📩📲",
            "from": "Adelanta Factoring<info@adelantafactoring.com>",
            "html": html_content,
        }
        try:
            email_response = resend.Emails.send(params)
            logging.info(f"Lead request email sent successfully: {email_response}")
        except Exception as e:
            logging.error(f"Failed to send lead request email: {e}")
            raise e

    def enviar_confirmacion(self, nombre: str, email: str, token: str):
        """
        Envía un correo de confirmación utilizando un link generado a partir del token.
        Se utiliza la plantilla 'confirmacion.html' que debe utilizar la variable confirmLink.
        """
        # Construir el link de confirmación
        confirm_link = f"{settings.FRONTEND_DOMAIN}/auth/verification?token={token}"

        # Renderizar la plantilla con el confirm_link
        env = Environment(loader=FileSystemLoader(self.template_dir))
        template = env.get_template("confirmacion.html")
        html_content = template.render(nombre=nombre, confirmLink=confirm_link)

        params: resend.Emails.SendParams = {
            "to": [email],
            "subject": "Confirma tu correo electrónico",
            "from": "Info <info@adelantafactoring.com>",
            "html": html_content,
        }
        try:
            email_response = resend.Emails.send(params)
            logging.info(f"Confirmation email sent successfully: {email_response}")
        except Exception as e:
            logging.error(f"Failed to send confirmation email: {e}")
            raise e
