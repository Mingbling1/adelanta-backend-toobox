#!/bin/bash

# Configuración
domains=(apiToolbox.adelantafactoring.com)
rsa_key_size=4096
data_path="./data/certbot"
email="jimmy.auris@adelantafactoring.com"
staging=0 # Cambia a 1 para pruebas

# Usar credenciales de variables de entorno o valores predeterminados
cf_email=${CLOUDFLARE_EMAIL:-"jimmy.auris@adelantafactoring.com"}
cf_key=${CLOUDFLARE_API_KEY:-""}

# Verificar que la API key esté disponible
if [ -z "$cf_key" ]; then
  echo "Error: No se proporcionó CLOUDFLARE_API_KEY"
  echo "Ejecute el script con: CLOUDFLARE_API_KEY=tu-api-key ./cloudflare-ssl.sh"
  exit 1
fi

echo "Instalando plugin de Cloudflare..."
docker-compose run --rm --entrypoint "\
  pip install certbot-dns-cloudflare" certbot

# Crear directorio para credenciales
mkdir -p ~/app/cloudflare-credentials
echo "Creando archivo de credenciales Cloudflare..."

# Crear archivo de credenciales solo con API KEY
cat > ~/app/cloudflare-credentials/credentials.ini << EOF
dns_cloudflare_email = "$cf_email"
dns_cloudflare_api_key = "$cf_key"
EOF

# Establecer permisos
chmod 600 ~/app/cloudflare-credentials/credentials.ini

# Descargar parámetros TLS si no existen
if [ ! -e "$data_path/conf/options-ssl-nginx.conf" ] || [ ! -e "$data_path/conf/ssl-dhparams.pem" ]; then
  echo "### Downloading recommended TLS parameters ..."
  mkdir -p "$data_path/conf"
  curl -s https://raw.githubusercontent.com/certbot/certbot/master/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf > "$data_path/conf/options-ssl-nginx.conf"
  curl -s https://raw.githubusercontent.com/certbot/certbot/master/certbot/certbot/ssl-dhparams.pem > "$data_path/conf/ssl-dhparams.pem"
fi

# Preparar argumentos de dominio
domain_args=""
for domain in "${domains[@]}"; do
  domain_args="$domain_args -d $domain"
done

# Configurar email
email_arg="--email $email"
if [ $staging != "0" ]; then staging_arg="--staging"; fi

# Solicitar certificado usando validación DNS de Cloudflare
echo "### Solicitando certificado Let's Encrypt usando validación DNS de Cloudflare..."
docker-compose run --rm --entrypoint "\
  certbot certonly --dns-cloudflare \
    --dns-cloudflare-credentials /cloudflare-credentials/credentials.ini \
    $staging_arg \
    $email_arg \
    $domain_args \
    --rsa-key-size $rsa_key_size \
    --agree-tos \
    --force-renewal" certbot

echo "### Reiniciando nginx..."
docker-compose exec nginx nginx -s reload || docker-compose restart nginx

echo "### Configuración SSL completada!"