#!/bin/bash

if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

domains=(apitoolbox.adelantafactoring.com)
rsa_key_size=4096
data_path="./data/certbot"
email="jimmy.auris@adelantafactoring.com"
staging=0

# Función para verificar si el certificado existe y es válido
check_certificate() {
    local domain=$1
    local cert_path="$data_path/conf/live/$domain/fullchain.pem"
    
    if [ -f "$cert_path" ]; then
        if openssl x509 -in "$cert_path" -noout -checkend 2592000 2>/dev/null; then
            echo "Certificado válido encontrado para $domain"
            return 0
        else
            echo "Certificado encontrado pero expira pronto"
            return 1
        fi
    else
        echo "No se encontró certificado para $domain"
        return 1
    fi
}

# FUNCIÓN CORREGIDA: Crear enlaces simbólicos en el sistema de archivos local
fix_certificate_path() {
    local domain=$1
    local main_cert_dir="$data_path/conf/live/$domain"
    local duplicate_cert_dir="$data_path/conf/live/$domain-0001"
    
    echo "### Verificando rutas de certificados..."
    echo "Buscando en: $main_cert_dir"
    echo "Buscando duplicado en: $duplicate_cert_dir"
    
    if [ ! -d "$main_cert_dir" ] && [ -d "$duplicate_cert_dir" ]; then
        echo "### Corrigiendo ruta del certificado..."
        
        # Crear directorio principal
        mkdir -p "$main_cert_dir"
        
        # Crear enlaces simbólicos relativos (CORRECCIÓN IMPORTANTE)
        ln -sf "../$domain-0001/fullchain.pem" "$main_cert_dir/fullchain.pem"
        ln -sf "../$domain-0001/privkey.pem" "$main_cert_dir/privkey.pem"
        ln -sf "../$domain-0001/cert.pem" "$main_cert_dir/cert.pem"
        ln -sf "../$domain-0001/chain.pem" "$main_cert_dir/chain.pem"
        
        echo "Enlaces simbólicos creados:"
        ls -la "$main_cert_dir/"
        return 0
    elif [ -d "$main_cert_dir" ]; then
        echo "Directorio principal ya existe: $main_cert_dir"
        # Verificar que los enlaces existan
        if [ -f "$main_cert_dir/fullchain.pem" ]; then
            echo "Certificado principal encontrado"
            return 0
        else
            echo "Directorio existe pero sin certificados, recreando enlaces..."
            if [ -d "$duplicate_cert_dir" ]; then
                ln -sf "../$domain-0001/fullchain.pem" "$main_cert_dir/fullchain.pem"
                ln -sf "../$domain-0001/privkey.pem" "$main_cert_dir/privkey.pem"
                ln -sf "../$domain-0001/cert.pem" "$main_cert_dir/cert.pem"
                ln -sf "../$domain-0001/chain.pem" "$main_cert_dir/chain.pem"
                return 0
            fi
        fi
    else
        echo "No se encontró ningún certificado"
        return 1
    fi
}

# FUNCIÓN AÑADIDA: Limpiar certificados duplicados
cleanup_duplicate_certs() {
    local domain=$1
    echo "### Limpiando certificados duplicados para $domain..."
    
    # Eliminar certificados con sufijos usando docker
    docker-compose run --rm --entrypoint "\
      find /etc/letsencrypt/live -name '$domain-*' -type d -exec rm -rf {} + 2>/dev/null || true" certbot
    docker-compose run --rm --entrypoint "\
      find /etc/letsencrypt/archive -name '$domain-*' -type d -exec rm -rf {} + 2>/dev/null || true" certbot
    docker-compose run --rm --entrypoint "\
      find /etc/letsencrypt/renewal -name '$domain-*.conf' -type f -exec rm -f {} + 2>/dev/null || true" certbot
}

# Verificar variable de entorno ECR
if [ -z "$ECR_REPOSITORY_URI" ]; then
    echo "### Configurando ECR_REPOSITORY_URI temporal"
    export ECR_REPOSITORY_URI="dummy-value"
fi

echo "### Iniciando proceso inteligente de certificados SSL..."

# Verificar certificado existente
for domain in "${domains[@]}"; do
    if check_certificate "$domain"; then
        echo "### Certificado válido encontrado. Reiniciando nginx..."
        docker-compose restart nginx
        
        sleep 5
        if docker-compose ps nginx | grep -q "Up"; then
            echo "### ✅ nginx iniciado correctamente con certificado existente"
            exit 0
        else
            echo "### ⚠️ nginx falló. Intentando corregir certificados..."
        fi
    fi
    
    # Intentar corregir ruta del certificado
    if fix_certificate_path "$domain"; then
        echo "### Reiniciando nginx después de corregir ruta..."
        docker-compose restart nginx
        sleep 5
        if docker-compose ps nginx | grep -q "Up"; then
            echo "### ✅ nginx iniciado correctamente después de corrección"
            exit 0
        else
            echo "### ⚠️ nginx aún falla, continuando con obtención de certificado..."
        fi
    fi
done

echo "### Necesario obtener nuevo certificado..."
# Limpiar certificados duplicados
for domain in "${domains[@]}"; do
    cleanup_duplicate_certs "$domain"
done

# Descargar parámetros TLS si no existen
if [ ! -e "$data_path/conf/options-ssl-nginx.conf" ] || [ ! -e "$data_path/conf/ssl-dhparams.pem" ]; then
  echo "### Descargando parámetros TLS recomendados..."
  mkdir -p "$data_path/conf"
  curl -s https://raw.githubusercontent.com/certbot/certbot/master/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf > "$data_path/conf/options-ssl-nginx.conf"
  curl -s https://raw.githubusercontent.com/certbot/certbot/master/certbot/certbot/ssl-dhparams.pem > "$data_path/conf/ssl-dhparams.pem"
fi

# Crear certificado dummy solo si es necesario
echo "### Creando certificado temporal para ${domains[0]}..."
path="/etc/letsencrypt/live/${domains[0]}"
mkdir -p "$data_path/conf/live/${domains[0]}"
docker-compose run --rm --entrypoint "\
  openssl req -x509 -nodes -newkey rsa:$rsa_key_size -days 1\
    -keyout '$path/privkey.pem' \
    -out '$path/fullchain.pem' \
    -subj '/CN=localhost'" certbot

echo "### Iniciando nginx con certificado temporal..."
docker-compose up --force-recreate -d nginx

echo "### Eliminando certificado temporal..."
docker-compose run --rm --entrypoint "\
  rm -Rf /etc/letsencrypt/live/${domains[0]} && \
  rm -Rf /etc/letsencrypt/archive/${domains[0]} && \
  rm -Rf /etc/letsencrypt/renewal/${domains[0]}.conf" certbot

echo "### Solicitando certificado Let's Encrypt..."
domain_args=""
for domain in "${domains[@]}"; do
  domain_args="$domain_args -d $domain"
done

case "$email" in
  "") email_arg="--register-unsafely-without-email" ;;
  *) email_arg="--email $email" ;;
esac

if [ $staging != "0" ]; then staging_arg="--staging"; fi

# Forzar eliminación de certificados existentes antes de crear uno nuevo
docker-compose run --rm --entrypoint "\
  certbot delete --cert-name ${domains[0]} --non-interactive 2>/dev/null || true" certbot

# Solicitar nuevo certificado
docker-compose run --rm --entrypoint "\
  certbot certonly --webroot -w /var/www/certbot \
    $staging_arg \
    $email_arg \
    $domain_args \
    --rsa-key-size $rsa_key_size \
    --agree-tos \
    --force-renewal \
    --cert-name ${domains[0]}" certbot

echo "### Verificando certificado creado..."
if [ -f "$data_path/conf/live/${domains[0]}/fullchain.pem" ]; then
    echo "### ✅ Certificado creado correctamente"
    echo "### Reiniciando nginx..."
    docker-compose restart nginx
    echo "### ✅ Proceso completado exitosamente"
else
    echo "### ❌ Error: No se pudo crear el certificado"
    echo "### Listando certificados disponibles:"
    ls -la "$data_path/conf/live/" 2>/dev/null || echo "Directorio no existe"
    exit 1
fi