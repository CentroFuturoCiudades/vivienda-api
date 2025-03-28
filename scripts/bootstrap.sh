#!/bin/bash

set -e  # Exit on error

# 1. Load .env
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env file not found. Please create one with DOMAIN=your.domain.com"
  exit 1
fi

# 2. Validate domain
if [ -z "$DOMAIN" ]; then
  echo "❌ DOMAIN not set in .env"
  exit 1
fi

# 3. Set paths
CERT_SRC="./nginx/ssl/live/$DOMAIN"
CERT_DST="./nginx/certs"

# 4. Check if certs already exist
if [ -f "$CERT_DST/fullchain.pem" ] && [ -f "$CERT_DST/privkey.pem" ]; then
  echo "✅ Certificates already exist in $CERT_DST"
else
  echo "🔐 Certificates not found. Running Certbot in Docker..."

  # 5. Run Certbot
  sudo docker run -it --rm \
    -v "$(pwd)/nginx/ssl:/etc/letsencrypt" \
    -v "/var/lib/letsencrypt:/var/lib/letsencrypt" \
    -p 80:80 \
    certbot/certbot certonly \
    --standalone \
    -d "$DOMAIN"

  if [ ! -d "$CERT_SRC" ]; then
    echo "❌ Certbot failed or directory not created: $CERT_SRC"
    exit 1
  fi

  # 6. Copy certs to flat folder
  mkdir -p "$CERT_DST"
  cp "$CERT_SRC/fullchain.pem" "$CERT_DST/"
  cp "$CERT_SRC/privkey.pem" "$CERT_DST/"
  cp "$CERT_SRC/chain.pem" "$CERT_DST/"
  echo "✅ Certificates copied to $CERT_DST"
fi

# 7. Start app with domain in env
echo "🚀 Starting Docker Compose with DOMAIN=$DOMAIN"
docker compose up -d --build
