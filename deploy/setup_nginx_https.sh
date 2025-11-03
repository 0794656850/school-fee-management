#!/usr/bin/env bash
set -euo pipefail

# Usage: sudo bash deploy/setup_nginx_https.sh -d yourdomain.com -e you@example.com [-u 127.0.0.1:5000]

DOMAIN=""
EMAIL=""
UPSTREAM="127.0.0.1:5000"

while getopts ":d:e:u:" opt; do
  case $opt in
    d) DOMAIN="$OPTARG" ;;
    e) EMAIL="$OPTARG" ;;
    u) UPSTREAM="$OPTARG" ;;
    *) echo "Usage: $0 -d domain -e email [-u upstream_host:port]" >&2; exit 2 ;;
  esac
done

if [[ -z "$DOMAIN" || -z "$EMAIL" ]]; then
  echo "Both -d <domain> and -e <email> are required." >&2
  echo "Example: sudo bash $0 -d example.com -e admin@example.com" >&2
  exit 2
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This script targets Ubuntu/Debian (apt). Please adapt for your distro." >&2
  exit 1
fi

if [[ $EUID -ne 0 ]]; then
  echo "Please run as root: sudo bash $0 -d $DOMAIN -e $EMAIL" >&2
  exit 1
fi

echo "Installing nginx and certbot..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y nginx certbot python3-certbot-nginx

SITE_CONF_PATH="/etc/nginx/sites-available/fee"
echo "Writing nginx site config for $DOMAIN → $UPSTREAM ..."
cat > "$SITE_CONF_PATH" <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;

    client_max_body_size 20m;

    location / {
        proxy_pass http://$UPSTREAM;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 300s;
    }

    location ~* \.(?:css|js|png|jpg|jpeg|gif|ico|svg|webp|woff2?)$ {
        proxy_pass http://$UPSTREAM;
        add_header Cache-Control "public, max-age=31536000";
        expires 1y;
    }
}
EOF

echo "Enabling site and reloading nginx..."
ln -sf "$SITE_CONF_PATH" /etc/nginx/sites-enabled/fee
if [[ -f /etc/nginx/sites-enabled/default ]]; then
  rm -f /etc/nginx/sites-enabled/default
fi
nginx -t
systemctl reload nginx

echo "Requesting TLS certificate via certbot for $DOMAIN ..."
certbot --nginx -d "$DOMAIN" --redirect -m "$EMAIL" --agree-tos -n

echo "Done. Your site should be available at: https://$DOMAIN"
echo "If DNS is new, allow time to propagate."

