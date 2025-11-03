Deploy: Docker Compose on VPS (with HTTPS)

This guide deploys the app using Docker Compose on a single VPS with a local MySQL and a reverse proxy (nginx or Caddy) for HTTPS.

Prerequisites
- A VPS (Ubuntu 22.04+ recommended) with a public IP
- A domain (e.g., example.com) pointing to your VPS public IP (A/AAAA DNS record)
- Docker + Docker Compose installed

1) Prepare environment
- Copy the production example: `.env.prod.example` → `.env`
- Edit `.env` and set secure values (SECRET_KEY, DB creds, branding, etc.)

2) Build and start with Compose
```bash
docker compose pull   # optional if using prebuilt images
docker compose up --build -d
```

Notes
- Compose has been hardened to bind services locally only:
  - MySQL: 127.0.0.1:3306
  - App:   127.0.0.1:5000
- Both services use `restart: unless-stopped`.

3) Configure HTTPS reverse proxy

Option A: Caddy (simplest, auto TLS)
1. Install Caddy (from official docs)
2. Create `/etc/caddy/Caddyfile` using the provided template and replace `yourdomain.com`.
3. Reload: `sudo systemctl reload caddy`

Template: see `deploy/CADDYFILE_TEMPLATE`

Option B: nginx + Certbot
1. Install nginx and Certbot:
```bash
sudo apt update && sudo apt install -y nginx
sudo apt install -y certbot python3-certbot-nginx
```
2. Copy `deploy/NGINX_SITE_TEMPLATE.conf` to `/etc/nginx/sites-available/fee` and replace `yourdomain.com`.
3. Enable and reload:
```bash
sudo ln -s /etc/nginx/sites-available/fee /etc/nginx/sites-enabled/fee
sudo nginx -t && sudo systemctl reload nginx
```
4. Issue TLS cert:
```bash
sudo certbot --nginx -d yourdomain.com --redirect -m you@example.com --agree-tos -n
```

4) Verify
- Open: `https://yourdomain.com`
- First login: username `user`, password `9133` (change in Admin → Settings)

5) Share review link
- Share `https://yourdomain.com` with your reviewers/friends.
- If you need a specific landing page, you can share paths like `https://yourdomain.com/auth/login` or `https://yourdomain.com/ai`.

6) M-Pesa callback (if used)
- Set `DARAJA_CALLBACK_URL=https://yourdomain.com/mpesa/callback` in `.env`.
- Update the M-Pesa portal to point to the same URL.

Operational Tips
- Logs: `docker compose logs -f web` or `docker compose logs -f db`
- Restart app: `docker compose restart web`
- Backup DB: use `mysqldump` inside the `db` container or from host port 3306 (bound to 127.0.0.1)

Security Reminders
- Rotate `SECRET_KEY`, change default passwords, keep `.env` private
- Leave ports bound to 127.0.0.1; only expose HTTPS via the reverse proxy
- Keep system and Docker images updated (`apt upgrade`, `docker compose pull`)

