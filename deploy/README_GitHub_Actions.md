GitHub Actions CI/CD to a VPS (Docker Compose)

This pipeline uploads the repo to your VPS and runs `docker compose up --build -d` remotely.

1) Prerequisites on VPS
- Docker and Compose installed
- A reverse proxy for HTTPS (see `deploy/README_Docker_VPS.md` and `deploy/setup_nginx_https.sh`)
- A directory for the app, e.g. `/opt/fee-mgmt`
- A `.env` file present on the VPS in that directory (not committed to Git)

2) GitHub Secrets (Repository → Settings → Secrets and variables → Actions)
- `VPS_HOST` — your server IP or DNS name
- `VPS_USER` — SSH username (e.g., `ubuntu`)
- `VPS_SSH_KEY` — contents of a private key that has access to the VPS
- `VPS_PATH` — absolute path on VPS (e.g., `/opt/fee-mgmt`)
- Optional: `VPS_PORT` — SSH port (defaults to 22)

3) Workflow
- Defined in `.github/workflows/deploy.yml`
- Triggers on push to `main` or manual `workflow_dispatch`
- Steps:
  - Checks out code
  - Uses `appleboy/scp-action` to upload project (excludes `.git`, `venv`, `.env`, `instance/`)
  - SSH into VPS and run `docker compose up --build -d`

4) First-time bootstrap
- Ensure the target folder exists and place `.env` there:
  ```bash
  sudo mkdir -p /opt/fee-mgmt
  sudo chown $USER:$USER /opt/fee-mgmt
  cp .env.prod.example .env   # edit values
  # Put this .env on your VPS at /opt/fee-mgmt/.env
  ```
- After the first CI run, set up HTTPS using:
  ```bash
  sudo bash deploy/setup_nginx_https.sh -d yourdomain.com -e you@example.com
  ```

5) Shareable URL
- Once HTTPS is configured, share: `https://yourdomain.com`

