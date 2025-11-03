# CS Fee Management System

A modern, multi-tenant school fee management web application for small and medium learning institutions (Kindergarten → High School). It digitizes the full fee lifecycle: student enrollment, term fees and invoicing, payments, credits and transfers, reminders, analytics, and admin/user access — with optional M-Pesa STK Push, WhatsApp Cloud API, and an AI assistant.

## Key Features
- Multi-tenant: per-school data isolation, settings, and branding.
- Students & Payments: balances, overpayments as credit, credit transfers between siblings.
- Terms & Invoices: academic terms, fee components, per-class defaults, discounts, invoice generation, term summaries.
- Collections: M-Pesa (Daraja) STK Push initiation and callback reconciliation.
- Reminders: Email reminders (via Gmail API) to guardians.
- Recovery: Fee Defaulter Recovery module (contact logging, promises-to-pay, export CSV).
- Receipts: Email receipts sent after payments (Gmail API with SMTP fallback).
- Analytics: dashboards with monthly/daily trends, class summaries, method breakdown, top debtors, and MoM change.
- Admin & Roles: simple auth; owner/admin/staff roles (multi-user as Pro), security and access settings.
- Auditability: basic ledger entries and audit trail for key events.
- AI Assistant (optional): RAG-backed answers for balances, debtors, analytics, and project Q&A.

## Tech Stack
- Backend: Flask (Blueprints), MySQL (mysql-connector), some legacy SQLAlchemy models.
- UI: Jinja templates, minimal JS/CSS in `static/`.
- Integrations: Safaricom Daraja (M-Pesa), Gmail API (email reminders), WhatsApp Cloud API (optional receipts), Vertex AI (Gemini) for the AI assistant.
- Deployment: Dockerfile, docker-compose, WSGI (for gunicorn/uwsgi), `.env` support via `config.py`.

## Repository Layout
- App entry: `app.py`, `wsgi.py`
- Blueprints: `routes/` (admin, auth, credit, mpesa, terms, reminders, ai)
- Utilities: `utils/` (settings, tenant, users, whatsapp, mpesa, ledger, ai, security, audit)
- Templates: `templates/` (pages, partials, printables)
- Static: `static/`
- Docs: `docs/` (M-Pesa setup, AI assistant)
- Scripts: `scripts/` (index/ask AI, seeding, testing Daraja, read settings)

## Quickstart (Local)
Prerequisites:
- Python 3.10+ and pip
- MySQL 8.x (or compatible) running locally
- Git

1) Create database (example)
```sql
CREATE DATABASE school_fee_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

2) Configure environment
- Copy `.env.example` to `.env` (optional) and/or set env vars.
- Minimum DB configuration (choose one approach):
  - Single URI (recommended):
    - `SQLALCHEMY_DATABASE_URI=mysql+pymysql://root:password@localhost/school_fee_db`
  - Or discrete vars used by direct connectors:
    - `DB_HOST=localhost`
    - `DB_USER=root`
    - `DB_PASSWORD=your_password`
    - `DB_NAME=school_fee_db`

3) Install dependencies
```bash
pip install -r requirements.txt
```

4) Run the app
```bash
python app.py
# App starts on http://127.0.0.1:5000
```

5) First login and bootstrap
- Visit `/auth/login`.
- Enter your school name/code; new schools bootstrap automatically.
- Default per-school credentials: username `user`, password `9133` (change after first login under Admin → Access Settings).
- Admin area: `/admin/login` (default password `9133`).

## Configuration
Most settings can be done in the Admin UI. Environment variables can override defaults.

- Branding & Profile: Admin → School Profile (`/admin/school`)
- Access Settings (login, passwords): Admin → Settings (`/admin/settings`)
- WhatsApp Cloud API: Admin → Settings → WhatsApp
  - `WHATSAPP_ACCESS_TOKEN`, `WHATSAPP_PHONE_NUMBER_ID`, optional `WHATSAPP_TEMPLATE_NAME`, `WHATSAPP_TEMPLATE_LANG`
 - Email Reminders (Gmail API): place OAuth client in `credentials.json` at project root; token cached in `token.json` after first sign-in. Scope: `gmail.send`.
   - Start auth at `/gmail/authorize`.
   - Local dev (HTTP): add an Authorized redirect URI in Google Cloud equal to `http://127.0.0.1:5000/oauth2callback` (or `http://localhost:5000/oauth2callback`). Set `GMAIL_REDIRECT_URI` to the same if needed, and ensure `OAUTHLIB_INSECURE_TRANSPORT=1` is set (the app enables it automatically for HTTP localhost). Do NOT use this flag in production.
   - Env overrides: `GMAIL_CREDENTIALS_JSON` (path to client JSON), `GMAIL_TOKEN_JSON` (path to token), `GMAIL_REDIRECT_URI` (explicit redirect URI), `OAUTHLIB_INSECURE_TRANSPORT` (dev only).
- M-Pesa (Daraja): Admin → M-Pesa Config (`/admin/mpesa`) or env vars
  - `DARAJA_ENV` (sandbox|production), `DARAJA_CONSUMER_KEY`, `DARAJA_CONSUMER_SECRET`, `DARAJA_SHORT_CODE`, `DARAJA_PASSKEY`, `DARAJA_CALLBACK_URL`
  - STK Push: Admin → Billing triggers an STK for Pro upgrade (config required)
- Payments QR/Link (optional): `PAYMENT_LINK` displays a QR on receipts

See detailed setup guides:
- `docs/MPESA_SETUP.md` for Daraja (STK Push)
- `docs/AI_ASSISTANT.md` for the AI assistant

## AI Assistant (Optional)
- Configure Vertex AI: set `VERTEX_PROJECT_ID`, optional `VERTEX_LOCATION`, and `GOOGLE_APPLICATION_CREDENTIALS` pointing to your service account JSON.
- Build index: `python scripts/ai_index.py` (outputs to `instance/ai/`)
- Ask: `python scripts/ai_ask.py "How do invoices work?"`
- In-app: visit `/ai` to chat; Pro gating may apply.

## Docker
A quick containerized setup is provided.
```bash
# Build image
docker build -t fee-mgmt .

# Or use docker compose
docker compose up --build
```
Set your envs via Compose or a `.env` file before running.

## Security Notes
- Change all default credentials immediately after first login.
- Keep `.env`, `instance/` (per-machine data), and `static/uploads/` out of version control (already handled in `.gitignore`).
- Restrict access to Admin pages and API credentials.

## Common Endpoints
- App login: `/auth/login`
- Choose school: `/choose_school`
- Admin dashboard: `/admin`
- Term fees & invoices: `/terms`
- Credit operations: `/credit`
- M-Pesa callback: `/mpesa/callback`

## Licensing Approvals (Email)
- After a school submits manual payment details, the system emails the owner/admin with Approve and Reject links.
- Links are HMAC signed and expire after 7 days. Configure `APP_SIGNING_SECRET` (or `SECRET_KEY`).
- Approve: activates the plan and emails the license key to the school automatically.
- Reject: marks the submission rejected; the school remains on the basic plan.
- AI assistant: `/ai`

### Quick Reply: YES/NO (Auto‑Activate)
- The admin email subject now includes a token like `REQ:<uuid>`.
- You may simply reply to that email with `YES` to approve (issues and auto‑activates the license, and emails the key to the school email) or `NO` to reject.
- Configure your email provider to POST inbound replies to `POST /billing/inbound-email`.
- Secure the endpoint by setting `EMAIL_INBOUND_SECRET` and passing it via header `X-Email-Secret`, form field `secret`, or query param `?secret=`.

## Scripts
- `scripts/seed_students.py` – seed example student data
- `scripts/read_app_settings.py` – view app settings
- `scripts/test_daraja_token.py` – test Daraja token/config
- `scripts/ai_index.py` / `scripts/ai_ask.py` – AI index and Q&A

## Screenshots
Drop your screenshots into `docs/screenshots/` using the filenames below and they will render here automatically on GitHub.

| View | Image |
| --- | --- |
| Dashboard | ![Dashboard](docs/screenshots/dashboard.png) |
| Students | ![Students](docs/screenshots/students.png) |
| Add Payment | ![Payments](docs/screenshots/payments.png) |
| Analytics | ![Analytics](docs/screenshots/analytics.png) |
| Credit Ops | ![Credit Ops](docs/screenshots/credit-ops.png) |
| Terms | ![Terms](docs/screenshots/terms.png) |
| Invoices | ![Invoices](docs/screenshots/invoices.png) |
| School Profile | ![School Profile](docs/screenshots/school-profile.png) |
| Admin Security | ![Admin Security](docs/screenshots/admin-security.png) |
| AI Assistant | ![AI Assistant](docs/screenshots/ai-assistant.png) |

## Development Tips
- Schema safety: the app runs idempotent "ensure_*" migrations at startup to add missing columns/tables.
- Multi-tenant: most tables include `school_id`; UI and queries scope by the active school.
- Legacy ORM models exist (`models.py`), but production code mainly uses direct SQL.

## License
No license specified. If you intend to open-source, add a suitable license file.

## New Enhancements
- Health check script: python scripts/health_check.py returns JSON status and exit code (0 on healthy).
- Login rate limiting: protects /auth/login at 5 requests/minute per IP.
- Dark mode: toggle via the moon icon in the header; preference is saved.
- PWA basics: manifest + service worker for static assets; installable from supported browsers.
### Fee Defaulter Recovery

- Open `http://127.0.0.1:5000/recovery` (or click Recovery in the sidebar).
- Filter by class, search term, and minimum balance.
- Click "Log Action" to record calls/SMS/emails/visits, promises-to-pay, and follow-up dates.
- Export the current defaulters list as CSV.
