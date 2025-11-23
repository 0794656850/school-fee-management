# CS Fee Management System

A modern, multi-tenant school fee management web application for small and medium learning institutions (Kindergarten → High School). It digitizes the full fee lifecycle: student enrollment, term fees and invoicing, payments, credits and transfers, reminders, analytics, and admin/user access — with optional M-Pesa STK Push, WhatsApp Cloud API, and an AI assistant.

## Key Features
- Multi-tenant: per-school data isolation, settings, and branding (each school stores its details in `school_settings` keyed by its own `school_id`, so identical data never cross-links).
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

-## Repository Layout
- App entry: `app.py`, `wsgi.py`
- Blueprints: `routes/` (admin, auth, credit, mpesa, terms, reminders, ai, approvals, insights)
- Utilities: `utils/` (settings, tenant, users, whatsapp, mpesa, ledger, ai, security, audit, alerts, document_qr, notifications, db_helpers)
- Templates: `templates/` (pages, partials, printables, guardian receipts, approvals, insights)
- Static: `static/`
- Docs: `docs/` (M-Pesa setup, AI assistant)
- Scripts: `scripts/` (index/ask AI, seeding, testing Daraja, read settings, start-ngrok)

## Governance & Admin Workflows
- **Approval workbench**: `/admin/approvals` lets staff submit OTP-secured requests for write-offs, discounts, or credit transfers. Requests are hashed, emailed via `utils/notifications`, recorded in `approval_requests`, and resolved with QR-signed documents from `utils/document_qr` so auditors can verify summaries.
- **Insights & alerts**: `/admin/insights` surfaces anomalies (collections drop, failed callbacks, unused credits) calculated in `utils/alerts`. A "Send alert" button notifies configured recipients via Gmail and logs the attempt for oversight.
- **Guardian receipts**: guardians can upload payment proofs through the new upload page, and supporting helpers in `utils/db_helpers` persist and track status so admins can verify before reconciliation.

## Interfaces

### School portal (admin + staff)
- **Dashboard (`/`)**: High-level KPIs for totals collected, outstanding balances, credits, and failed payments sit alongside a timeline of recent receipts so admins can spot trends at a glance. A floating "Add Payment" action opens the payment form used by staff to log cash, bank transfers, or imported M-Pesa/STK callbacks without leaving the dashboard.
- **Students & payments (`/students` et al.)**: Student profiles surface class, guardian contacts, credit balances, invoices, and ledger rows; clicking a student reveals historical payments, attached documents, and an "Add Payment" shortcut to settle balances faster.
- **Credit Ops**: The credit workspace (`/credit`) hosts overpayments, sibling transfers, and the audit trail for reversals so excess funds stay transparent while being reallocated where needed.
- **Terms & invoices (`/terms`, `/terms/invoices`)**: Term planners manage fee components, discounts, class defaults, and printable invoices; invoicing pages include bulk actions, PDF exports, and email dispatch that references the selected term and guardian.
- **Reminders & recovery**: The reminders module generates templated Gmail reminders, while the recovery dashboard tracks defaulters, logged contacts, promises to pay, and CSV exports for outreach.
- **Insights (`/admin/insights`)**: Anomaly detection compares two weeks of collections, failed callbacks, and unused credits, surfaces the delta, and lets admins fire alert emails to configured recipients from the same screen.
- **Approvals (`/admin/approvals`)**: Staff can request write-offs, discounts, or credit transfers; each submission sends a hashed OTP via Gmail and records the approval request for approvers to confirm before the change touches live data.
- **Guardian receipts (`/admin/guardian_receipts`)**: Uploaded receipts show guardian details, file links, verification status, and admin notes so finance teams can release funds only after manually vetting uploads.
- **Billing & Upgrade (`/admin/billing`)**: This page accepts M-Pesa references to toggle Pro features (multi-user, exports, WhatsApp receipts) and shows the activation key so the same entry point drives manual or webhook-backed upgrades.
- **AI assistant & documentation**: The Vertex/Gemini-backed assistant (`/ai`) climbs the RAG index for last payment details, defaulter summaries, and architectural Q&A while the docs page surfaces guides stored under `docs/`.

### Guardian portal (parents)
- **Login & child switching (`/guardian/login`, `/guardian/switch`)**: Guardians authenticate with email/phone or tokens; once signed in they can switch between enrolled children without reauthentication.
- **Dashboard (`/guardian/dashboard`)**: The portal lists each child's balance, recent payments, remaining term dues, and contextual actions (print receipt, open payment modal, upload proof). Native filters surface pending invoices, pending STK pushes, and guardian analytics in one scrollable page.
- **Payments (`/guardian/make_payment`, PayPal integration)**: Guardians trigger STK Push requests, manually confirm references, or use PayPal buttons; status refresh buttons poll `/guardian/status` and highlight success before the admin ledger is updated.
- **Receipt printing & verification (`/guardian/receipt/<id>`)**: Receipts are fully branded, printable, and QR-signed for guard verification; the same view surfaces current balance, credit, and action buttons for reprinting or emailing to other guardians.
- **Receipt upload (`/guardian/upload-receipt`)**: Upload forms accept PNG/JPG/PDF proof, capture guardian metadata, and queue each file for admin verification using the `utils/db_helpers` workflow described above.
- **Analytics, events, and notifications (`/guardian/analytics`, `/guardian/events`, `/guardian/notifications`)**: Spending analytics chart payments across time, show progress toward targets, and pair with scheduled events/notification feeds so guardians never miss due dates.
- **Guardian AI assistant (`/guardian/ai_assistant`)**: A limited version of the RAG assistant answers balance queries or clarifies invoice line items right from the guardian dashboard.

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

### Alerts & approvals (optional)
- `ALERT_EMAIL_RECIPIENTS` (comma-separated) toggles who receives insights alerts.
- `ALERT_COLLECTION_DROP_PERCENT`, `ALERT_FAILED_PAYMENT_RATIO`, and `ALERT_UNUSED_CREDITS_THRESHOLD` adjust the detection thresholds (defaults: 30%, 1.5×, KES 5,000).
- `GUARDIAN_RECEIPT_UPLOADS_DIR` chooses where guardian-uploaded receipts live under `static/` (default `uploads/guardian_receipts`).

### Security configuration (optional)
- `ALLOWED_HOSTS` (comma-separated) locks the app to specific hostnames/aliases and rejects invalid Host headers.
- `MAX_CONTENT_LENGTH` caps request bodies (default 10 MB) and helps mitigate large payload abuse.

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
- Admin login (`/admin/login`) now caps POSTs at 6/min per IP and records failed attempts in the audit log.
- Insights & Alerts: `/admin/insights` reports collection drops, failed payment spikes, and unused credits and emails configured recipients (`ALERT_EMAIL_RECIPIENTS`) whenever thresholds trigger.
- Approval workflows: `/admin/approvals` lets staff log write-offs/discounts/credit transfers, dispatches OTPs before submissions, and issues QR-backed confirmations once admins approve.
- Guardian receipt uploads: guardians can submit receipt or cheque scans from `/g/upload-receipt`; admins verify and track them at `/admin/guardian-receipts`.
- Guardian receipts (`/g/receipt/<payment_id>`) now embed signed QR codes that link back to the verified receipt payload.
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
- `scripts/seed_students.py` - seed example student data
- `scripts/read_app_settings.py` - view app settings
- `scripts/test_daraja_token.py` - test Daraja token/config
- `scripts/ai_index.py` / `scripts/ai_ask.py` - AI index and Q&A

## Screenshots
Drop your screenshots into `docs/screenshots/` using the filenames below and they will render here automatically on GitHub.

| View | Image |
| --- | --- |
| Dashboard | <picture><source srcset="docs/screenshots/dashboard.png"><img alt="Dashboard" src="docs/screenshots/dashboard.svg" width="320"></picture> |
| Students | <picture><source srcset="docs/screenshots/students.png"><img alt="Students" src="docs/screenshots/students.svg" width="320"></picture> |
| Add Payment | <picture><source srcset="docs/screenshots/payments.png"><img alt="Payments" src="docs/screenshots/payments.svg" width="320"></picture> |
| Analytics | <picture><source srcset="docs/screenshots/analytics.png"><img alt="Analytics" src="docs/screenshots/analytics.svg" width="320"></picture> |
| Credit Ops | <picture><source srcset="docs/screenshots/credit-ops.png"><img alt="Credit Ops" src="docs/screenshots/credit-ops.svg" width="320"></picture> |
| Terms | <picture><source srcset="docs/screenshots/terms.png"><img alt="Terms" src="docs/screenshots/terms.svg" width="320"></picture> |
| Invoices | <picture><source srcset="docs/screenshots/invoices.png"><img alt="Invoices" src="docs/screenshots/invoices.svg" width="320"></picture> |
| School Profile | <picture><source srcset="docs/screenshots/school-profile.png"><img alt="School Profile" src="docs/screenshots/school-profile.svg" width="320"></picture> |
| Admin Security | <picture><source srcset="docs/screenshots/admin-security.png"><img alt="Admin Security" src="docs/screenshots/admin-security.svg" width="320"></picture> |
| AI Assistant | <picture><source srcset="docs/screenshots/ai-assistant.png"><img alt="AI Assistant" src="docs/screenshots/ai-assistant.svg" width="320"></picture> |

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

---

## Security & Secrets
- Do not commit real secrets. Keep secrets in environment variables or `.env` (which is gitignored).
- Recommended production secrets:
  - `SECRET_KEY` (Flask session/signing key)
  - `LICENSE_SECRET` (license signing/verification)
  - Database credentials or `SQLALCHEMY_DATABASE_URI`
  - Daraja (`DARAJA_*`) if using STK Push
  - Gmail OAuth (`credentials.json`, `GMAIL_*`) if using reminders
  - `EMAIL_INBOUND_SECRET` if using inbound email approvals
- GitHub secret scanning is enabled via `.github/workflows/secret-scan.yml` (gitleaks).

## Media via Git LFS
- Large media in `static/media/` is tracked with Git LFS via `.gitattributes`.
- Install once: `git lfs install`.
- Add the promo video at `static/media/Stop_Guessing_Fees__SmartEduPay_Made_Easy.mp4` and commit normally.
- See `static/media/README.md` for step-by-step instructions.

## AI Providers (Options)
- Vertex AI (recommended): set `VERTEX_PROJECT_ID`, optional `VERTEX_LOCATION`, and `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON.
- Gemini API key (no service account): set `GOOGLE_API_KEY` and use `/gemini/chat` or configure UI to use Gemini.
- OpenAI/Azure (optional): `OPENAI_API_KEY` or `AZURE_OPENAI_API_KEY` are supported in the AI utility if you extend usage.
- Build/search the local code index with `scripts/ai_index.py` and query with `scripts/ai_ask.py`.

## CI/CD
- Secret scan on pushes/PRs: `.github/workflows/secret-scan.yml` (gitleaks).
- Deployment examples in `deploy/README_GitHub_Actions.md` and `deploy/README_Docker_VPS.md`.

## Production Checklist
- Set `SECRET_KEY`, `LICENSE_SECRET`, DB credentials/URI; disable `OAUTHLIB_INSECURE_TRANSPORT` in prod.
- Use HTTPS; set `PREFERRED_URL_SCHEME=https` and ensure secure cookies.
- Configure Daraja for `production` and update callback URL.
- Configure Gmail OAuth redirect URIs for your domain; place client JSON securely.
- Change default passwords in Admin → Settings.

## Architecture (MVC)
- Controllers: controllers/ (blueprints; backward compatible with outes/).
- Views: 	emplates/ + static/ (Jinja + assets).
- Models: models.py (to be split gradually as needed).
See docs/MVC.md for details.



## Data Wipe
To remove all profiles and all data:

- Preview: python scripts/wipe_all_data.py --dry-run
- Irreversible wipe: python scripts/wipe_all_data.py --force "DELETE ALL"

This truncates all tables in the configured MySQL database and purges stored files in static/media, static/uploads, uploads/, and instance/. Ensure you have backups if needed.
