Fee Management Portal - System Overview

This document summarises the product identity, the primary navigation for both school and guardian users, and the core capabilities that define the active experience. Feed it into the AI index so the assistant can reference the current feature set without guessing.

## Branding
- **Brand Name**: SmartEduPay (configured via `BRAND_NAME`).
- **Portal Title**: SmartEduPay Portal (`PORTAL_TITLE`).
- **Primary Logo**: `static/img/smartedupay_logo.svg`.
- **Secondary Logo / favicon**: `static/img/smartedupay_logo_secondary.svg`.
- **Footer Signature**: (c) 2025 Fee_Management_System.
- **Multi-tenant safety**: Every school's data sits in tables with a `school_id` so even similarly named records never leak between tenants.

## School Admin Navigation
- **Dashboard (`/`)**: Presents KPI cards for collections, outstanding balances, unused credit, and failed callbacks, plus a feed of recent payments and a floating "Add Payment" action that launches the core cash/STK entry form.
- **Students & payments (`/students`, student detail)**: Maintain student profiles, contact info, class assignments, invoice history, and the ledger view. Each student page lets admins append payments, credits, notes, and documents without switching context.
- **Add Payment modal**: Accessible from the top bar or dashboard; records cash, bank, or M-Pesa/STK callbacks, fills audit rows, and can send confirmations immediately.
- **Credit Ops (`/credit`)**: Tracks overpayment credit, sibling transfers, and the balance audit trail so staff can refund or reapply funds with accountability.
- **Terms & invoices (`/terms`, `/terms/invoices`)**: Define academic terms, fee components, per-class defaults, discounts, and generate printable and email-ready invoices that reference the selected term.
- **Reminders (`/reminders`)**: Send Gmail-based reminder templates to guardians about unpaid invoices or balances, log the attempts, and view the reminder history before following up manually.
- **Guardian receipts (`/admin/guardian_receipts`)**: Displays guardian uploads, file previews, description, status, and admin notes so finance teams only reconcile once receipt documentation is verified.
- **Approvals (`/admin/approvals`)**: Staff submit OTP-secured write-off, discount, or credit transfer requests; emails carry the OTP, the hashed request lives in `approval_requests`, and approvers confirm before the action reaches the ledger.
- **Insights (`/admin/insights`)**: Compares the last 7 days against the preceding week for collections, callback failures, and unused credit, surfaces drop percentages, and provides a button to email anomalies to configured recipients.
- **Recovery & defaulters (`/recovery`)**: Track defaulter contacts, promises to pay, and notes; exports provide CSV snapshots for finance and recovery teams.
- **Billing & Upgrade (`/admin/billing`)**: Accepts M-Pesa references for manual license activation, shows whether Daraja credentials are configured, and toggles Pro-only capabilities such as multi-user, exports, and WhatsApp receipts.
- **Access & Security**: Manage owner/admin/staff roles, reset passwords, and review login activity to keep tenant access tight.
- **AI assistant (`/ai`)**: Vertex/Gemini-backed RAG chatbot that cites documentation, recent payments, and ledger events; available across the admin namespace for rapid answers.
- **Documentation (`/docs`)**: Central reference for setup guides, AI assistant context, and architectural notes (see `docs/`).

## Guardian Navigation
- **Login & session switching (`/guardian/login`, `/guardian/switch`)**: Guardians authenticate with email/phone or tokens sent by schools; once signed in they can switch between children without re-entering credentials.
- **Dashboard (`/guardian/dashboard`)**: Lists each child with current balance, credit, term progress, invoices, recent payments, and quick actions (print receipt, start payment, upload proof); mobile-aware layout keeps tablets/phones usable.
- **Payments (`/guardian/make_payment`, `/guardian/status`)**: Guardians launch STK Push or PayPal checkouts, poll `/guardian/status` for callback success, and drop manual M-Pesa references that staff can verify before updating balances.
- **Receipt printing & verification (`/guardian/receipt/<payment_id>`)**: Branded receipt view shows payment metadata, balance, credit, print button, and a QR that admins can scan to verify authenticity via `utils/document_qr`.
- **Receipt upload (`/guardian/upload-receipt`)**: Guardians submit PNG/JPG/PDF proof along with contact info; the upload is stored with status metadata so admins only reconcile after confirming the file.
- **Analytics & progress (`/guardian/analytics`)**: Spending charts show term intake, balances, and progress percentages so guardians can see whether payments keep pace with dues.
- **Events & notifications (`/guardian/events`, `/guardian/notifications`)**: Event feeds highlight upcoming due dates while notification endpoints surface status changes or reminders.
- **Guardian AI assistant (`/guardian/ai_assistant`)**: A scoped RAG assistant answers balance/receipt questions directly from the guardian dashboard without exposing admin data.

## Core Capabilities
- Multi-tenant isolation per `school_id` with optional branding packages.
- Student balances, credits, ledger entries, and per-term invoices with email/print distribution.
- Daraja (M-Pesa) STK push integration plus PayPal fallbacks for guardians; manual references flow through billing.
- Gmail-powered reminders/receipts and WhatsApp-ready messaging (when configured).
- Guardian uploads verified through `utils/db_helpers` and surfaced in `/admin/guardian_receipts`.
- OTP-based approvals for write-offs, discounts, and credit transfers, logged through `utils/notifications` and `utils/document_qr`.
- Analytics + insights dashboards that detect collection drops, failed callbacks, and unused credits, with alert emails logged via `utils/alerts`.
- AI assistant (Vertex/Gemini RAG) for both admins (`/ai`) and guardians (`/guardian/ai_assistant`).

## Key Endpoints
- `/` - Admin dashboard.
- `/students`, `/students/<id>` - Student list and detail/ledger.
- `/credit` - Credit operations workspace.
- `/terms`, `/terms/invoices` - Term planner and invoice center.
- `/reminders` - Email reminder composer.
- `/recovery` - Defaulter recovery workflow.
- `/admin/approvals` - Approval request workbench.
- `/admin/insights` - Anomaly detector + alert sender.
- `/admin/guardian_receipts` - Guardian uploads queue.
- `/admin/billing` - Manual activation + MPesa health.
- `/ai` - Admin AI assistant.
- `/guardian/login`, `/guardian/dashboard`, `/guardian/switch`.
- `/guardian/make_payment`, `/guardian/status`.
- `/guardian/receipt/<payment_id>`, `/guardian/upload-receipt`.
- `/guardian/analytics`, `/guardian/events`, `/guardian/notifications`, `/guardian/ai_assistant`.

## Branding Configuration (env vars)
- `BRAND_NAME=SmartEduPay`
- `PORTAL_TITLE="SmartEduPay Portal"`
- `LOGO_PRIMARY=img/smartedupay_logo.svg`
- `LOGO_SECONDARY=img/smartedupay_logo_secondary.svg`
- `FAVICON=img/smartedupay_logo_secondary.svg`

## Notes for the AI Assistant
- When asked what the system can do, summarise the sections under School Admin Navigation, Guardian Navigation, and Core Capabilities.
- When asked where to find X, map X to the listed endpoints and the related helpers noted in this document.
- For brand questions, cite the Branding Configuration section.
- If uncertain, say "I do not know" rather than guessing; prefer citing file paths and line numbers when available.
