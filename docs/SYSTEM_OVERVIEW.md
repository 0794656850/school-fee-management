Fee Management Portal — System Overview

This document summarizes the product identity, main navigation, and core capabilities so the in‑app AI Assistant can answer questions grounded in this system. It is safe to include in the AI index.

Branding
- Brand Name: SmartEduPay
- Portal Title: SmartEduPay Portal
- Primary Logo: static/img/smartedupay_logo.svg
- Secondary Logo: static/img/smartedupay_logo_secondary.svg
- Favicon: static/img/smartedupay_logo_secondary.svg (default)
- Footer Signature: Ac 2025 Fee_Management_System
- Profile isolation: every school's branding settings stay in `school_settings` keyed by `school_id`, so even matching data never leaks between tenants.

Navigation (Sidebar)
- Dashboard: High‑level KPIs and quick links; totals collected, pending balances, credits, and trends.
- Students: Manage student profiles, classes, balances, credits, and ledgers.
- Add Payment: Shortcut to the Payments page/form to record a payment for a student.
- Analytics: Charts for collections over time, class breakdowns, debtors, and method distributions.
- AI Assistant: Project‑aware chat that can answer fee, analytics, and “how does the app work” questions using local RAG.
- Credit Ops: View and manage overpayments as credit, and perform credit transfers between siblings.
- Terms: Manage academic terms, fee components, per‑class defaults, discounts, and generate invoices.
- Invoices: List and manage generated invoices; print and share with guardians.
- Reminders: Send Email reminders (templated) to guardians about balances or invoices.
- Documentation: In‑app documentation and quick links to setup guides.
- School Profile: Branding (logos, names), contact info, and school‑specific details.
- Access & Security: Access settings, default credentials, password updates, and role management.

Core Capabilities
- Multi‑tenant: Per‑school data isolation and branding; most tables include a school_id.
- Students & Payments: Balances tracked per student; overpayments accrue as credit; credits transferable.
- Terms & Invoices: Configure per‑term fee structure and generate invoices; term summaries available.
- Collections (M‑Pesa): Optional Daraja STK Push initiation and callback reconciliation.
- Reminders (Email): Email reminders to guardians (via Gmail API).
- Receipts (Email): Email receipts to guardians immediately after payments (Gmail API with SMTP fallback).
- Analytics: Monthly/daily trends, class summaries, top debtors, and MoM change.
- Admin & Roles: Owner/admin/staff roles (multi‑user as Pro), access settings and basic security measures.
- Audit: Basic ledger entries and audit trail for key events.
- AI Assistant: Retrieval-augmented answers over project files and docs, powered by Vertex AI (Gemini).

Key Endpoints (selected)
- / (dashboard)
- /students, /students/<id>
- /payments (create/list payments)
- /analytics
- /credit
- /terms, /terms/invoices
- /reminders
- /docs
- /admin/school, /admin/settings, /admin/security
- /ai

Branding Configuration (env vars)
- BRAND_NAME=SmartEduPay
- PORTAL_TITLE="SmartEduPay Portal"
- LOGO_PRIMARY=img/smartedupay_logo.svg
- LOGO_SECONDARY=img/smartedupay_logo_secondary.svg
- FAVICON=img/smartedupay_logo_secondary.svg

Notes for the AI Assistant
- When asked about what this system can do, summarize items under Core Capabilities.
- When asked where to find X, map X to the Navigation items and related endpoints above.
- When asked about branding or logos, use the Branding section and/or environment variables.
- If uncertain, say you don’t know rather than guessing; prefer citing file paths and line starts when possible.
