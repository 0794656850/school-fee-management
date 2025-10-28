Kenyan Data Protection Act (2019) — Practical Checklist

- Lawful basis: document purpose and consent/contractual basis for processing guardian/student data.
- Data minimisation: collect only necessary fields (phone, name, class, balances).
- Privacy policy: expose a link in the app footer describing data usage and rights.
- Access control: per‑school isolation enforced; users mapped to a single school.
- Security: hash passwords (bcrypt/argon2), enforce HTTPS, rotate secrets, session protection.
- Data subject rights: add flows to export/update/delete student records on request.
- Retention: define retention periods; purge audit/ledger older than policy unless needed for finance.
- Breach handling: add incident log and contact procedure.
- Processor agreements: if using third‑party providers (SMS/WhatsApp/Email), sign DPAs and limit fields.
- Cross‑border transfers: store data in region where possible; document if exporting.
- Logging: ensure logs exclude sensitive content and include `school_id`.

This repository implements: strict tenant scoping, per‑school sessions, audit logs, and role‑based access. Review and tune defaults in production.

