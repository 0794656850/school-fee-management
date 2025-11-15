Architecture: MVC (Model–View–Controller)

Overview
- Model: domain/data layer. Currently in `models.py` (SQL/ORM helpers). A `models/` package can be introduced later if models are split; existing imports keep working.
- View: presentation layer. Flask/Jinja templates in `templates/` and static assets in `static/`.
- Controller: request/route layer. All Flask blueprints are accessible in `controllers/`.

Compatibility
- The legacy `routes.*` modules remain available. New `controllers/*` modules re‑export from `routes.*` so existing imports (`from routes.x import bp`) continue to work.
- Prefer importing from `controllers.*` for new code.

Layout
- controllers/
  - admin_routes.py, auth_routes.py, ... (re-exports from routes)
- templates/ (views)
- static/
- models.py (models)
- app.py (app wiring / blueprint registration)

Migration tips
- When splitting `models.py`, create `models/` package and move pieces gradually. Keep `models.py` as thin re-export or update imports module-by-module.
- Keep views free of business logic; centralize that in services/helpers used by controllers.

