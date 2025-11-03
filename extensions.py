import os
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail
from flask_migrate import Migrate

db = SQLAlchemy()
mail = Mail()
migrate = Migrate()

def _truthy(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on", "y"}

try:
    from flask_limiter import Limiter  # type: ignore
    from flask_limiter.util import get_remote_address  # type: ignore

    # In-memory rate limiter (sufficient for single-instance deployments).
    limiter = Limiter(get_remote_address, storage_uri="memory://")
    # Allow disabling via env for any environment
    if _truthy(os.environ.get("DISABLE_RATE_LIMITING")):
        def _identity(x):
            return x
        try:
            limiter.init_app = lambda *a, **k: None  # type: ignore
        except Exception:
            pass
        limiter.limit = lambda *a, **k: _identity  # type: ignore
except Exception:
    # Fallback no-op limiter so app can start without Flask-Limiter present
    def _identity(x):
        return x

    class _NoOpLimiter:
        def init_app(self, *_, **__):
            return None

        def limit(self, *_, **__):
            return _identity

    limiter = _NoOpLimiter()  # type: ignore
