from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote, urlparse

from flask import current_app

from utils.timezone_helpers import east_africa_now

try:
    import mysql.connector as mysql_connector
except Exception:
    mysql_connector = None


class BackupException(Exception):
    pass


def _parse_mysql_uri(uri: str) -> dict[str, Any]:
    parsed = urlparse(uri)
    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "localhost"
    port = parsed.port
    database = parsed.path.lstrip("/") or ""
    return {"user": username, "password": password, "host": host, "port": port, "database": database}


def _ensure_backup_root(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    return root


def _format_archive_name(src: Path, index: int) -> str:
    label = src.name or "root"
    clean = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in label)
    return f"{index:02d}-{clean}"


def _archive_path(src: Path, dest_dir: Path, idx: int) -> dict[str, Any]:
    info: dict[str, Any] = {"name": src.name, "status": "missing", "source": str(src)}
    if not src.exists():
        info["status"] = "skipped"
        info["reason"] = "not found"
        return info
    archive_base = dest_dir / _format_archive_name(src, idx)
    archive_path = archive_base.with_suffix(".zip")
    try:
        if src.is_dir():
            shutil.make_archive(str(archive_base), "zip", root_dir=str(src))
        else:
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(str(src), arcname=src.name)
        size = archive_path.stat().st_size
        info.update(
            {
                "archive": str(archive_path),
                "status": "ok",
                "size": size,
                "relative_path": str(archive_path.relative_to(dest_dir)),
                "is_dir": src.is_dir(),
            }
        )
    except Exception as exc:
        info.update({"status": "error", "reason": str(exc)})
    return info


def _dump_database(destination: Path, uri: str) -> dict[str, Any]:
    info: dict[str, Any] = {"status": "skipped", "path": ""}
    parsed = _parse_mysql_uri(uri)
    if not parsed["database"]:
        info["reason"] = "no database name"
        return info
    dest_file = destination / "database.sql"
    cmd = [
        "mysqldump",
        "-h",
        parsed["host"],
        "-u",
        parsed["user"],
        parsed["database"],
    ]
    if parsed["password"]:
        cmd.append(f"--password={parsed['password']}")
    if parsed["port"]:
        cmd.extend(["-P", str(parsed["port"])])
    info["path"] = str(dest_file)
    try:
        with dest_file.open("wb") as out:
            result = subprocess.run(cmd, stdout=out, stderr=subprocess.PIPE, check=True)
        info["status"] = "ok"
        info["size"] = dest_file.stat().st_size
        if result.stderr:
            info["warnings"] = result.stderr.decode("utf-8", "ignore").strip()
    except FileNotFoundError:
        info["status"] = "error"
        info["reason"] = "mysqldump not found"
    except subprocess.CalledProcessError as exc:
        info["status"] = "error"
        info["reason"] = f"mysqldump failed ({exc.returncode})"
        if exc.stderr:
            info["details"] = exc.stderr.decode("utf-8", "ignore")
    except Exception as exc:
        info["status"] = "error"
        info["reason"] = str(exc)
    return info


def _iter_sql_statements(source: Path) -> Iterator[str]:
    delimiter = ";"
    buffer: list[str] = []
    with source.open("r", encoding="utf-8", errors="ignore") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\r\n")
            clean = line.rstrip()
            if not clean:
                continue
            stripped = clean.strip()
            if stripped.lower().startswith("mysqldump:"):
                continue
            if stripped.upper().startswith("DELIMITER"):
                parts = stripped.split(None, 1)
                delimiter = parts[1] if len(parts) > 1 else ";"
                continue
            buffer.append(clean)
            if delimiter and clean.endswith(delimiter):
                buffer[-1] = buffer[-1][: len(buffer[-1]) - len(delimiter)]
                statement = "\n".join(buffer).strip()
                buffer = []
                if statement:
                    yield statement
        if buffer:
            statement = "\n".join(buffer).strip()
            if statement:
                yield statement


def _import_database_dump(source: Path, uri: str) -> dict[str, Any]:
    info: dict[str, Any] = {"status": "skipped", "path": str(source)}
    if not source.exists():
        info["status"] = "error"
        info["reason"] = "dump file missing"
        return info
    parsed = _parse_mysql_uri(uri)
    if not parsed["database"]:
        info["status"] = "error"
        info["reason"] = "no database name"
        return info

    if mysql_connector is not None:
        connection = None
        cursor = None
        try:
            conn_kwargs = {
                "host": parsed["host"],
                "user": parsed["user"],
                "password": parsed["password"],
                "database": parsed["database"],
            }
            if parsed["port"]:
                conn_kwargs["port"] = parsed["port"]
            connection = mysql_connector.connect(**conn_kwargs)
            cursor = connection.cursor()
            for statement in _iter_sql_statements(source):
                if not statement:
                    continue
                cursor.execute(statement)
            connection.commit()
            info["status"] = "ok"
            info["size"] = source.stat().st_size
        except mysql_connector.Error as exc:
            info["status"] = "error"
            info["reason"] = str(exc)
        except Exception as exc:
            info["status"] = "error"
            info["reason"] = str(exc)
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
        return info

    cmd = [
        "mysql",
        "-h",
        parsed["host"],
        "-u",
        parsed["user"],
        parsed["database"],
    ]
    if parsed["password"]:
        cmd.append(f"--password={parsed['password']}")
    if parsed["port"]:
        cmd.extend(["-P", str(parsed["port"])])
    try:
        with source.open("rb") as fh:
            subprocess.run(
                cmd,
                stdin=fh,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
        info["status"] = "ok"
        info["size"] = source.stat().st_size
    except FileNotFoundError:
        info["status"] = "error"
        info["reason"] = "mysql not found"
    except subprocess.CalledProcessError as exc:
        info["status"] = "error"
        info["reason"] = f"mysql import failed ({exc.returncode})"
        if exc.stdout:
            info["details"] = exc.stdout.decode("utf-8", "ignore")
    except Exception as exc:
        info["status"] = "error"
        info["reason"] = str(exc)
    return info


def format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except Exception:
        return "0 B"
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(suffixes) - 1:
        size /= 1024
        idx += 1
    return f"{size:.1f} {suffixes[idx]}"


def _package_snapshot(dest: Path) -> Path:
    """Zip the backup folder to produce a single file for local restores."""
    archive_path = dest.with_suffix(".zip")
    try:
        if archive_path.exists():
            archive_path.unlink()
        shutil.make_archive(str(dest), "zip", root_dir=str(dest))
    except Exception:
        pass
    return archive_path if archive_path.exists() else dest



def cleanup_old_backups(root: Path, keep_days: int) -> list[str]:
    if keep_days <= 0:
        return []
    removed = []
    cutoff = east_africa_now().timestamp() - (keep_days * 86400)
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if child.stat().st_mtime < cutoff:
            try:
                shutil.rmtree(child)
                removed.append(child.name)
            except Exception:
                continue
    return removed


def _resolve_school_root(application, school_id: int | str | None) -> Path:
    base = Path(application.config.get("BACKUP_DIRECTORY", "instance/backups"))
    segment = f"school-{school_id}" if school_id else "global"
    return _ensure_backup_root(base / segment)


def backup_root_for_school(app=None, school_id: int | str | None = None) -> Path:
    application = app or current_app._get_current_object()
    return _resolve_school_root(application, school_id)


def create_backup(app=None, reason: str = "manual", school_id: int | str | None = None) -> dict[str, Any]:
    application = app or current_app._get_current_object()
    root = _resolve_school_root(application, school_id)
    timestamp = east_africa_now()
    label = timestamp.strftime("%Y%m%dT%H%M%S")
    dest = root / label
    dest.mkdir(parents=True, exist_ok=True)

    archives: list[dict[str, Any]] = []
    include_dirs = list(application.config.get("BACKUP_INCLUDE_DIRS") or [])
    include_dirs.extend(
        filter(
            None,
            [
                application.config.get("GUARDIAN_RECEIPT_UPLOADS_DIR"),
                application.config.get("PAYMENT_PROOF_UPLOADS_DIR"),
            ],
        )
    )
    seen_paths: set[str] = set()
    for idx, raw in enumerate(include_dirs, start=1):
        path = Path(raw)
        if not path.is_absolute():
            path = Path(application.root_path) / raw
        path = path.resolve()
        if str(root) in str(path) or path == root or str(path) in seen_paths:
            continue
        seen_paths.add(str(path))
        archives.append(_archive_path(path, dest, idx))

    db_info = _dump_database(dest, application.config.get("SQLALCHEMY_DATABASE_URI", "")) if application.config.get("SQLALCHEMY_DATABASE_URI") else {"status": "skipped", "reason": "no URI"}

    removed = cleanup_old_backups(root, int(application.config.get("BACKUP_KEEP_DAYS", 60)))

    snapshot_zip = _package_snapshot(dest)
    entry = {
        "timestamp": timestamp.isoformat(),
        "dir": str(dest),
        "reason": reason,
        "school_id": school_id,
        "db_dump": db_info,
        "archives": archives,
        "removed": removed,
        "snapshot": str(snapshot_zip),
    }
    history_file = root / "history.jsonl"
    try:
        with history_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception as exc:
        raise BackupException(f"Unable to record backup history: {exc}")
    return entry


def get_backup_history(app=None, limit: int = 6, school_id: int | str | None = None) -> list[dict[str, Any]]:
    application = app or current_app._get_current_object()
    root = _resolve_school_root(application, school_id)
    history_file = root / "history.jsonl"
    if not history_file.exists():
        return []
    entries: list[dict[str, Any]] = []
    try:
        with history_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                entries.append(data)
    except Exception:
        return []
    return list(reversed(entries[-limit:]))


def _restore_archives(archives: list[dict[str, Any]], dest_root: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for archive_info in archives or []:
        rel_path = archive_info.get("relative_path")
        archive_path = None
        if rel_path:
            archive_path = dest_root / rel_path
        elif archive_info.get("archive"):
            archive_path = dest_root / Path(archive_info.get("archive")).name
        result: dict[str, Any] = {
            "archive": str(archive_path) if archive_path else None,
            "target": archive_info.get("source"),
        }
        if archive_path is None or not archive_path.exists():
            result.update({"status": "missing", "reason": "archive file unavailable"})
            results.append(result)
            continue
        target_path = Path(archive_info.get("source") or "")
        if not target_path:
            result.update({"status": "skipped", "reason": "missing target path"})
            results.append(result)
            continue
        is_dir = archive_info.get("is_dir", target_path.is_dir())
        extract_to = target_path if is_dir else target_path.parent
        if is_dir and extract_to.exists():
            shutil.rmtree(extract_to, ignore_errors=True)
        elif not is_dir and target_path.exists():
            try:
                target_path.unlink()
            except Exception:
                pass
        try:
            extract_to.mkdir(parents=True, exist_ok=True)
            shutil.unpack_archive(str(archive_path), extract_to=str(extract_to))
            result.update({"status": "ok", "restored_to": str(extract_to)})
        except Exception as exc:
            result.update({"status": "error", "reason": str(exc)})
        results.append(result)
    return results


def restore_backup_snapshot(entry: dict[str, Any], app=None) -> dict[str, Any]:
    application = app or current_app._get_current_object()
    snapshot_path_str = entry.get("snapshot") or ""
    if not snapshot_path_str:
        raise BackupException("Snapshot file not recorded")
    snapshot_path = Path(snapshot_path_str)
    if not snapshot_path.exists():
        raise BackupException("Snapshot file not found")
    temp_dir: Path | None = None
    try:
        if snapshot_path.is_dir():
            working_dir = snapshot_path
        else:
            if snapshot_path.suffix.lower() != ".zip":
                raise BackupException("Snapshot archive must be a .zip file")
            temp_dir = Path(tempfile.mkdtemp(prefix="fms-restore-"))
            try:
                with zipfile.ZipFile(snapshot_path, "r") as archive:
                    archive.extractall(temp_dir)
            except zipfile.BadZipFile as exc:
                raise BackupException(f"Snapshot archive corrupted: {exc}") from exc
            working_dir = temp_dir
        db_file = next(working_dir.rglob("database.sql"), None)
        if db_file is None:
            raise BackupException("Snapshot does not contain a database dump")
        db_result = _import_database_dump(db_file, application.config.get("SQLALCHEMY_DATABASE_URI", ""))
        assets_result = _restore_archives(entry.get("archives") or [], working_dir)
        return {"database": db_result, "assets": assets_result}
    finally:
        if temp_dir is not None and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
