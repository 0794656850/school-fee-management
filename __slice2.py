        if date and isinstance(day_val, (date, datetime)):
            day_str = day_val.isoformat()
        else:
            day_str = str(day_val)
        _daily.append({"day": day_str, "total": _to_float((r.get("total") if isinstance(r, dict) else r[1]))})
    daily_trend = _daily

    # Class summary numbers
    class_summary = [
        {
            "class_name": (r.get("class_name") if isinstance(r, dict) else r[0]),
            "total_students": _to_int((r.get("total_students") if isinstance(r, dict) else r[1])),
            "total_paid": _to_float((r.get("total_paid") if isinstance(r, dict) else r[2])),
            "total_pending": _to_float((r.get("total_pending") if isinstance(r, dict) else r[3])),
            "total_credit": _to_float((r.get("total_credit") if isinstance(r, dict) else r[4])),
        }
        for r in (class_summary or [])
    ]

    # Payment method breakdown
    method_breakdown = [
        {
            "method": (r.get("method") if isinstance(r, dict) else r[0]) or "",
            "count": _to_int((r.get("count") if isinstance(r, dict) else r[1])),
            "total": _to_float((r.get("total") if isinstance(r, dict) else r[2])),
        }
        for r in (method_breakdown or [])
    ]

    # Top debtors
    top_debtors = [
        {
            "name": (r.get("name") if isinstance(r, dict) else r[0]) or "",
            "class_name": (r.get("class_name") if isinstance(r, dict) else r[1]) or "",
            "balance": _to_float((r.get("balance") if isinstance(r, dict) else r[2])),
        }
        for r in (top_debtors or [])
    ]

    # Enrich class summary with percent_paid
    for row in class_summary:
        paid = _to_float(row.get("total_paid"))
        pending = _to_float(row.get("total_pending"))
        total = paid + pending
        row["percent_paid"] = round((paid / total * 100), 1) if total > 0 else 0.0

    resp = jsonify(
        {
            "monthly_data": monthly_data,
            "daily_trend": daily_trend,
            "class_summary": class_summary,
            "method_breakdown": method_breakdown,
            "top_debtors": top_debtors,
            "mom": {
                "current_month_total": current_month_total,
                "prev_month_total": prev_month_total,
                "percent_change": percent_change,
            },
            "meta": {"active_classes": int(active_classes or 0)},
        }
    )
    # Ensure real-time: prevent intermediary/browser caching
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


# ---------- DOCUMENTATION ----------
@app.route("/docs")
def docs():
    """Media hub for documentation with featured video support."""
    # Ensure media folder exists
    media_root = os.path.join(app.root_path, "static", "media")
    try:
        os.makedirs(media_root, exist_ok=True)
    except Exception:
        pass

    # Resolve featured video from settings
    featured_name = (get_setting("FEATURED_VIDEO_NAME") or "").strip()
    featured_url = None
    if featured_name:
        candidate = os.path.join(media_root, featured_name)
        if os.path.exists(candidate):
            featured_url = url_for("static", filename=f"media/{featured_name}")

    return render_template("docs.html", featured_url=featured_url, featured_name=featured_name)


@app.route("/docs/media")
def docs_media():
    """List media files under static/media for the library grid."""
    media_root = os.path.join(app.root_path, "static", "media")
    try:
        os.makedirs(media_root, exist_ok=True)
    except Exception:
        pass
    items = []
    allowed = {".mp4", ".webm", ".mov", ".png", ".jpg", ".jpeg", ".gif"}
    try:
        for name in sorted(os.listdir(media_root)):
            ext = os.path.splitext(name)[1].lower()
            if ext not in allowed:
                continue
            mtype = "video" if ext in {".mp4", ".webm", ".mov"} else "image"
            items.append({
                "name": name,
                "type": mtype,
                "url": url_for("static", filename=f"media/{name}"),
            })
    except Exception:
        items = []
    return jsonify({"ok": True, "media": items})


@app.route("/docs/upload", methods=["POST"])
def docs_upload():
    """Upload one or more media files to static/media."""
    media_root = os.path.join(app.root_path, "static", "media")
    try:
        os.makedirs(media_root, exist_ok=True)
    except Exception:
        pass
    files = request.files.getlist("files") if request.files else []
    if not files:
        return jsonify({"ok": False, "error": "No files"}), 400
    allowed = {".mp4", ".webm", ".mov", ".png", ".jpg", ".jpeg", ".gif"}
    saved = []
    for f in files:
        try:
            name = secure_filename(f.filename or "")
            if not name:
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in allowed:
                continue
            path = os.path.join(media_root, name)
            f.save(path)
            saved.append(name)
        except Exception:
            continue
    return jsonify({"ok": True, "saved": saved})


@app.route("/docs/media/<name>", methods=["DELETE"])
def docs_media_delete(name: str):
    """Delete a media file by name from static/media."""
    media_root = os.path.join(app.root_path, "static", "media")
    safe_name = secure_filename(name or "")
    if not safe_name:
        return jsonify({"ok": False, "error": "Invalid name"}), 400
    path = os.path.join(media_root, safe_name)
    try:
        if os.path.isfile(path):
            os.remove(path)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/docs/feature", methods=["POST"])
def docs_feature():
    """Set the featured video by file name in settings."""
    try:
        data = request.get_json(silent=True) or {}
        name = (data.get("name") or "").strip()
    except Exception:
        name = ""
    name = secure_filename(name)
    if not name:
        return jsonify({"ok": False, "error": "Invalid name"}), 400
    media_root = os.path.join(app.root_path, "static", "media")
    if not os.path.exists(os.path.join(media_root, name)):
        return jsonify({"ok": False, "error": "File not found"}), 404
    try:
        set_school_setting("FEATURED_VIDEO_NAME", name)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500



# ---------- REPORTS PAGE ----------
@app.route("/reports")
def reports():
    # Simple page; exports are scoped to current term
    return render_template("reports.html")


# ---------- XLSX EXPORT (optional) ----------
@app.route("/export_fees_xlsx")
def export_fees_xlsx():
    if not is_pro_enabled(app):
        try:
