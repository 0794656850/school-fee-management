from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from config import Config

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if username == Config.ADMIN_USERNAME and password == Config.ADMIN_PASSWORD:
            session["user"] = username
            flash("Login successful!", "success")
            return redirect(url_for("auth.dashboard"))
        else:
            flash("Invalid credentials", "danger")

    return render_template("login.html")

@auth_bp.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("auth.login"))
    return render_template("dashboard.html")

@auth_bp.route("/logout")
def logout():
    session.pop("user", None)
    flash("You have been logged out.", "info")
    return redirect(url_for("auth.login"))
