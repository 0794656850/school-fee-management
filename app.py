from flask import Flask
from config import Config
from utils import db, init_db
from flask_migrate import Migrate

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # initialize DB
    init_db(app)

    # set up Flask-Migrate
    migrate = Migrate(app, db)

    @app.route("/")
    def home():
        return "Fee Management System Running ✅"

    @app.route("/students")
    def students():
        return "students page"

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)